package peerhub

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sprawl/internal/pspnet"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

const peerShutdownTimeout = 1 * time.Second
const peerShutdownPollInterval = 10 * time.Millisecond

func requirePeerUnregistered(t *testing.T, hub *PeerHub, p *peer) {
	t.Helper()

	require.Eventually(t, func() bool {
		switch p.peerType {
		case peerTypeListening:
			return hub.findListeningPeer(p.key) == nil
		case peerTypeConnecting:
			return hub.findConnectedPeer(p.key) == nil
		default:
			return false
		}
	}, peerShutdownTimeout, peerShutdownPollInterval, "Peer should be unregistered")
}

func requirePeerWriterDone(t *testing.T, p *peer) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case <-p.writerDone:
			return true
		default:
			return false
		}
	}, peerShutdownTimeout, peerShutdownPollInterval, "Peer writer should be done")
}

func requirePeerEOF(t *testing.T, conn net.Conn) {
	t.Helper()

	require.Eventually(t, func() bool {
		if err := conn.SetReadDeadline(time.Now().Add(peerShutdownPollInterval)); err != nil {
			_, readErr := conn.Read(make([]byte, 1))
			return errors.Is(readErr, io.EOF)
		}

		_, err := conn.Read(make([]byte, 1))
		return errors.Is(err, io.EOF)
	}, peerShutdownTimeout, peerShutdownPollInterval, "Peer connection should be closed")
}

func requirePeerShutdown(t *testing.T, hub *PeerHub, p *peer, remoteConn net.Conn) {
	t.Helper()

	p.close()
	requirePeerUnregistered(t, hub, p)
	requirePeerWriterDone(t, p)
	requirePeerEOF(t, remoteConn)
}

func TestPeerKey(t *testing.T) {
	mac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
	port := uint16(28468)
	key := newPeerKey(mac, port)

	t.Run("New", func(t *testing.T) {
		assert.Equal(t, key[:6], mac[:])
		assert.Equal(t, key[6], uint8(0x34))
		assert.Equal(t, key[7], uint8(0x6f))
	})

	t.Run("Encode", func(t *testing.T) {
		encoded := key.encode()
		assert.Len(t, encoded, 10, "Encoded bytes should be 10 bytes")

		assert.Equal(t, key[:6], encoded[:6], "Encoded MAC address should match")
		assert.Equal(t, uint8(0x34), encoded[8], "Encoded port should match")
		assert.Equal(t, uint8(0x6f), encoded[9], "Encoded port should match")
	})
}

func TestPeer(t *testing.T) {
	defer goleak.VerifyNone(t)

	t.Run("DataTransfer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		hub := NewHub(ctx)

		macA := pspnet.MustParseMacAddr("00:01:02:03:04:05")
		srvConnA, clientConnA := net.Pipe()
		defer func() { assert.NoError(t, srvConnA.Close()) }()
		defer func() { assert.NoError(t, clientConnA.Close()) }()

		macB := pspnet.MustParseMacAddr("00:01:02:03:04:06")
		srvConnB, clientConnB := net.Pipe()
		defer func() { assert.NoError(t, srvConnB.Close()) }()
		defer func() { assert.NoError(t, clientConnB.Close()) }()

		peerA := hub.registerPeer(clientConnA, macA, 1001, peerTypeConnecting)

		peerB := hub.registerPeer(clientConnB, macB, 1002, peerTypeConnecting)

		peerA.peer.Store(peerB)
		peerB.peer.Store(peerA)

		payload := []byte("hello from A")
		packet := make([]byte, 4+len(payload))
		binary.LittleEndian.PutUint32(packet[0:4], uint32(len(payload)))
		copy(packet[4:], payload)

		_, err := srvConnA.Write(packet)
		require.NoError(t, err)

		received := make([]byte, 4+len(payload))
		_ = srvConnB.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, err = io.ReadFull(srvConnB, received)
		require.NoError(t, err)

		assert.Equal(t, uint32(len(payload)), binary.LittleEndian.Uint32(received[0:4]))
		assert.Equal(t, payload, received[4:])

		requirePeerShutdown(t, hub, peerA, srvConnA)
		requirePeerShutdown(t, hub, peerB, srvConnB)
		cancel()
	})

	t.Run("CloseLinkedPeers", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		hub := NewHub(ctx)

		macA := pspnet.MustParseMacAddr("00:01:02:03:04:05")
		srvConnA, clientConnA := net.Pipe()
		defer func() { _ = srvConnA.Close() }()
		defer func() { _ = clientConnA.Close() }()

		macB := pspnet.MustParseMacAddr("00:01:02:03:04:06")
		srvConnB, clientConnB := net.Pipe()
		defer func() { _ = srvConnB.Close() }()
		defer func() { _ = clientConnB.Close() }()

		peerA := hub.registerPeer(clientConnA, macA, 1001, peerTypeConnecting)
		peerB := hub.registerPeer(clientConnB, macB, 1002, peerTypeConnecting)

		peerA.peer.Store(peerB)
		peerB.peer.Store(peerA)

		peerA.close()

		assert.Eventually(t, func() bool {
			return hub.findConnectedPeer(peerA.key) == nil
		}, 1*time.Second, 50*time.Millisecond)

		assert.Eventually(t, func() bool {
			return hub.findConnectedPeer(peerB.key) == nil
		}, 1*time.Second, 50*time.Millisecond)

		requirePeerWriterDone(t, peerA)
		requirePeerWriterDone(t, peerB)
		requirePeerEOF(t, srvConnA)
		requirePeerEOF(t, srvConnB)
		cancel()
	})
}
