package peerhub

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestPeerKey(t *testing.T) {
	mac, _ := hex.DecodeString("701e7be12c05")
	port := uint16(28468)
	key := newPeerKey(mac, port)

	t.Run("New", func(t *testing.T) {
		assert.Equal(t, mac, key[:6])
		assert.Equal(t, uint8(0x34), key[6])
		assert.Equal(t, uint8(0x6f), key[7])
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

		macA, _ := hex.DecodeString("000102030405")
		srvConnA, clientConnA := net.Pipe()
		defer func() { assert.NoError(t, srvConnA.Close()) }()
		defer func() { assert.NoError(t, clientConnA.Close()) }()

		macB, _ := hex.DecodeString("000102030406")
		srvConnB, clientConnB := net.Pipe()
		defer func() { assert.NoError(t, srvConnB.Close()) }()
		defer func() { assert.NoError(t, clientConnB.Close()) }()

		ctxA, cancelA := context.WithCancel(ctx)
		peerA := RegisterPeer(ctxA, cancelA, hub, clientConnA, macA, 1001, peerTypeConnecting)

		ctxB, cancelB := context.WithCancel(ctx)
		peerB := RegisterPeer(ctxB, cancelB, hub, clientConnB, macB, 1002, peerTypeConnecting)

		peerA.peer = peerB
		peerB.peer = peerA

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
	})

	t.Run("WriteOnClosedPeer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		hub := NewHub(ctx)

		mac, _ := hex.DecodeString("000102030405")
		srvConn, clientConn := net.Pipe()
		defer func() { _ = srvConn.Close() }()

		ctxP, cancelP := context.WithCancel(ctx)
		p := RegisterPeer(ctxP, cancelP, hub, clientConn, mac, 1001, peerTypeConnecting)

		// Close the peer
		p.close()

		// Verify write returns ErrPeerDisconnected
		err := p.write([]byte("test"), 1*time.Second)
		assert.ErrorIs(t, err, ErrPeerDisconnected)
	})
}
