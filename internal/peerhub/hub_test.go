package peerhub

import (
	"context"
	"io"
	"net"
	"sprawl/internal/pspnet"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestHub(t *testing.T) {
	defer goleak.VerifyNone(t)

	t.Run("RegisterListeningPeer", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		hub := NewHub(ctx)
		srvConn, clientConn := net.Pipe()
		defer func() { assert.NoError(t, srvConn.Close()) }()
		defer func() { assert.NoError(t, clientConn.Close()) }()

		macAddr := pspnet.MustParseMacAddr("00:01:02:03:04:05")
		port := uint16(1001)
		key := newPeerKey(macAddr, port)

		hub.HandleListeningPeer(clientConn, macAddr, port)

		require.Eventually(t, func() bool {
			return hub.findListeningPeer(key) != nil
		}, 1*time.Second, 10*time.Millisecond, "Listening peer to be registered")

		p := hub.findListeningPeer(key)
		require.NotNil(t, p)
		requirePeerShutdown(t, hub, p, srvConn)
		cancel()
	})

	t.Run("HandleConnectingPeer", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			targetMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			targetPort := uint16(1002)
			srvConnT, clientConnT := net.Pipe()
			defer func() { assert.NoError(t, srvConnT.Close()) }()
			defer func() { assert.NoError(t, clientConnT.Close()) }()

			hub.HandleListeningPeer(clientConnT, targetMac, targetPort)

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:06")
			srcPort := uint16(1001)
			srvConnS, clientConnS := net.Pipe()
			defer func() { assert.NoError(t, srvConnS.Close()) }()
			defer func() { assert.NoError(t, clientConnS.Close()) }()

			hub.HandleConnectingPeer(clientConnS, srcMac, srcPort, targetMac, targetPort)

			receivedKey := make([]byte, 10)
			_ = srvConnT.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := io.ReadFull(srvConnT, receivedKey)
			require.NoError(t, err, "Target peer should receive source peer key")

			expectedKey := newPeerKey(srcMac, srcPort)
			assert.Equal(t, expectedKey.encode(), receivedKey, "Received key should match source peer key")

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(newPeerKey(srcMac, srcPort)) != nil
			}, 1*time.Second, 10*time.Millisecond, "Source peer should be registered as connected")

			targetPeer := hub.findListeningPeer(newPeerKey(targetMac, targetPort))
			sourcePeer := hub.findConnectedPeer(newPeerKey(srcMac, srcPort))
			require.NotNil(t, targetPeer)
			require.NotNil(t, sourcePeer)
			requirePeerShutdown(t, hub, targetPeer, srvConnT)
			requirePeerShutdown(t, hub, sourcePeer, srvConnS)
			cancel()
		})

		t.Run("NotFound", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			srcPort := uint16(1001)
			targetMac := pspnet.MustParseMacAddr("00:01:02:03:04:06")
			targetPort := uint16(1002)

			srvConn, clientConn := net.Pipe()
			defer func() { assert.NoError(t, srvConn.Close()) }()
			defer func() { assert.NoError(t, clientConn.Close()) }()

			hub.HandleConnectingPeer(clientConn, srcMac, srcPort, targetMac, targetPort)

			_ = srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := srvConn.Read([]byte{})
			assert.ErrorIs(t, err, io.EOF, "Peer connection should be closed when target not found")
			cancel()
		})
	})

	t.Run("HandleAcceptingPeer", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			targetMac := pspnet.MustParseMacAddr("00:01:02:03:04:06")
			targetPort := uint16(1002)
			srvConnTarget, clientConnT := net.Pipe()
			defer func() { assert.NoError(t, srvConnTarget.Close()) }()
			defer func() { assert.NoError(t, clientConnT.Close()) }()

			targetPeer := hub.registerPeer(clientConnT, targetMac, targetPort, peerTypeConnecting)

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(targetPeer.key) != nil
			}, 1*time.Second, 10*time.Millisecond, "Target peer should be registered")

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			srcPort := uint16(1001)
			srcKey := newPeerKey(srcMac, srcPort)
			srvConnSrc, clientConnSrc := net.Pipe()
			defer func() { assert.NoError(t, srvConnSrc.Close()) }()
			defer func() { assert.NoError(t, clientConnSrc.Close()) }()

			hub.HandleAcceptingPeer(clientConnSrc, srcMac, srcPort, targetMac, targetPort)

			receivedSrcKey := make([]byte, 10)
			_ = srvConnTarget.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, err := io.ReadFull(srvConnTarget, receivedSrcKey)
			require.NoError(t, err, "Target peer should receive source peer key")
			assert.Equal(t, srcKey.encode(), receivedSrcKey)

			receivedTargetKey := make([]byte, 10)
			_ = srvConnSrc.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, err = io.ReadFull(srvConnSrc, receivedTargetKey)
			require.NoError(t, err, "Source peer should receive target peer key")
			assert.Equal(t, targetPeer.key.encode(), receivedTargetKey)

			assert.Eventually(t, func() bool {
				pS := hub.findConnectedPeer(srcKey)
				pT := hub.findConnectedPeer(targetPeer.key)
				return pS != nil && pT != nil && pS.peer.Load() == pT && pT.peer.Load() == pS
			}, 1*time.Second, 10*time.Millisecond, "Expected both peers to be connected and interlinked with each other")

			sourcePeer := hub.findConnectedPeer(srcKey)
			require.NotNil(t, sourcePeer)
			requirePeerShutdown(t, hub, targetPeer, srvConnTarget)
			requirePeerShutdown(t, hub, sourcePeer, srvConnSrc)
			cancel()
		})

		t.Run("NotFound", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			srcPort := uint16(1001)
			targetMac := pspnet.MustParseMacAddr("00:01:02:03:04:06")
			targetPort := uint16(1002)

			srvConn, clientConn := net.Pipe()
			defer func() { assert.NoError(t, srvConn.Close()) }()
			defer func() { assert.NoError(t, clientConn.Close()) }()

			hub.HandleAcceptingPeer(clientConn, srcMac, srcPort, targetMac, targetPort)

			_ = srvConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, err := srvConn.Read([]byte{})
			assert.ErrorIs(t, err, io.EOF, "Connection should be closed when target not found")
			cancel()
		})

		t.Run("Hijack", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			srvConnTarget, clientConnTarget := net.Pipe()
			defer func() { assert.NoError(t, srvConnTarget.Close()) }()
			defer func() { assert.NoError(t, clientConnTarget.Close()) }()

			srvConnExisting, clientConnExisting := net.Pipe()
			defer func() { assert.NoError(t, srvConnExisting.Close()) }()
			defer func() { assert.NoError(t, clientConnExisting.Close()) }()

			targetPeer := hub.registerPeer(clientConnTarget, pspnet.MustParseMacAddr("00:01:02:03:04:06"), 1002, peerTypeConnecting)
			existingPeer := hub.registerPeer(clientConnExisting, pspnet.MustParseMacAddr("00:01:02:03:04:07"), 1003, peerTypeConnecting)
			targetPeer.peer.Store(existingPeer)
			existingPeer.peer.Store(targetPeer)

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(targetPeer.key) != nil
			}, 1*time.Second, 50*time.Millisecond, "Target peer should be registered")

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			srcPort := uint16(1001)

			srvConn, clientConn := net.Pipe()
			defer func() { assert.NoError(t, srvConn.Close()) }()
			defer func() { assert.NoError(t, clientConn.Close()) }()

			// try to connect a new peer to the already connected/linked peer
			hub.HandleAcceptingPeer(clientConn, srcMac, srcPort, targetPeer.macAddr, targetPeer.port)

			_ = srvConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, err := srvConn.Read([]byte{})
			assert.ErrorIs(t, err, io.EOF, "Connection should be closed when attempting to hijack another connected client")

			requirePeerShutdown(t, hub, targetPeer, srvConnTarget)
			requirePeerShutdown(t, hub, existingPeer, srvConnExisting)
			requirePeerEOF(t, srvConn)
			cancel()
		})
	})

	t.Run("HouseKeeper", func(t *testing.T) {
		t.Run("UnregisterListening", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			mac := pspnet.MustParseMacAddr("00:01:02:03:04:05")

			srvConn, clientConn := net.Pipe()
			defer func() { assert.NoError(t, srvConn.Close()) }()

			p := hub.registerPeer(clientConn, mac, 1001, peerTypeListening)

			require.Eventually(t, func() bool {
				return hub.findListeningPeer(p.key) != nil
			}, 1*time.Second, 10*time.Millisecond, "Expected listening peer to be registered")

			p.close()
			requirePeerUnregistered(t, hub, p)
			requirePeerWriterDone(t, p)
			requirePeerEOF(t, srvConn)
			cancel()
		})

		t.Run("UnregisterConnecting", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			mac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			key := newPeerKey(mac, 1001)
			srvConn, clientConn := net.Pipe()
			defer func() { assert.NoError(t, srvConn.Close()) }()

			p := hub.registerPeer(clientConn, mac, 1001, peerTypeConnecting)

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(key) != nil
			}, 1*time.Second, 10*time.Millisecond)

			p.close()
			requirePeerUnregistered(t, hub, p)
			requirePeerWriterDone(t, p)
			requirePeerEOF(t, srvConn)
			cancel()
		})
	})
}
