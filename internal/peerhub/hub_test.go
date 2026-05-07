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

		HandleListeningPeer(ctx, hub, clientConn, macAddr, port)

		require.Eventually(t, func() bool {
			return hub.findListeningPeer(key) != nil
		}, 1*time.Second, 10*time.Millisecond, "Listening peer to be registered")
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

			HandleListeningPeer(ctx, hub, clientConnT, targetMac, targetPort)

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:06")
			srcPort := uint16(1001)
			srvConnS, clientConnS := net.Pipe()
			defer func() { assert.NoError(t, srvConnS.Close()) }()
			defer func() { assert.NoError(t, clientConnS.Close()) }()

			HandleConnectingPeer(ctx, hub, clientConnS, srcMac, srcPort, targetMac, targetPort)

			receivedKey := make([]byte, 10)
			_ = srvConnT.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := io.ReadFull(srvConnT, receivedKey)
			require.NoError(t, err, "Target peer should receive source peer key")

			expectedKey := newPeerKey(srcMac, srcPort)
			assert.Equal(t, expectedKey.encode(), receivedKey, "Received key should match source peer key")

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(newPeerKey(srcMac, srcPort)) != nil
			}, 1*time.Second, 10*time.Millisecond, "Source peer should be registered as connected")
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

			HandleConnectingPeer(ctx, hub, clientConn, srcMac, srcPort, targetMac, targetPort)

			_ = srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := srvConn.Read([]byte{})
			assert.ErrorIs(t, err, io.EOF, "Peer connection should be closed when target not found")
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

			targetPeer := hub.registerPeer(ctx, clientConnT, targetMac, targetPort, peerTypeConnecting)

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(targetPeer.key) != nil
			}, 1*time.Second, 10*time.Millisecond, "Target peer should be registered")

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			srcPort := uint16(1001)
			srcKey := newPeerKey(srcMac, srcPort)
			srvConnSrc, clientConnSrc := net.Pipe()
			defer func() { assert.NoError(t, srvConnSrc.Close()) }()
			defer func() { assert.NoError(t, clientConnSrc.Close()) }()

			HandleAcceptingPeer(ctx, hub, clientConnSrc, srcMac, srcPort, targetMac, targetPort)

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
				return pS != nil && pT != nil && pS.peer == pT && pT.peer == pS
			}, 1*time.Second, 10*time.Millisecond, "Expected both peers to be connected and interlinked with each other")
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

			HandleAcceptingPeer(ctx, hub, clientConn, srcMac, srcPort, targetMac, targetPort)

			_ = srvConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, err := srvConn.Read([]byte{})
			assert.ErrorIs(t, err, io.EOF, "Connection should be closed when target not found")
		})

		t.Run("Hijack", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			targetMac := pspnet.MustParseMacAddr("00:01:02:03:04:06")
			targetPort := uint16(1002)
			targetKey := newPeerKey(targetMac, targetPort)

			pTarget := &peer{
				peerType: peerTypeConnecting,
				key:      targetKey,
				peer:     &peer{}, // the hijack check only looks if the target peer is already connected to another
			}
			hub.connectedPeers[targetKey] = pTarget

			srcMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			srcPort := uint16(1001)

			srvConn, clientConn := net.Pipe()

			HandleAcceptingPeer(ctx, hub, clientConn, srcMac, srcPort, targetMac, targetPort)

			_ = srvConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, err := srvConn.Read([]byte{})
			assert.ErrorIs(t, err, io.EOF, "Connection should be closed when attempting to hijack another connected client")
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

			p := hub.registerPeer(ctx, clientConn, mac, 1001, peerTypeListening)

			require.Eventually(t, func() bool {
				return hub.findListeningPeer(p.key) != nil
			}, 1*time.Second, 10*time.Millisecond, "Expected listening peer to be registered")

			p.close()

			require.Eventually(t, func() bool {
				return hub.findListeningPeer(p.key) == nil
			}, 1*time.Second, 10*time.Millisecond, "Expected listening peer to be unregistered")
		})

		t.Run("UnregisterConnecting", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			mac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
			key := newPeerKey(mac, 1001)
			srvConn, clientConn := net.Pipe()
			defer func() { assert.NoError(t, srvConn.Close()) }()

			p := hub.registerPeer(ctx, clientConn, mac, 1001, peerTypeConnecting)

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(key) != nil
			}, 1*time.Second, 10*time.Millisecond)

			p.close()

			require.Eventually(t, func() bool {
				return hub.findConnectedPeer(key) == nil
			}, 1*time.Second, 10*time.Millisecond, "Expected peer to be unregistered")

			_ = srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := srvConn.Read([]byte{})
			require.ErrorIs(t, err, io.EOF, "Expected peer A connection to be closed")
		})
	})
}
