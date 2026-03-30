package peerhub

import (
	"context"
	"encoding/hex"
	"io"
	"net"
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

		macAddr, _ := hex.DecodeString("000102030405")
		port := uint16(1001)
		key := newPeerKey(macAddr, port)

		RegisterListeningPeer(ctx, hub, clientConn, macAddr, port)

		assert.Eventually(t, func() bool {
			return hub.findListeningPeer(key) != nil
		}, 1*time.Second, 10*time.Millisecond, "Listening peer to be registered")
	})

	t.Run("HandleConnectingPeer", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			targetMac, _ := hex.DecodeString("000102030406")
			targetPort := uint16(1002)
			srvConnT, clientConnT := net.Pipe()
			defer func() { assert.NoError(t, srvConnT.Close()) }()
			defer func() { assert.NoError(t, clientConnT.Close()) }()

			RegisterListeningPeer(ctx, hub, clientConnT, targetMac, targetPort)

			srcMac, _ := hex.DecodeString("000102030405")
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

			assert.Eventually(t, func() bool {
				return hub.findConnectedPeer(newPeerKey(srcMac, srcPort)) != nil
			}, 1*time.Second, 10*time.Millisecond, "Source peer should be registered as connected")
		})

		t.Run("NotFound", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			srcMac, _ := hex.DecodeString("000102030405")
			srcPort := uint16(1001)
			targetMac, _ := hex.DecodeString("000102030406")
			targetPort := uint16(1002)

			srvConn, clientConn := net.Pipe()

			HandleConnectingPeer(ctx, hub, clientConn, srcMac, srcPort, targetMac, targetPort)

			_ = srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := srvConn.Read([]byte{})
			assert.ErrorIs(t, err, io.EOF, "Connection should be closed when target not found")
		})
	})

	t.Run("HandleAcceptingPeer", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			targetMac, _ := hex.DecodeString("000102030406")
			targetPort := uint16(1002)
			targetKey := newPeerKey(targetMac, targetPort)
			srvConnT, clientConnT := net.Pipe()
			defer func() { assert.NoError(t, srvConnT.Close()) }()
			defer func() { assert.NoError(t, clientConnT.Close()) }()

			pTarget := &peer{
				peerType:  peerTypeConnecting,
				macAddr:   [6]byte(targetMac),
				port:      targetPort,
				key:       targetKey,
				conn:      clientConnT,
				ctx:       ctx,
				ctxCancel: func() {},
				txQueue:   make(chan []byte, 16),
			}
			hub.registerConnected <- pTarget
			go pTarget.writer()

			assert.Eventually(t, func() bool {
				return hub.findConnectedPeer(targetKey) != nil
			}, 1*time.Second, 10*time.Millisecond, "Target peer should be registered before accepting")

			srcMac, _ := hex.DecodeString("000102030405")
			srcPort := uint16(1001)
			srcKey := newPeerKey(srcMac, srcPort)
			srvConnS, clientConnS := net.Pipe()
			defer func() { assert.NoError(t, srvConnS.Close()) }()
			defer func() { assert.NoError(t, clientConnS.Close()) }()

			HandleAcceptingPeer(ctx, hub, clientConnS, srcMac, srcPort, targetMac, targetPort)

			receivedSrcKey := make([]byte, 10)
			_ = srvConnT.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, err := io.ReadFull(srvConnT, receivedSrcKey)
			require.NoError(t, err, "Target peer should receive source peer key")
			assert.Equal(t, srcKey.encode(), receivedSrcKey)

			receivedTargetKey := make([]byte, 10)
			_ = srvConnS.SetReadDeadline(time.Now().Add(1 * time.Second))
			_, err = io.ReadFull(srvConnS, receivedTargetKey)
			require.NoError(t, err, "Source peer should receive target peer key")
			assert.Equal(t, targetKey.encode(), receivedTargetKey)

			assert.Eventually(t, func() bool {
				pS := hub.findConnectedPeer(srcKey)
				pT := hub.findConnectedPeer(targetKey)
				return pS != nil && pT != nil && pS.peer == pT && pT.peer == pS
			}, 1*time.Second, 10*time.Millisecond, "Expected both peers to be connected and interlinked with each other")
		})

		t.Run("NotFound", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			srcMac, _ := hex.DecodeString("000102030405")
			srcPort := uint16(1001)
			targetMac, _ := hex.DecodeString("000102030406")
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

			targetMac, _ := hex.DecodeString("000102030406")
			targetPort := uint16(1002)
			targetKey := newPeerKey(targetMac, targetPort)

			pTarget := &peer{
				peerType: peerTypeConnecting,
				key:      targetKey,
				peer:     &peer{}, // the hijack check only looks if the target peer is already connected to another
			}
			hub.connectedPeers[targetKey] = pTarget

			srcMac, _ := hex.DecodeString("000102030405")
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

			mac, _ := hex.DecodeString("000102030405")
			port := uint16(1001)
			key := newPeerKey(mac, port)

			srvConn, clientConn := net.Pipe()
			defer func() { assert.NoError(t, srvConn.Close()) }()

			p := &peer{
				peerType:  peerTypeListening,
				macAddr:   [6]byte(mac),
				port:      port,
				key:       key,
				conn:      clientConn,
				ctx:       ctx,
				ctxCancel: func() {},
			}

			hub.registerListening <- p

			assert.Eventually(t, func() bool {
				return hub.findListeningPeer(key) != nil
			}, 1*time.Second, 10*time.Millisecond, "Expected listening peer to be registered")

			hub.unregister <- p

			assert.Eventually(t, func() bool {
				return hub.findListeningPeer(key) == nil
			}, 1*time.Second, 10*time.Millisecond, "Expected listening peer to be unregistered")
		})

		t.Run("UnregisterConnecting", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			hub := NewHub(ctx)

			macS, _ := hex.DecodeString("000102030405")
			keyS := newPeerKey(macS, 1001)
			srvConnS, clientConnS := net.Pipe()
			defer func() { assert.NoError(t, srvConnS.Close()) }()

			macT, _ := hex.DecodeString("000102030406")
			keyT := newPeerKey(macT, 1002)
			srvConnT, clientConnT := net.Pipe()
			defer func() { assert.NoError(t, srvConnT.Close()) }()

			ctxS, cancelS := context.WithCancel(ctx)
			pS := &peer{
				peerType:  peerTypeConnecting,
				key:       keyS,
				conn:      clientConnS,
				ctx:       ctxS,
				ctxCancel: cancelS,
			}

			ctxT, cancelT := context.WithCancel(ctx)
			pT := &peer{
				peerType:  peerTypeConnecting,
				key:       keyT,
				conn:      clientConnT,
				ctx:       ctxT,
				ctxCancel: cancelT,
				peer:      pS,
			}
			pS.peer = pT

			hub.registerConnected <- pS
			hub.registerConnected <- pT

			assert.Eventually(t, func() bool {
				return hub.findConnectedPeer(keyS) != nil && hub.findConnectedPeer(keyT) != nil
			}, 1*time.Second, 10*time.Millisecond)

			hub.unregister <- pS

			assert.Eventually(t, func() bool {
				return hub.findConnectedPeer(keyS) == nil && hub.findConnectedPeer(keyT) == nil
			}, 1*time.Second, 10*time.Millisecond)

			_ = srvConnS.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, errS := srvConnS.Read([]byte{})
			assert.ErrorIs(t, errS, io.EOF, "Expected peer connection to be closed")

			_ = srvConnT.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, errT := srvConnT.Read([]byte{})
			assert.ErrorIs(t, errT, io.EOF, "Expected peer connection to be closed")
		})
	})
}
