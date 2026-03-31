package peerhub

import (
	"context"
	"encoding/hex"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const listeningPeerTimeout = 30 * time.Second

type PeerHub struct {
	sync.RWMutex
	ctx            context.Context
	listeningPeers map[peerKey]*peer
	connectedPeers map[peerKey]*peer

	registerListening chan *peer
	registerConnected chan *peer
	unregister        chan *peer
}

func NewHub(ctx context.Context) *PeerHub {
	hub := &PeerHub{
		ctx:               ctx,
		listeningPeers:    make(map[peerKey]*peer),
		connectedPeers:    make(map[peerKey]*peer),
		registerListening: make(chan *peer, 16),
		registerConnected: make(chan *peer, 16),
		unregister:        make(chan *peer, 16),
	}

	go hub.houseKeeper()
	return hub
}

func (hub *PeerHub) findConnectedPeer(key peerKey) *peer {
	hub.RLock()
	defer hub.RUnlock()
	return hub.connectedPeers[key]
}

func (hub *PeerHub) findListeningPeer(key peerKey) *peer {
	hub.RLock()
	defer hub.RUnlock()
	return hub.listeningPeers[key]
}

func (hub *PeerHub) houseKeeper() {
	for {
		select {
		case <-hub.ctx.Done():
			return
		case p := <-hub.registerListening:
			hub.Lock()
			if _, ok := hub.listeningPeers[p.key]; ok {
				log.Warn().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("attempted to register duplicate listening peer")
				p.close()
				continue
			}
			hub.listeningPeers[p.key] = p
			hub.Unlock()
			listeningPeersCount.Inc()
		case p := <-hub.registerConnected:
			hub.Lock()
			if _, ok := hub.connectedPeers[p.key]; ok {
				log.Warn().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("attempted to register duplicate connected peer")
				p.close()
				continue
			}
			hub.connectedPeers[p.key] = p
			hub.Unlock()
			connectedPeersCount.Inc()
		case p := <-hub.unregister:
			switch p.peerType {
			case peerTypeListening:
				hub.Lock()
				if _, ok := hub.listeningPeers[p.key]; ok {
					delete(hub.listeningPeers, p.key)
					listeningPeersCount.Dec()
				}
				hub.Unlock()
			case peerTypeConnecting:
				hub.Lock()
				if _, ok := hub.connectedPeers[p.key]; ok {
					delete(hub.connectedPeers, p.key)
					connectedPeersCount.Dec()
				}
				hub.Unlock()
			}
		}
	}
}

func HandleListeningPeer(ctx context.Context, hub *PeerHub, conn net.Conn, macAddr []byte, port uint16) {
	ctx, cancelFunc := context.WithTimeout(ctx, listeningPeerTimeout)

	p := RegisterPeer(ctx, cancelFunc, hub, conn, macAddr, port, peerTypeListening)

	log.Info().
		Str("ip", p.remoteAddr()).
		Str("key", p.key.String()).
		Msg("registered a new listening peer to the hub")
}

func HandleConnectingPeer(ctx context.Context, hub *PeerHub, conn net.Conn, srcAddr []byte, srcPort uint16, targetAddr []byte, targetPort uint16) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().
				Str("ip", conn.RemoteAddr().String()).
				Err(err).
				Msg("failed to close connection")
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("closed connection")
	}
	const maxRetries = 5
	const retryDelay = 500 * time.Millisecond

	var targetPeer *peer

	srcPeerKey := newPeerKey(srcAddr, srcPort)
	targetPeerKey := newPeerKey(targetAddr, targetPort)

	for range maxRetries {
		targetPeer = hub.findListeningPeer(targetPeerKey)
		if targetPeer != nil {
			break
		}
		time.Sleep(retryDelay)
	}

	if targetPeer == nil {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Str("src-key", srcPeerKey.String()).
			Str("target-key", targetPeerKey.String()).
			Msg("target peer not found after retries")
		go connCloser(conn)
		return
	}

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("src-key", srcPeerKey.String()).
		Str("target-key", targetPeerKey.String()).
		Msg("target peer found to connect")

	log.Debug().Msgf("notifying %v that it should connect to %v on port %d", hex.EncodeToString(targetPeer.macAddr[:]), hex.EncodeToString(srcAddr[:]), srcPort)
	targetPeer.txQueue <- srcPeerKey.encode()

	ctx, cancelFunc := context.WithCancel(ctx)

	p := RegisterPeer(ctx, cancelFunc, hub, conn, srcAddr, srcPort, peerTypeConnecting)

	log.Info().
		Str("ip", p.remoteAddr()).
		Str("key", p.key.String()).
		Msg("registered a new connected peer to the hub")
}

func HandleAcceptingPeer(ctx context.Context, hub *PeerHub, conn net.Conn, srcAddr []byte, srcPort uint16, targetAddr []byte, targetPort uint16) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().
				Str("ip", conn.RemoteAddr().String()).
				Err(err).
				Msg("failed to close connection")
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("closed connection")
	}
	srcPeerKey := newPeerKey(srcAddr, srcPort)
	targetPeerKey := newPeerKey(targetAddr, targetPort)

	targetPeer := hub.findConnectedPeer(targetPeerKey)

	if targetPeer == nil {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Str("src-key", srcPeerKey.String()).
			Str("target-key", targetPeerKey.String()).
			Msg("target peer not found to accept connection")
		go connCloser(conn)
		return
	}

	if targetPeer.peer != nil {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Str("src-key", srcPeerKey.String()).
			Str("target-key", targetPeerKey.String()).
			Msg("attempted to hijack peer connection, disconnecting...")
		go connCloser(conn)
		return
	}
	ctx, cancelFunc := context.WithCancel(ctx)

	p := RegisterPeer(ctx, cancelFunc, hub, conn, srcAddr, srcPort, peerTypeConnecting)
	p.peer = targetPeer
	targetPeer.peer = p

	if err := p.write(targetPeerKey.encode(), writePacketTimeout); err != nil {
		log.Warn().Err(err).Msg("failed to write accept conn packet to peer")
		hub.unregister <- p
		return
	}
	if err := targetPeer.write(srcPeerKey.encode(), writePacketTimeout); err != nil {
		log.Warn().Err(err).Msg("failed to write accept conn packet to peer")
		hub.unregister <- p
		return
	}

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("src-key", srcPeerKey.String()).
		Str("target-key", targetPeerKey.String()).
		Msg("peer accepted connection")
}
