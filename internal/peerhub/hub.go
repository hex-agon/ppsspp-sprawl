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
			hub.listeningPeers[p.key] = p
			hub.Unlock()
			listeningPeersCount.Inc()
		case p := <-hub.registerConnected:
			hub.Lock()
			hub.connectedPeers[p.key] = p
			hub.Unlock()
			connectedPeersCount.Inc()
		case p := <-hub.unregister:
			switch p.peerType {
			case peerTypeListening:
				hub.Lock()
				delete(hub.listeningPeers, p.key)
				hub.Unlock()
				listeningPeersCount.Dec()
			case peerTypeConnecting:
				go p.close()
				hub.Lock()
				delete(hub.connectedPeers, p.key)
				hub.Unlock()
				connectedPeersCount.Dec()

				if p.peer != nil {
					log.Debug().
						Str("key", p.peer.key.String()).
						Msg("closing peer as other was closed")
					go p.peer.close()
					hub.Lock()
					delete(hub.connectedPeers, p.peer.key)
					hub.Unlock()
					connectedPeersCount.Dec()
				}
			}
		}
	}
}

func RegisterListeningPeer(ctx context.Context, hub *PeerHub, conn net.Conn, macAddr []byte, port uint16) {
	ctx, cancelFunc := context.WithTimeout(ctx, listeningPeerTimeout)
	key := newPeerKey(macAddr, port)

	p := &peer{
		peerType:  peerTypeListening,
		macAddr:   [6]byte(macAddr),
		port:      port,
		key:       key,
		conn:      conn,
		ctx:       ctx,
		ctxCancel: cancelFunc,
		txQueue:   make(chan []byte, 16),
	}

	log.Info().
		Str("ip", p.remoteAddr()).
		Str("key", p.key.String()).
		Msg("registered a new listening peer to the hub")

	hub.registerListening <- p
	go p.reader(hub)
	go p.writer()
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

	p := &peer{
		peerType:  peerTypeConnecting,
		macAddr:   [6]byte(srcAddr),
		port:      srcPort,
		key:       srcPeerKey,
		conn:      conn,
		ctx:       ctx,
		ctxCancel: cancelFunc,
		txQueue:   make(chan []byte, 16),
	}

	hub.registerConnected <- p
	go p.reader(hub)
	go p.writer()

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

	p := &peer{
		peerType:  peerTypeConnecting,
		macAddr:   [6]byte(srcAddr),
		port:      srcPort,
		key:       srcPeerKey,
		conn:      conn,
		ctx:       ctx,
		ctxCancel: cancelFunc,
		txQueue:   make(chan []byte, 16),
		peer:      targetPeer,
	}
	targetPeer.peer = p

	hub.registerConnected <- p
	go p.reader(hub)
	go p.writer()

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
