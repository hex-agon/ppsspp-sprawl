package peerhub

import (
	"context"
	"encoding/hex"
	"net"
	"time"

	"github.com/rs/zerolog/log"
)

type PeerHub struct {
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

func (hub *PeerHub) houseKeeper() {
	for {
		select {
		case <-hub.ctx.Done():
			return
		case p := <-hub.registerListening:
			hub.listeningPeers[p.key] = p
		case p := <-hub.registerConnected:
			hub.connectedPeers[p.key] = p
		case p := <-hub.unregister:
			switch p.peerType {
			case peerTypeListening:
				delete(hub.listeningPeers, p.key)
			case peerTypeConnecting:
				go p.close()
				delete(hub.connectedPeers, p.key)
				if p.peer != nil {
					log.Debug().
						Str("key", p.peer.key.String()).
						Msg("closing peer as other was closed")
					go p.peer.close()
					delete(hub.connectedPeers, p.key)
				}
			}
		}
	}
}

func RegisterListeningPeer(ctx context.Context, hub *PeerHub, conn net.Conn, macAddr []byte, port uint16) {
	ctx, cancelFunc := context.WithCancel(ctx)
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

	// Retry finding the target peer
	for i := 0; i < maxRetries; i++ {
		if hub.listeningPeers[targetPeerKey] != nil {
			targetPeer = hub.listeningPeers[targetPeerKey]
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

	targetPeer := hub.connectedPeers[targetPeerKey]

	if targetPeer == nil {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Str("src-key", srcPeerKey.String()).
			Str("target-key", targetPeerKey.String()).
			Msg("target peer not found to accept connection")
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
	}
	if err := targetPeer.write(srcPeerKey.encode(), writePacketTimeout); err != nil {
		log.Warn().Err(err).Msg("failed to write accept conn packet to peer")
	}

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("src-key", srcPeerKey.String()).
		Str("target-key", targetPeerKey.String()).
		Msg("peer accepted connection")
}
