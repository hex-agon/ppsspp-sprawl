package peerhub

import (
	"context"
	"encoding/hex"
	"net"
	"sprawl/internal/pspnet"
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

type PeerInfo struct {
	MAC         string `json:"mac"`
	Port        uint16 `json:"port"`
	IP          string `json:"ip"`
	ConnectedTo string `json:"connected_to,omitzero"`
}

func formatMAC(mac [6]byte) string {
	return net.HardwareAddr(mac[:]).String()
}

func (hub *PeerHub) GetPeers() (listening []PeerInfo, connected []PeerInfo) {
	hub.RLock()
	defer hub.RUnlock()

	listening = make([]PeerInfo, 0, len(hub.listeningPeers))
	for _, p := range hub.listeningPeers {
		listening = append(listening, PeerInfo{
			MAC:  formatMAC(p.macAddr),
			Port: p.port,
			IP:   p.conn.RemoteAddr().String(),
		})
	}

	connected = make([]PeerInfo, 0, len(hub.connectedPeers))
	for _, p := range hub.connectedPeers {
		info := PeerInfo{
			MAC:  formatMAC(p.macAddr),
			Port: p.port,
			IP:   p.conn.RemoteAddr().String(),
		}
		if p.peer != nil {
			info.ConnectedTo = formatMAC(p.peer.macAddr)
		}
		connected = append(connected, info)
	}

	return listening, connected
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
				p.logger.Warn().Msg("[relay] attempted to register duplicate listening peer")
				p.close()
				continue
			}
			hub.listeningPeers[p.key] = p
			hub.Unlock()
			listeningPeersCount.Inc()
		case p := <-hub.registerConnected:
			hub.Lock()
			if _, ok := hub.connectedPeers[p.key]; ok {
				p.logger.Warn().Msg("[relay] attempted to register duplicate connected peer")
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

func HandleListeningPeer(ctx context.Context, hub *PeerHub, conn net.Conn, macAddr pspnet.MacAddr, port uint16) {
	ctx, cancelFunc := context.WithCancel(ctx)

	p := registerPeer(ctx, cancelFunc, hub, conn, macAddr, port, peerTypeListening)

	p.logger.Info().Msg("[relay] registered a new listening peer to the hub")
}

func HandleConnectingPeer(ctx context.Context, hub *PeerHub, conn net.Conn, srcAddr pspnet.MacAddr, srcPort uint16, targetAddr pspnet.MacAddr, targetPort uint16) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().
				Str("ip", conn.RemoteAddr().String()).
				Err(err).
				Msg("[relay] failed to close connection")
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("[relay] closed connection")
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
			Msg("[relay] target peer not found after retries")
		go connCloser(conn)
		return
	}

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("src-key", srcPeerKey.String()).
		Str("target-key", targetPeerKey.String()).
		Msg("[relay] target peer found to connect")

	log.Debug().Msgf("[relay] notifying %v that it should connect to %v on port %d", hex.EncodeToString(targetPeer.macAddr[:]), hex.EncodeToString(srcAddr[:]), srcPort)
	targetPeer.txQueue <- srcPeerKey.encode()

	ctx, cancelFunc := context.WithCancel(ctx)

	p := registerPeer(ctx, cancelFunc, hub, conn, srcAddr, srcPort, peerTypeConnecting)

	log.Info().
		Str("ip", p.remoteAddr()).
		Str("key", p.key.String()).
		Msg("[relay] registered a new connected peer to the hub")
}

func HandleAcceptingPeer(ctx context.Context, hub *PeerHub, conn net.Conn, srcAddr pspnet.MacAddr, srcPort uint16, targetAddr pspnet.MacAddr, targetPort uint16) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().
				Str("ip", conn.RemoteAddr().String()).
				Err(err).
				Msg("[relay] failed to close connection")
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("[relay] closed connection")
	}
	srcPeerKey := newPeerKey(srcAddr, srcPort)
	targetPeerKey := newPeerKey(targetAddr, targetPort)

	targetPeer := hub.findConnectedPeer(targetPeerKey)

	if targetPeer == nil {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Str("src-key", srcPeerKey.String()).
			Str("target-key", targetPeerKey.String()).
			Msg("[relay] target peer not found to accept connection")
		go connCloser(conn)
		return
	}

	if targetPeer.peer != nil {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Str("src-key", srcPeerKey.String()).
			Str("target-key", targetPeerKey.String()).
			Msg("[relay] attempted to hijack peer connection, disconnecting...")
		go connCloser(conn)
		return
	}
	ctx, cancelFunc := context.WithCancel(ctx)

	p := registerPeer(ctx, cancelFunc, hub, conn, srcAddr, srcPort, peerTypeConnecting)
	p.peer = targetPeer
	targetPeer.peer = p

	if err := p.write(targetPeerKey.encode(), writePacketTimeout); err != nil {
		log.Warn().Err(err).Msg("[relay] failed to write accept conn packet to peer")
		hub.unregister <- p
		return
	}
	if err := targetPeer.write(srcPeerKey.encode(), writePacketTimeout); err != nil {
		log.Warn().Err(err).Msg("[relay] failed to write accept conn packet to peer")
		hub.unregister <- p
		return
	}

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("src-key", srcPeerKey.String()).
		Str("target-key", targetPeerKey.String()).
		Msg("[relay] peer accepted connection")
}
