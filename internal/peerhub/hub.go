package peerhub

import (
	"context"
	"net"
	"sprawl/internal/pspnet"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

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

func (hub *PeerHub) registerPeer(conn net.Conn, macAddr pspnet.MacAddr, port uint16, pType peerType) *peer {
	ctx, cancelFunc := context.WithCancel(hub.ctx)
	p := &peer{
		hub:       hub,
		conn:      conn,
		ctx:       ctx,
		ctxCancel: cancelFunc,
		closeOnce: sync.Once{},
		txQueue:   make(chan p2pOutboundPacket, 16),
		macAddr:   macAddr,
		port:      port,
		key:       newPeerKey(macAddr, port),
		peerType:  pType,
		logger: log.With().
			Str("ip", conn.RemoteAddr().String()).
			Str("mac", macAddr.String()).
			Str("peerType", pType.String()).
			Logger(),
		encoder: newEncoder(conn),
		decoder: newDecoder(conn),
	}

	if pType == peerTypeListening {
		hub.registerListening <- p
	} else {
		hub.registerConnected <- p
	}

	go p.reader()
	go p.writer()

	return p
}

func (hub *PeerHub) HandleListeningPeer(conn net.Conn, macAddr pspnet.MacAddr, port uint16) {
	p := hub.registerPeer(conn, macAddr, port, peerTypeListening)

	p.logger.Info().Msg("[relay] registered a new listening peer to the hub")
}

func (hub *PeerHub) HandleConnectingPeer(conn net.Conn, srcAddr pspnet.MacAddr, srcPort uint16, targetAddr pspnet.MacAddr, targetPort uint16) {
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

	log.Debug().
		Str("ip", conn.RemoteAddr().String()).
		Str("src-key", srcPeerKey.String()).
		Str("target-key", targetPeerKey.String()).
		Msg("[relay] target peer found to connect")

	log.Debug().Msgf("[relay] notifying %v that it should connect to %v on port %d", targetPeer.macAddr.String(), srcAddr.String(), srcPort)
	peerAnnouncePkt := &peerAnnouncePacket{
		macAddr: srcAddr,
		port:    srcPort,
	}
	if err := targetPeer.write(peerAnnouncePkt, time.Second*1); err != nil {
		targetPeer.logger.Err(err).Msg("[relay] failed to write peer announce packet")
		go connCloser(conn)
		return
	}

	p := hub.registerPeer(conn, srcAddr, srcPort, peerTypeConnecting)
	p.logger.Info().Msg("[relay] registered a new connected peer to the hub")
}

func (hub *PeerHub) HandleAcceptingPeer(conn net.Conn, srcAddr pspnet.MacAddr, srcPort uint16, targetAddr pspnet.MacAddr, targetPort uint16) {
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
	p := hub.registerPeer(conn, srcAddr, srcPort, peerTypeConnecting)
	p.peer = targetPeer
	targetPeer.peer = p

	announceTargetPeer := &peerAnnouncePacket{
		macAddr: targetAddr,
		port:    targetPort,
	}
	if err := p.write(announceTargetPeer, writePacketTimeout); err != nil {
		log.Warn().Err(err).Msg("[relay] failed to write accept conn packet to peer")
		hub.unregister <- p
		return
	}

	announceSrcPeer := &peerAnnouncePacket{
		macAddr: srcAddr,
		port:    srcPort,
	}
	if err := targetPeer.write(announceSrcPeer, writePacketTimeout); err != nil {
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
