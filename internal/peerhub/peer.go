package peerhub

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sprawl/internal/pspnet"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const p2pHeaderSize = 4
const p2pMaxPacketSize = 10 * 1024

const writePacketTimeout = 2 * time.Second

type peerType int8

const peerTypeListening peerType = 1
const peerTypeConnecting peerType = 2

func (t peerType) String() string {
	switch t {
	case peerTypeListening:
		return "LISTENING"
	case peerTypeConnecting:
		return "CONNECTING"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

var ErrPeerDisconnected = errors.New("peer disconnected")

type peerKey [8]byte

func newPeerKey(mac pspnet.MacAddr, port uint16) peerKey {
	bytes := make([]byte, 8)
	copy(bytes[:6], mac[:])
	binary.LittleEndian.PutUint16(bytes[6:8], port)
	return peerKey(bytes[:])
}

func (key *peerKey) encode() []byte {
	bytes := make([]byte, 10)
	copy(bytes[:6], key[:6])
	copy(bytes[8:10], key[6:8])
	return bytes
}

func (key *peerKey) String() string {
	return hex.EncodeToString(key[:])
}

type peer struct {
	hub       *PeerHub
	conn      net.Conn
	ctx       context.Context
	ctxCancel context.CancelFunc
	closeOnce sync.Once
	txQueue   chan p2pOutboundPacket
	macAddr   pspnet.MacAddr
	port      uint16
	key       peerKey
	peerType  peerType
	peer      *peer
	logger    zerolog.Logger
	encoder   *encoder
	decoder   *decoder
}

func (p *peer) close() {
	p.closeOnce.Do(func() {
		p.ctxCancel()

		if err := p.conn.Close(); err != nil {
			p.logger.Err(err).Msg("[relay] failed to close connection")
		}
		p.hub.unregister <- p

		if p.peer != nil {
			other := p.peer
			p.peer = nil
			other.peer = nil
			go other.close()
		}

		p.logger.Info().Msg("[relay] peer disconnected")
	})
}

func (p *peer) write(pkt p2pOutboundPacket, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-p.ctx.Done():
		return ErrPeerDisconnected
	case p.txQueue <- pkt:
		return nil
	case <-timer.C:
		return errors.New("tx queue full: timeout exceeded")
	}
}

func (p *peer) reader() {
	for {
		pkt, err := p.decoder.next()
		if err != nil {
			if err != io.EOF {
				p.logger.Warn().Err(err).Msg("[relay] reader: client read error")
			}
			p.close()
			return
		}

		if p.peer == nil {
			p.logger.Warn().Msg("[relay] reader: peer has no peer, discarding packet")
			continue
		}

		if err := p.peer.write(pkt, writePacketTimeout); err != nil {
			if !errors.Is(err, ErrPeerDisconnected) {
				p.logger.Warn().Err(err).Msg("[relay] failed to write packet to peer")
			}
			p.close()
		}
	}
}

func (p *peer) writer() {
	for {
		select {
		case <-p.ctx.Done():
			p.logger.Debug().Msg("[relay] writer: context cancelled")
			return
		case pkt := <-p.txQueue:
			if err := p.encoder.writeAndFlush(pkt); err != nil {
				p.logger.Warn().Err(err).Msg("[relay] writer: failed to write data to the client")
			}
		}
	}
}
