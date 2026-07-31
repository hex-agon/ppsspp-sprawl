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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
)

const p2pHeaderSize = 4
const p2pMaxPacketSize = 10 * 1024

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
var ErrPeerTxQueueFull = errors.New("peer tx queue is full")

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
	hub        *PeerHub
	conn       net.Conn
	ctx        context.Context
	ctxCancel  context.CancelFunc
	closeOnce  sync.Once
	closed     atomic.Bool
	writerDone chan struct{}
	txQueue    chan p2pOutboundPacket
	macAddr    pspnet.MacAddr
	port       uint16
	key        peerKey
	peerType   peerType
	peer       *peer
	logger     zerolog.Logger
	encoder    *encoder
	decoder    *decoder
}

func (p *peer) close() {
	p.closeOnce.Do(func() {
		p.logger.Info().Msg("[hub] closing peer")
		// best-effort to prevent more packets from being queued
		p.closed.Store(true)
		// causes the writer goroutine to exit
		p.ctxCancel()
		// queues the client to be unregistered so that packets become unroutable
		p.hub.unregister <- p

		// tell the other peer to close too
		if p.peer != nil {
			p.logger.Info().Msg("[hub] closing the other peer")
			other := p.peer
			p.peer = nil
			other.peer = nil
			other.close()
		}

		p.logger.Info().Msg("[hub] waiting for peer writer to finish")
		timer := time.NewTimer(1 * time.Second)
		defer timer.Stop()

		// then we wait at most 1 second before closing the house down...
		select {
		case <-p.writerDone:
		case <-timer.C:
			p.logger.Warn().Msg("timeout waiting for writer to close, closing connection")
			break
		}

		if err := p.conn.Close(); err != nil {
			p.logger.Err(err).Msg("[hub] failed to close connection")
		}

		p.logger.Info().Msg("[hub] peer closed")
	})
}

func (p *peer) write(pkt p2pOutboundPacket) error {
	if p.closed.Load() {
		return ErrPeerDisconnected
	}
	select {
	case <-p.ctx.Done():
		return ErrPeerDisconnected
	case p.txQueue <- pkt:
		return nil
	default:
		return ErrPeerTxQueueFull
	}
}

func (p *peer) reader() {
	for {
		pkt, err := p.decoder.next()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
			case errors.Is(err, net.ErrClosed):
			case errors.Is(err, syscall.ECONNRESET):
			default:
				p.logger.Warn().Err(err).Msg("[hub] reader: client read error")
			}
			p.close()
			return
		}

		if p.peer == nil {
			p.logger.Warn().Msg("[hub] reader: peer has no peer, discarding packet")
			pkt.release()
			continue
		}

		if err := p.peer.write(pkt); err != nil {
			if !errors.Is(err, ErrPeerDisconnected) {
				p.logger.Warn().Err(err).Msg("[hub] failed to write packet to peer")
			}
			pkt.release()
			p.close()
		}
	}
}

func (p *peer) writer() {
	drainTxQueue := func() {
		for {
			select {
			case pkt := <-p.txQueue:
				pkt.release()
			default:
				return
			}
		}
	}
	defer func() {
		drainTxQueue()
		close(p.writerDone)
	}()

	for {
		select {
		case <-p.ctx.Done():
			return
		case pkt := <-p.txQueue:
			if err := p.encoder.writeAndFlush(pkt); err != nil {
				p.logger.Warn().Err(err).Msg("[hub] writer: failed to write data to the client")
				pkt.release()
				p.close()
				continue
			}
			pkt.release()
		}
	}
}
