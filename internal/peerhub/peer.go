package peerhub

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const p2pHeaderSize = 4
const p2pMaxPacketSize = 10 * 1024

const readPacketTimeout = 5 * time.Second
const writePacketTimeout = 2 * time.Second

type peerType int8

const peerTypeListening peerType = 1
const peerTypeConnecting peerType = 2

var ErrPeerDisconnected = errors.New("peer disconnected")

type peerKey [8]byte

func newPeerKey(mac []byte, port uint16) peerKey {
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
	txQueue   chan []byte
	macAddr   [6]byte
	port      uint16
	key       peerKey
	peerType  peerType
	peer      *peer
}

func RegisterPeer(ctx context.Context, cancelFunc context.CancelFunc, hub *PeerHub, conn net.Conn, macAddr []byte, port uint16, pType peerType) *peer {
	p := &peer{
		hub:       hub,
		conn:      conn,
		ctx:       ctx,
		ctxCancel: cancelFunc,
		closeOnce: sync.Once{},
		txQueue:   make(chan []byte, 16),
		macAddr:   [6]byte(macAddr),
		port:      port,
		key:       newPeerKey(macAddr, port),
		peerType:  pType,
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

func (p *peer) remoteAddr() string {
	return p.conn.RemoteAddr().String()
}

func (p *peer) close() {
	p.closeOnce.Do(func() {
		p.ctxCancel()

		if err := p.conn.Close(); err != nil {
			log.Warn().
				Str("ip", p.remoteAddr()).
				Err(err).
				Msg("[relay] failed to close connection")
		}
		p.hub.unregister <- p

		if p.peer != nil {
			other := p.peer
			p.peer = nil
			other.peer = nil
			go other.close()
		}

		log.Info().
			Str("ip", p.remoteAddr()).
			Msg("[relay] peer disconnected")
	})
}

func (p *peer) read(buf []byte) error {
	_, err := io.ReadFull(p.conn, buf)
	return err
}

func (p *peer) readWithTimeout(buf []byte, timeout time.Duration) error {
	if err := p.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err := io.ReadFull(p.conn, buf)
	_ = p.conn.SetReadDeadline(time.Time{})

	return err
}

func (p *peer) write(buf []byte, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-p.ctx.Done():
		return ErrPeerDisconnected
	case p.txQueue <- buf:
		return nil
	case <-timer.C:
		return errors.New("tx queue full: timeout exceeded")
	}
}

func (p *peer) reader() {
	headerBuf := make([]byte, p2pHeaderSize)

	for {
		select {
		case <-p.ctx.Done():
			if p.peerType == peerTypeListening && errors.Is(p.ctx.Err(), context.DeadlineExceeded) {
				log.Debug().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("[relay] reader: no connection attempt made to listening peer, closing connection.")

				p.close()
			} else {
				log.Debug().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("[relay] reader: context cancelled")
			}
			return
		default:
			if err := p.read(headerBuf); err != nil {
				if err != io.EOF {
					log.Warn().
						Err(err).
						Str("ip", p.remoteAddr()).
						Str("key", p.key.String()).
						Msg("[relay] reader: client read error")
				}
				p.close()
				return
			}
			packetSize := binary.LittleEndian.Uint32(headerBuf[:])

			if packetSize > p2pMaxPacketSize {
				log.Warn().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Uint32("packet-size", packetSize).
					Msg("[relay] reader: client packed size exceeds limit")
				p.close()
				return
			}
			packetBuf := make([]byte, p2pHeaderSize+packetSize)
			copy(packetBuf[0:4], headerBuf[:]) // copy the packet size

			if err := p.readWithTimeout(packetBuf[p2pHeaderSize:], readPacketTimeout); err != nil {
				log.Warn().
					Err(err).
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("[relay] reader: client read timeout")
				p.close()
				return
			}

			if p.peer == nil {
				log.Warn().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("[relay] reader: peer has no peer, discarding packet")
				break
			}

			if err := p.peer.write(packetBuf, writePacketTimeout); err != nil {
				if !errors.Is(err, ErrPeerDisconnected) {
					log.Warn().Err(err).Msg("[relay] failed to write packet to peer")
				}
				p.close()
			}
			peersRxBytes.Add(float64(len(packetBuf)))
		}
	}
}

func (p *peer) writer() {
	for {
		select {
		case <-p.ctx.Done():
			log.Debug().
				Str("ip", p.remoteAddr()).
				Str("key", p.key.String()).
				Msg("[relay] writer: context cancelled")
			return
		case payload := <-p.txQueue:
			if _, err := p.conn.Write(payload); err != nil {
				log.Warn().
					Err(err).
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("[relay] writer: failed to write data to the client")
			}
			peersTxBytes.Add(float64(len(payload)))
		}
	}
}
