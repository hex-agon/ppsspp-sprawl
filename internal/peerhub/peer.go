package peerhub

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
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
	conn      net.Conn
	ctx       context.Context
	ctxCancel context.CancelFunc
	txQueue   chan []byte
	macAddr   [6]byte
	port      uint16
	key       peerKey
	peerType  peerType
	peer      *peer
}

func (p *peer) remoteAddr() string {
	return p.conn.RemoteAddr().String()
}

func (p *peer) close() {
	p.ctxCancel()

	if err := p.conn.Close(); err != nil {
		log.Warn().
			Str("ip", p.remoteAddr()).
			Err(err).
			Msg("failed to close connection")
	}

	log.Info().
		Str("ip", p.remoteAddr()).
		Msg("peer disconnected")
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
	select {
	case p.txQueue <- buf:
		return nil
	case <-time.After(timeout):
		return errors.New("tx queue full: timeout exceeded")
	}
}

func (p *peer) reader(hub *PeerHub) {
	headerBuf := make([]byte, p2pHeaderSize)

	for {
		select {
		case <-p.ctx.Done():
			log.Debug().
				Str("ip", p.remoteAddr()).
				Str("key", p.key.String()).
				Msg("reader: context cancelled")
			return
		default:
			if err := p.read(headerBuf); err != nil {
				if err != io.EOF {
					log.Warn().
						Err(err).
						Str("ip", p.remoteAddr()).
						Str("key", p.key.String()).
						Msg("reader: client read error")
				}
				hub.unregister <- p
				return
			}
			packetSize := binary.LittleEndian.Uint32(headerBuf[:])

			if packetSize > p2pMaxPacketSize {
				log.Warn().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Uint32("packet-size", packetSize).
					Msg("reader: client packed size exceeds limit")
				hub.unregister <- p
				return
			}
			packetBuf := make([]byte, p2pHeaderSize+packetSize)
			copy(packetBuf[0:4], headerBuf[:]) // copy the packet size

			if err := p.readWithTimeout(packetBuf[p2pHeaderSize:], readPacketTimeout); err != nil {
				log.Warn().
					Err(err).
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("reader: client read timeout")
				hub.unregister <- p
				return
			}

			if p.peer == nil {
				log.Warn().
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("reader: peer has no peer, discarding packet")
				break
			}

			if err := p.peer.write(packetBuf, writePacketTimeout); err != nil {
				log.Warn().Err(err).Msg("failed to write packet to peer")
				hub.unregister <- p
			}
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
				Msg("writer: context cancelled")
			return
		case payload := <-p.txQueue:
			if _, err := p.conn.Write(payload); err != nil {
				log.Warn().
					Err(err).
					Str("ip", p.remoteAddr()).
					Str("key", p.key.String()).
					Msg("writer: failed to write data to the client")
			}
		}
	}
}
