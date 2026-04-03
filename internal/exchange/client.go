package exchange

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

// RoutingKey is composed of the client's informed mac address (6 bytes) and it's port
type routingKey [8]byte

func newRoutingKey(mac []byte, port uint16) routingKey {
	bytes := make([]byte, 8)
	copy(bytes[:6], mac[:])
	binary.LittleEndian.PutUint16(bytes[6:8], port)
	return routingKey(bytes[:])
}

func (key routingKey) String() string {
	return hex.EncodeToString(key[:])
}

type client struct {
	exchange   *Exchange
	conn       net.Conn
	ctx        context.Context
	ctxCancel  context.CancelFunc
	closeOnce  sync.Once
	txQueue    chan []byte
	macAddr    [6]byte
	port       uint16
	routingKey routingKey
}

// RegisterClient registers a new client, initializes its context, binds it to a specific exchange, and starts reader and writer goroutines for communication.
func RegisterClient(ctx context.Context, exchange *Exchange, conn net.Conn, macAddr []byte, port uint16) {
	ctx, cancelFunc := context.WithCancel(ctx)

	key := make([]byte, 8)
	copy(key, macAddr[:])
	binary.LittleEndian.PutUint16(key[6:], port)

	cli := &client{
		exchange:   exchange,
		conn:       conn,
		ctx:        ctx,
		ctxCancel:  cancelFunc,
		closeOnce:  sync.Once{},
		txQueue:    make(chan []byte, 16),
		macAddr:    [6]byte(macAddr),
		port:       port,
		routingKey: routingKey(key),
	}

	exchange.register <- cli
	go cli.reader()
	go cli.writer()

	log.Info().
		Str("ip", cli.remoteAddr()).
		Str("key", cli.routingKey.String()).
		Msg("[relay] registered a new client to the exchange")
}

func (c *client) remoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		c.ctxCancel()

		if err := c.conn.Close(); err != nil {
			log.Warn().
				Str("ip", c.remoteAddr()).
				Str("client-key", c.routingKey.String()).
				Err(err).
				Msg("[relay] failed to close connection")
		}
		c.exchange.unregister <- c

		log.Info().
			Str("ip", c.remoteAddr()).
			Str("client-key", c.routingKey.String()).
			Msg("[relay] client disconnected")
	})
}

func (c *client) read(buf []byte) error {
	_, err := io.ReadFull(c.conn, buf)
	return err
}

func (c *client) readWithTimeout(buf []byte, timeout time.Duration) error {
	if err := c.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err := io.ReadFull(c.conn, buf)
	_ = c.conn.SetReadDeadline(time.Time{})

	return err
}

func (c *client) write(buf []byte, timeout time.Duration) error {
	select {
	case c.txQueue <- buf:
		return nil
	case <-time.After(timeout):
		return errors.New("tx queue full: timeout exceeded")
	}
}

func (c *client) reader() {
	headerBuf := make([]byte, pdpHeaderSize)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := c.read(headerBuf); err != nil {

				if err != io.EOF {
					log.Warn().
						Err(err).
						Str("ip", c.remoteAddr()).
						Str("key", c.routingKey.String()).
						Msg("[relay] reader: client read error")
				}
				c.close()
				return
			}
			targetMacAddr := headerBuf[:6]
			targetPort := binary.LittleEndian.Uint16(headerBuf[8:])
			packetSize := binary.LittleEndian.Uint32(headerBuf[10:])

			if packetSize > pdpMaxPacketSize {
				log.Warn().
					Str("ip", c.remoteAddr()).
					Str("key", c.routingKey.String()).
					Uint32("packet-size", packetSize).
					Msg("[relay] reader: client packed size exceeds limit")

				c.close()
				return
			}
			packetBuf := make([]byte, pdpHeaderSize+packetSize)

			copy(packetBuf[0:6], c.macAddr[:])
			binary.LittleEndian.PutUint16(packetBuf[8:10], c.port)
			binary.LittleEndian.PutUint32(packetBuf[10:14], packetSize)

			if err := c.readWithTimeout(packetBuf[pdpHeaderSize:], readPacketTimeout); err != nil {
				log.Warn().
					Err(err).
					Str("ip", c.remoteAddr()).
					Str("key", c.routingKey.String()).
					Msg("[relay] reader: client read timeout")

				c.close()
				return
			}
			clientsRxBytes.Add(float64(len(packetBuf)))

			targetKey := newRoutingKey(targetMacAddr, targetPort)
			c.exchange.routePacket(targetKey, packetBuf)
		}
	}
}

func (c *client) writer() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case payload := <-c.txQueue:
			if _, err := c.conn.Write(payload); err != nil {
				log.Warn().
					Err(err).
					Str("ip", c.remoteAddr()).
					Str("key", c.routingKey.String()).
					Msg("[relay] writer: failed to write data to the client")
			}
			clientsTxBytes.Add(float64(len(payload)))
		}
	}
}
