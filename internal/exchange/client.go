package exchange

import (
	"context"
	"errors"
	"io"
	"net"
	"sprawl/internal/pspnet"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

var ErrClientDisconnected = errors.New("client disconnected")

type client struct {
	exchange   *Exchange
	conn       net.Conn
	decoder    *decoder
	encoder    *encoder
	ctx        context.Context
	ctxCancel  context.CancelFunc
	closeOnce  sync.Once
	txQueue    chan *pdpPacket
	macAddr    pspnet.MacAddr
	port       uint16
	routingKey routingKey
	logger     zerolog.Logger
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		c.ctxCancel()

		if err := c.conn.Close(); err != nil {
			c.logger.Warn().
				Err(err).
				Msg("[relay] failed to close connection")
		}
		c.exchange.unregister <- c

		c.logger.Info().Msg("[relay] client disconnected")
	})
}

func (c *client) write(pkt *pdpPacket, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-c.ctx.Done():
		return ErrClientDisconnected
	case c.txQueue <- pkt:
		return nil
	case <-timer.C:
		return errors.New("tx queue full: timeout exceeded")
	}
}

func (c *client) reader() {
	for {
		pkt, err := c.decoder.next()
		if err != nil {
			if err != io.EOF {
				c.logger.Warn().Err(err).Msg("[relay] reader: client read error")
			}
			c.close()
			return
		}
		clientsRxBytes.Add(float64(pdpHeaderSize + pkt.packetSize))
		targetMacAddr := pkt.macAddr
		targetPort := pkt.port

		// Re-route the pdp packet
		pkt.macAddr = c.macAddr
		pkt.port = c.port

		c.exchange.routePacket(targetMacAddr, targetPort, pkt)
	}
}

func (c *client) writer() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case pkt := <-c.txQueue:
			if err := c.encoder.writeAndFlush(pkt); err != nil {
				c.logger.Warn().Err(err).Msg("[relay] writer: failed to write data to the client")
			}
			clientsTxBytes.Add(float64(pdpHeaderSize + pkt.packetSize))
		}
	}
}
