package exchange

import (
	"context"
	"errors"
	"io"
	"net"
	"sprawl/internal/pspnet"
	"sync"
	"syscall"

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
	drainTxQueue := func() {
		for {
			select {
			case pkt := <-c.txQueue:
				pkt.release()
			default:
				return
			}
		}
	}

	c.closeOnce.Do(func() {
		c.ctxCancel()
		drainTxQueue()

		if err := c.conn.Close(); err != nil {
			c.logger.Warn().
				Err(err).
				Msg("[exchange] failed to close connection")
		}
		c.exchange.unregister <- c

		c.logger.Info().Msg("[exchange] client disconnected")
	})
}

func (c *client) write(pkt *pdpPacket) error {
	select {
	case <-c.ctx.Done():
		return ErrClientDisconnected
	case c.txQueue <- pkt:
		return nil
	default:
		return errors.New("writer: tx queue full")
	}
}

func (c *client) reader() {
	for {
		pkt, err := c.decoder.next()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
			case errors.Is(err, net.ErrClosed):
			case errors.Is(err, syscall.ECONNRESET):
			default:
				c.logger.Warn().Err(err).Msg("[exchange] reader: client read error")
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
				c.logger.Warn().Err(err).Msg("[exchange] writer: failed to write data to the client")
				pkt.release()
				c.close()
				continue
			}
			pkt.release()
			clientsTxBytes.Add(float64(pdpHeaderSize + pkt.packetSize))
		}
	}
}
