package exchange

import (
	"context"
	"errors"
	"io"
	"net"
	"sprawl/internal/pspnet"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/rs/zerolog"
)

var ErrClientClosed = errors.New("client closed")
var ErrClientTxQueueFull = errors.New("client tx queue full")

type client struct {
	exchange   *Exchange
	conn       net.Conn
	decoder    *decoder
	encoder    *encoder
	ctx        context.Context
	ctxCancel  context.CancelFunc
	closeOnce  sync.Once
	closed     atomic.Bool
	writerDone chan struct{}
	txQueue    chan *pdpPacket
	macAddr    pspnet.MacAddr
	port       uint16
	routingKey routingKey
	logger     zerolog.Logger
}

func (c *client) close() {
	go c.closeOnce.Do(func() {
		// immediately queue to unregister so no more packets are routed
		c.exchange.unregister <- c

		c.ctxCancel()

		// wait for writer to be done
		<-c.writerDone

		if err := c.conn.Close(); err != nil {
			c.logger.Warn().
				Err(err).
				Msg("[exchange] failed to close connection")
		}

		c.logger.Info().Msg("[exchange] client disconnected")
	})
}

func (c *client) write(pkt *pdpPacket) error {
	select {
	case c.txQueue <- pkt:
		return nil
	default:
		return ErrClientTxQueueFull
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

		pkt.macAddr = c.macAddr
		pkt.port = c.port

		c.exchange.routePacket(targetMacAddr, targetPort, pkt)
	}
}

func (c *client) writer() {
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

	defer func() {
		drainTxQueue()
		close(c.writerDone)
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case pkt := <-c.txQueue:
			if err := c.encoder.writeAndFlush(pkt); err != nil {
				c.logger.Warn().Err(err).Msg("[exchange] writer: failed to write data to the client")
				pkt.release()
				c.close()
				return
			}
			clientsTxBytes.Add(float64(pdpHeaderSize + pkt.packetSize))
			pkt.release()
		}
	}
}
