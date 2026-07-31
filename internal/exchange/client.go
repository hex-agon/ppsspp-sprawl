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
	"time"

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
	c.closeOnce.Do(func() {
		c.logger.Info().Msg("[exchange] closing client")
		// best-effort to prevent more packets from being queued
		c.closed.Store(true)
		// causes the writer goroutine to exit
		c.ctxCancel()
		// queues the client to be unregistered so that packets become unroutable
		c.exchange.unregister <- c

		c.logger.Info().Msg("[exchange] waiting for client writer to finish")
		timer := time.NewTimer(1 * time.Second)
		defer timer.Stop()

		// then we wait at most 1 second before closing the house down...
		select {
		case <-c.writerDone:
		case <-timer.C:
			c.logger.Warn().Msg("timeout waiting for writer to close, closing connection")
			break
		}

		if err := c.conn.Close(); err != nil {
			c.logger.Warn().
				Err(err).
				Msg("[exchange] failed to close connection")
		}
		c.logger.Info().Msg("[exchange] client closed")
	})
}

func (c *client) write(pkt *pdpPacket) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
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
