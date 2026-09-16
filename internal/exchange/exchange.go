package exchange

import (
	"context"
	"errors"
	"net"
	"sprawl/internal/pspnet"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"
)

const pdpHeaderSize = 14
const pdpMaxPacketSize = 10 * 1024

// Exchange represents a hub for managing client connections and routing messages between them.
type Exchange struct {
	sync.RWMutex
	ctx        context.Context
	clients    map[routingKey]*client
	register   chan *client
	unregister chan *client
}

type ClientInfo struct {
	MAC  string `json:"mac"`
	Port uint16 `json:"port"`
	IP   string `json:"ip"`
}

func (ex *Exchange) GetClients() []ClientInfo {
	ex.RLock()
	defer ex.RUnlock()

	result := make([]ClientInfo, 0, len(ex.clients))
	for _, cli := range ex.clients {
		result = append(result, ClientInfo{
			MAC:  net.HardwareAddr(cli.macAddr[:]).String(),
			Port: cli.port,
			IP:   cli.conn.RemoteAddr().String(),
		})
	}
	return result
}

func NewExchange(ctx context.Context) *Exchange {
	ex := &Exchange{
		ctx:        ctx,
		clients:    make(map[routingKey]*client),
		register:   make(chan *client, 32),
		unregister: make(chan *client, 128),
	}
	go ex.houseKeeper()
	return ex
}

func (ex *Exchange) findClient(key routingKey) *client {
	ex.RLock()
	defer ex.RUnlock()
	return ex.clients[key]
}

func (ex *Exchange) houseKeeper() {
	for {
		select {
		case <-ex.ctx.Done():
			ex.Lock()
			for key, cli := range ex.clients {
				delete(ex.clients, key)
				go cli.closeConn()
			}
			ex.Unlock()
			return
		case c := <-ex.register:
			if c.routingKey == (routingKey{}) {
				c.logger.Warn().Msg("[exchange] client has empty routing key")
				go c.closeConn()
				break
			}

			ex.Lock()
			if _, ok := ex.clients[c.routingKey]; ok {
				c.logger.Warn().Msg("[exchange] duplicate client registration attempt")

				go c.closeConn()
				ex.Unlock()
				break
			}
			ex.clients[c.routingKey] = c
			clientsCount.Inc()
			ex.Unlock()
		case cli := <-ex.unregister:
			ex.Lock()
			if _, ok := ex.clients[cli.routingKey]; ok {
				delete(ex.clients, cli.routingKey)
				clientsCount.Dec()
				cli.logger.Info().Msg("[exchange] unregistered client")
			}
			ex.Unlock()
			go cli.closeConn()
		}
	}
}

// RegisterClient registers a new client, initializes its context, binds it to a specific exchange, and starts reader and writer goroutines for communication.
func (ex *Exchange) RegisterClient(conn net.Conn, macAddr pspnet.MacAddr, port uint16) {
	cancellableCtx, cancelFunc := context.WithCancel(ex.ctx)

	c := &client{
		exchange:   ex,
		conn:       conn,
		decoder:    newDecoder(conn),
		encoder:    newEncoder(conn, pdpHeaderSize+pdpMaxPacketSize),
		ctx:        cancellableCtx,
		ctxCancel:  cancelFunc,
		closeOnce:  sync.Once{},
		closed:     atomic.Bool{},
		writerDone: make(chan struct{}),
		txQueue:    make(chan *pdpPacket, 16),
		macAddr:    macAddr,
		port:       port,
		routingKey: newRoutingKey(macAddr, port),
		logger: log.With().
			Str("ip", conn.RemoteAddr().String()).
			Str("mac", macAddr.String()).
			Uint16("port", port).
			Logger(),
	}

	ex.register <- c
	go c.reader()
	go c.writer()

	c.logger.Info().Msg("[exchange] registered a new client to the exchange")
}

func (ex *Exchange) routePacket(targetMacAddr pspnet.MacAddr, targetPort uint16, pkt *pdpPacket) {
	key := newRoutingKey(targetMacAddr, targetPort)

	if c := ex.findClient(key); c != nil {
		if err := c.write(pkt); err != nil {
			if !errors.Is(err, ErrClientClosed) {
				c.logger.Warn().Err(err).Msg("[exchange] failed to write packet")
			}
			pkt.release()
			c.close()
		}
		return
	}
	pkt.release()
}
