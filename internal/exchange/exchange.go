package exchange

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const pdpHeaderSize = 14
const pdpMaxPacketSize = 10 * 1024

const readPacketTimeout = 30 * time.Second
const writePacketTimeout = 2 * time.Second

// Exchange represents a hub for managing client connections and routing messages between them.
type Exchange struct {
	sync.RWMutex
	ctx        context.Context
	clients    map[routingKey]*client
	register   chan *client
	unregister chan *client
}

func NewExchange(ctx context.Context) *Exchange {
	ex := &Exchange{
		ctx:        ctx,
		clients:    make(map[routingKey]*client),
		register:   make(chan *client, 32),
		unregister: make(chan *client, 32),
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
			return
		case cli := <-ex.register:
			if cli.routingKey == (routingKey{}) {
				log.Warn().Str("ip", cli.remoteAddr()).Msg("client has empty routing key")
				go cli.close()
				break
			}

			ex.Lock()
			if _, ok := ex.clients[cli.routingKey]; ok {
				log.Warn().
					Str("ip", cli.remoteAddr()).
					Str("key", cli.routingKey.String()).
					Msg("duplicate client registration attempt")

				go cli.close()
				ex.Unlock()
				break
			}
			ex.clients[cli.routingKey] = cli
			clientsCount.Inc()
			ex.Unlock()
		case cli := <-ex.unregister:
			go cli.close()

			ex.Lock()
			if _, ok := ex.clients[cli.routingKey]; ok {
				delete(ex.clients, cli.routingKey)
				clientsCount.Dec()
			}
			ex.Unlock()
		}
	}
}

func (ex *Exchange) routePacket(key routingKey, payload []byte) {
	if cli := ex.findClient(key); cli != nil {
		if err := cli.write(payload, writePacketTimeout); err != nil {
			log.Warn().
				Err(err).
				Str("ip", cli.remoteAddr()).
				Str("key", cli.routingKey.String()).
				Msg("failed to write packet")
			ex.unregister <- cli
		}
		return
	}
}
