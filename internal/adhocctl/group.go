package adhocctl

import (
	"sprawl-relay/internal/titledb"
	"sync"

	"github.com/rs/zerolog/log"
)

type group struct {
	sync.RWMutex
	id             groupId
	tid            titledb.TitleId
	founderMacAddr macAddr
	srv            *ctlServer
	clients        map[*client]struct{}
}

func (g *group) add(c *client) error {
	g.Lock()
	defer g.Unlock()
	g.clients[c] = struct{}{}
	return nil
}

func (g *group) remove(c *client) {
	g.Lock()
	defer g.Unlock()

	delete(g.clients, c)

	if len(g.clients) == 0 {
		g.srv.disbandGroup(g)
		return
	}

	if c.macAddr == g.founderMacAddr {
		for newLeader := range g.clients {
			log.Debug().Str("groupId", g.id.String()).Msgf("[ctl] group remove: Group founder left, electing %s as the new leader", newLeader.macAddr.String())
			g.founderMacAddr = newLeader.macAddr
			break
		}
	}
}

func (g *group) forEach(consumer func(*client)) {
	g.RLock()
	defer g.RUnlock()

	for c := range g.clients {
		consumer(c)
	}
}

func (g *group) broadcast(packet any, filter func(*client) bool) {
	g.forEach(func(c *client) {
		if filter(c) {
			if err := c.write(packet, writePacketTimeout); err != nil {
				c.logger.Warn().Err(err).Msg("[ctl] group broadcast: write to client error")
				c.close()
			}
		}
	})
}

func (g *group) broadcastAll(packet any) {
	g.forEach(func(c *client) {
		if err := c.write(packet, writePacketTimeout); err != nil {
			c.logger.Warn().Err(err).Msg("[ctl] group broadcast: write to client error")
			c.close()
		}
	})
}
