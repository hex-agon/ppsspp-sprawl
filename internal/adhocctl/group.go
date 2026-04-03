package adhocctl

import (
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var ErrGroupDisbanded = errors.New("group has been disbanded")

type group struct {
	sync.RWMutex
	id             groupId
	tid            titleId
	founderMacAddr macAddr
	srv            *ctlServer
	clients        map[*client]struct{}
}

func (g *group) add(c *client) {
	g.Lock()
	defer g.Unlock()

	g.clients[c] = struct{}{}
}

func (g *group) remove(c *client) {
	g.Lock()
	defer g.Unlock()

	// TODO: likely need to elect a new group leader here?
	// TODO: there's an edge case where when a client is removed, the packet informs by ip, so if there's more than one client with the same ip address, you have to dc them all and then reconnect them to the group (at a network level)
	// TODO: if the group is empty it should be unregistered
	delete(g.clients, c)

	if len(g.clients) == 0 {
		g.srv.disbandGroup(g)
	}
}

func (g *group) broadcast(payload []byte) {
	g.RLock()
	defer g.RUnlock()

	for c := range g.clients {
		if err := c.write(payload, 100*time.Millisecond); err != nil {
			log.Warn().
				Err(err).
				Str("ip", c.remoteAddr()).
				Str("mac", c.macAddr.String()).
				Msg("[ctl] group broadcast: write to client error")
			c.close()
		}
	}
}
