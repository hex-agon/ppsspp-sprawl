package adhocctl

import (
	"bytes"
	"sprawl/internal/pspnet"
	"sprawl/internal/titledb"
	"sync"

	"github.com/rs/zerolog/log"
)

const groupIdLen = 8

type groupId [groupIdLen]byte

func (g *groupId) String() string {
	terminator := bytes.IndexByte(g[:], 0)
	if terminator == -1 {
		return string(g[:])
	}
	return string(g[:terminator])
}

func (g *groupId) valid() bool {
	length := bytes.IndexByte(g[:], 0)

	if length == -1 {
		length = groupIdLen
	}

	for i := 0; i < length; i++ {
		char := g[i]
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

type group struct {
	sync.RWMutex
	id             groupId
	tid            titledb.TitleId
	founderMacAddr pspnet.MacAddr
	srv            *ctlServer
	clients        map[*client]struct{}
	createdAt      int64
}

func (g *group) add(c *client) error {
	g.Lock()
	defer g.Unlock()
	g.clients[c] = struct{}{}
	return nil
}

// remove removes the specified client from the group and elects a new leader if the removed client was the founder.
// It returns true if the group becomes empty after the removal.
func (g *group) remove(c *client) bool {
	g.Lock()
	defer g.Unlock()

	delete(g.clients, c)

	if c.macAddr == g.founderMacAddr {
		for newLeader := range g.clients {
			log.Debug().Str("groupId", g.id.String()).Msgf("[ctl] group remove: Group founder left, electing %s as the new leader", newLeader.macAddr.String())
			g.founderMacAddr = newLeader.macAddr
			break
		}
	}
	return len(g.clients) == 0
}

func (g *group) forEach(consumer func(*client)) {
	g.RLock()
	defer g.RUnlock()

	for c := range g.clients {
		consumer(c)
	}
}

func (g *group) broadcast(packet outboundPacket, filter func(*client) bool) {
	g.forEach(func(c *client) {
		if filter(c) {
			if err := c.write(packet); err != nil {
				c.logger.Warn().Err(err).Msg("[ctl] group broadcast: write to client error")
				c.close()
			}
		}
	})
}
