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

func (g *group) add(c *client) error {
	g.Lock()
	defer g.Unlock()

	// TODO: Should probably move this to the client but then we have the problem of having to skip the new client
	newUserPacket := &s2cClientConnected{
		s2cBasePacket: s2cBasePacket{
			packetTypeId: packetTypeConnect.id,
		},
		displayName: c.name,
		macAddr:     c.macAddr,
		ipAddr:      c.remoteAddr4(),
	}

	for other := range g.clients {
		// notify the other client about the new client
		log.Debug().Msgf("[ctl] group add: Notifying %s about the new %s client", other.macAddr, c.macAddr)
		if err := other.write(newUserPacket, writePacketTimeout); err != nil {
			return err
		}

		log.Debug().Msgf("[ctl] group add: Notifying %s about the existing %s client", c.macAddr, other.macAddr)
		// notify the client about the existing client
		otherUserPacket := &s2cClientConnected{
			s2cBasePacket: s2cBasePacket{
				packetTypeId: packetTypeConnect.id,
			},
			displayName: other.name,
			macAddr:     other.macAddr,
			ipAddr:      other.remoteAddr4(),
		}
		if err := c.write(otherUserPacket, writePacketTimeout); err != nil {
			return err
		}
	}
	g.clients[c] = struct{}{}
	return nil
}

func (g *group) remove(c *client) {
	g.Lock()
	defer g.Unlock()

	// TODO: likely need to elect a new group leader here?
	delete(g.clients, c)

	if len(g.clients) == 0 {
		g.srv.disbandGroup(g)
	}
}

func (g *group) broadcast(packet any) {
	g.RLock()
	defer g.RUnlock()

	for c := range g.clients {
		if err := c.write(packet, 100*time.Millisecond); err != nil {
			log.Warn().
				Err(err).
				Str("ip", c.remoteAddr()).
				Str("mac", c.macAddr.String()).
				Msg("[ctl] group broadcast: write to client error")
			c.close()
		}
	}
}
