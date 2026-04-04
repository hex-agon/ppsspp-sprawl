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
		log.Debug().Msgf("[ctl] group add: Notifying %s about the new %s client", other.macAddr.String(), c.macAddr.String())
		if err := other.write(newUserPacket, writePacketTimeout); err != nil {
			return err
		}

		log.Debug().Msgf("[ctl] group add: Notifying %s about the existing %s client", c.macAddr.String(), other.macAddr.String())
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

	delete(g.clients, c)

	if len(g.clients) == 0 {
		g.srv.disbandGroup(g)
		return
	}
	log.Debug().Msgf("[ctl] group remove: Notifying that %s left the group", c.macAddr.String())

	// If there are still players left in the group, let's notify them that someone disconnected
	packet := s2cDisconnected{
		s2cBasePacket: s2cBasePacket{
			packetTypeId: packetTypeDisconnect.id,
		},
		ipAddr: c.remoteAddr4(),
	}

	// can't use broadcast due to locks
	for c := range g.clients {
		if err := c.write(packet, writePacketTimeout); err != nil {
			log.Warn().
				Err(err).
				Str("ip", c.remoteAddr()).
				Str("mac", c.macAddr.String()).
				Msg("[ctl] group broadcast: write to client error")
			c.close()
		}
	}

	if c.macAddr == g.founderMacAddr {
		for newLeader := range g.clients {
			log.Debug().
				Str("groupId", g.id.String()).
				Msgf("[ctl] group remove: Group founder left, electing %s as the new leader", newLeader.macAddr.String())
			g.founderMacAddr = newLeader.macAddr
			break
		}
	}
}

func (g *group) broadcast(packet any) {
	g.RLock()
	defer g.RUnlock()

	for c := range g.clients {
		if err := c.write(packet, writePacketTimeout); err != nil {
			log.Warn().
				Err(err).
				Str("ip", c.remoteAddr()).
				Str("mac", c.macAddr.String()).
				Msg("[ctl] group broadcast: write to client error")
			c.close()
		}
	}
}
