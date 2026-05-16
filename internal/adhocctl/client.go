package adhocctl

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sprawl/internal/packet"
	"sprawl/internal/pspnet"
	"sprawl/internal/titledb"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
)

type client struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
	closeOnce sync.Once
	conn      net.Conn
	decoder   *packet.Decoder
	encoder   *packet.Encoder
	server    *ctlServer
	macAddr   pspnet.MacAddr
	name      pspnet.DisplayName
	titleId   titledb.TitleId // the titleId the client is playing
	group     *group          // the group the client is currently in
	txQueue   chan packet.OutboundPacket
	logger    zerolog.Logger
}

func (c *client) remoteAddr4() uint32 {
	tcpAddr := c.conn.RemoteAddr().(*net.TCPAddr)
	return binary.LittleEndian.Uint32(tcpAddr.IP.To4())
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		c.ctxCancel()

		if err := c.conn.Close(); err != nil {
			c.logger.Warn().Err(err).Msg("[ctl] failed to close connection")
		}

		c.server.unregisterClient <- c

		if c.group != nil {
			broadcastDisconnect(c)
			c.server.leaveGroup(c)
			c.group = nil
		}
		c.logger.Info().Msg("[ctl] client disconnected")
	})
}

func broadcastDisconnect(c *client) {
	// Since the protocol disconnects by ipv4 addr, we need to do a small workaround, similar to what aemu does
	sameIpCount := 0
	ipAddr := c.remoteAddr4()

	g := c.group
	g.RLock()
	defer g.RUnlock()

	var clientsToReconnect []*client
	// count everyone under the same ipAddr and gather them
	for other := range g.clients {
		if other.remoteAddr4() == ipAddr {
			if other != c {
				clientsToReconnect = append(clientsToReconnect, other)
			}
			sameIpCount++
		}
	}

	pkt := &s2cClientDisconnected{ipAddr: ipAddr}

	// repeat broadcast disconnecting all the clients under the same ipAddr
	for i := 0; i < sameIpCount; i++ {
		for other := range g.clients {
			if other == c { // no need to notify ourselves
				continue
			}

			if err := other.write(pkt); err != nil {
				other.logger.Warn().Err(err).Msg("[ctl] group broadcast: write to client error")
				other.close()
			}
		}
	}

	for _, reconnectingClient := range clientsToReconnect {
		for other := range g.clients {
			if other == reconnectingClient || other == c { // no need to notify itself
				continue
			}
			pkt := &s2cClientConnected{
				displayName: reconnectingClient.name,
				macAddr:     reconnectingClient.macAddr,
				ipAddr:      reconnectingClient.remoteAddr4(),
			}
			if err := other.write(pkt); err != nil {
				other.logger.Warn().Err(err).Msg("[ctl] group broadcast: write to client error")
				other.close()
			}
		}
	}
}

func (c *client) write(packet packet.OutboundPacket) error {
	select {
	case c.txQueue <- packet:
		return nil
	default:
		return errors.New("writer: tx queue full")
	}
}

func (c *client) reader() {
	for {
		inboundPkt, err := c.decoder.Next()

		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
			case errors.Is(err, net.ErrClosed):
			case errors.Is(err, syscall.ECONNRESET):
			default:
				c.logger.Warn().Err(err).Msg("[ctl] reader: client read error")
			}
			c.close()
			return
		}

		handleErr := func(err error) {
			c.logger.Warn().Err(err).Msg("[ctl] reader: packet handler error")
			c.close()
		}

		switch pkt := inboundPkt.(type) {
		case *c2sPing:
			break
		case *c2sConnect:
			if err = handleConnectPacket(c, pkt); err != nil {
				handleErr(err)
			}
		case *c2sDisconnect:
			if err = handleDisconnectPacket(c); err != nil {
				handleErr(err)
			}
		case *c2sScan:
			if err = handleScanPacket(c); err != nil {
				handleErr(err)
			}
		case *c2sChat:
			if err = handleChatPacket(c, pkt); err != nil {
				handleErr(err)
			}
		}
	}
}

func handleConnectPacket(c *client, pkt *c2sConnect) error {
	c.logger.Debug().Msg("[ctl] connect: handing packet")

	if !pkt.gid.valid() {
		return errors.New("connect: invalid group id: " + pkt.gid.String())
	}

	if c.group != nil {
		return errors.New("connect: already connected to a group")
	}

	if err := c.server.joinGroup(c, c.titleId, pkt.gid); err != nil {
		return err
	}

	// Synchronize group members
	newClientPacket := &s2cClientConnected{
		displayName: c.name,
		macAddr:     c.macAddr,
		ipAddr:      c.remoteAddr4(),
	}
	c.group.forEach(func(other *client) {
		if other == c {
			return
		}
		// Notify other about the new group member
		if err := other.write(newClientPacket); err != nil {
			other.logger.Warn().Err(err).Msg("[ctl] client: write error")
			other.close()
		}

		// Notify the new client about other group members
		otherClientPacket := &s2cClientConnected{
			displayName: other.name,
			macAddr:     other.macAddr,
			ipAddr:      other.remoteAddr4(),
		}

		if err := c.write(otherClientPacket); err != nil {
			c.logger.Warn().Err(err).Msg("[ctl] client: write error")
			c.close()
		}
	})

	if err := c.write(&s2cConnectBssid{macAddr: c.macAddr}); err != nil {
		return err
	}

	c.logger.Info().Msgf("[ctl] connect: connected client to group %s", pkt.gid.String())
	return nil
}

func handleDisconnectPacket(c *client) error {
	c.logger.Debug().Msg("[ctl] disconnect: handing packet")

	g := c.group
	if g == nil {
		return errors.New("disconnect: client is not connected to any group")
	}
	gid := g.id

	broadcastDisconnect(c)
	g.srv.leaveGroup(c)
	c.group = nil

	c.logger.Info().Msgf("[ctl] disconnect: disconnected client from group %s", gid.String())
	return nil
}

func handleScanPacket(c *client) error {
	if c.group != nil {
		return errors.New("scan: client tried to scan while already connected to a group")
	}
	// finds all the available groups for the game and then reply with repeat([name] [founderMacAddr])
	// then send scan_complete packet
	// send scan complete

	c.logger.Info().Msgf("[ctl] scan: requested scan for tileId=%s", c.titleId.String())

	c.server.scanGroups(c.titleId, func(g *group) {
		scanReply := &s2cScanReply{
			gid:         g.id,
			founderAddr: g.founderMacAddr,
		}

		if err := c.write(scanReply); err != nil {
			c.logger.Warn().Err(err).Msg("[ctl] client: write error")
			c.close()
		}
	})

	if err := c.write(&s2cScanComplete{}); err != nil {
		return err
	}
	return nil
}

func handleChatPacket(c *client, pkt *c2sChat) error {
	c.logger.Debug().Msg("[ctl] chat: handing packet")

	if c.group == nil {
		return errors.New("chat: client is not connected to any group")
	}

	chatBroadcast := &s2cChat{
		message:     pkt.message,
		displayName: c.name,
	}
	c.group.broadcast(chatBroadcast, func(other *client) bool { return c != other })
	return nil
}

func (c *client) writer() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case pkt := <-c.txQueue:
			if err := c.encoder.WriteAndFlush(pkt); err != nil {
				c.logger.Warn().Err(err).Msg("[ctl] writer: failed to write data to the client")
			}
		}
	}
}
