package adhocctl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sprawl-relay/internal/titledb"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type client struct {
	conn      net.Conn
	ctx       context.Context
	ctxCancel context.CancelFunc
	closeOnce sync.Once
	server    *ctlServer
	macAddr   macAddr
	name      displayName
	titleId   titledb.TitleId // the titleId the client is playing
	group     *group          // the group the client is currently in
	txQueue   chan any
}

func registerClient(srv *ctlServer, conn net.Conn, mac macAddr, name displayName, tid titledb.TitleId) *client {
	ctx, cancelFunc := context.WithCancel(srv.ctx)

	c := &client{
		conn:      conn,
		ctx:       ctx,
		ctxCancel: cancelFunc,
		closeOnce: sync.Once{},
		macAddr:   mac,
		server:    srv,
		titleId:   tid,
		name:      name,
		txQueue:   make(chan any, 32),
	}

	go c.reader()
	go c.writer()
	return c
}

func (c *client) remoteAddr() string {
	return c.conn.RemoteAddr().String()
}

func (c *client) remoteAddr4() uint32 {
	tcpAddr := c.conn.RemoteAddr().(*net.TCPAddr)
	return binary.LittleEndian.Uint32(tcpAddr.IP.To4())
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		c.ctxCancel()

		if err := c.conn.Close(); err != nil {
			log.Warn().
				Str("ip", c.remoteAddr()).
				Err(err).
				Msg("[ctl] failed to close connection")
		}

		if c.group != nil {
			// Trigger a disconnect broadcast
			c.group.remove(c)
			c.group = nil
		}
		log.Info().
			Str("ip", c.remoteAddr()).
			Str("mac", c.macAddr.String()).
			Msg("[ctl] client disconnected")
	})
}

func (c *client) read(buf []byte) error {
	_, err := io.ReadFull(c.conn, buf)
	return err
}

func (c *client) write(packet any, timeout time.Duration) error {
	select {
	case c.txQueue <- packet:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("tx queue full: timeout exceeded when writing to mac: %s", c.macAddr.String())
	}
}

func (c *client) reader() {
	pktBuff := make([]byte, 256)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			var packetTypeId uint8

			if err := binary.Read(c.conn, binary.LittleEndian, &packetTypeId); err != nil {
				if err != io.EOF {
					log.Warn().
						Err(err).
						Str("ip", c.remoteAddr()).
						Msg("[ctl] reader: client read error")
				}
				c.close()
				return
			}

			if int(packetTypeId) > len(c2sPacketTypes) {
				log.Warn().Msgf("[ctl] reader: unknown packet type id: %d", packetTypeId)
				c.close()
				return
			}
			pktType := c2sPacketTypes[packetTypeId]

			if pktType == packetTypePing {
				continue
			}

			if pktType == (packetType{}) {
				log.Warn().Msgf("[ctl] reader: unexpected packet type id: %d", packetTypeId)
				c.close()
				return
			}

			if err := c.read(pktBuff[:pktType.size]); err != nil {
				if err != io.EOF {
					log.Warn().
						Err(err).
						Str("ip", c.remoteAddr()).
						Msg("[ctl] reader: client read error")
				}
				c.close()
				return
			}

			handlePacketErr := func(err error) {
				log.Warn().
					Err(err).
					Str("ip", c.remoteAddr()).
					Str("mac", c.macAddr.String()).
					Msg("[ctl] reader: client handle packet error")
				c.close()
			}

			switch pktType {
			case packetTypeConnect:
				if err := handleConnectPacket(c, pktBuff[:pktType.size]); err != nil {
					handlePacketErr(err)
					return
				}
			case packetTypeDisconnect:
				if err := handleDisconnectPacket(c); err != nil {
					handlePacketErr(err)
					return
				}
			case packetTypeScan:
				if err := handleScanPacket(c); err != nil {
					handlePacketErr(err)
					return
				}
			case packetTypeChat:
				if err := handleChatPacket(c, pktBuff[:pktType.size]); err != nil {
					handlePacketErr(err)
					return
				}
			}
		}
	}
}

func handleConnectPacket(cli *client, payload []byte) error {
	var gid = groupId(payload)

	log.Debug().
		Str("ip", cli.remoteAddr()).
		Str("mac", cli.macAddr.String()).
		Str("groupId", gid.String()).
		Msg("[ctl] connect: handing packet")

	if !gid.valid() {
		return errors.New("connect: invalid group id")
	}

	if cli.group != nil {
		return errors.New("connect: already connected to a group")
	}

	if err := cli.server.joinGroup(cli, cli.titleId, gid); err != nil {
		return err
	}
	packet := s2cConnectBssid{
		basePacket: basePacket{
			packetTypeId: packetTypeConnectBssid.id,
		},
		macAddr: cli.macAddr,
	}
	if err := cli.write(packet, writePacketTimeout); err != nil {
		return err
	}

	log.Info().
		Str("ip", cli.remoteAddr()).
		Str("mac", cli.macAddr.String()).
		Str("groupId", gid.String()).
		Msg("[ctl] connect: connected client to group")
	return nil
}

func handleDisconnectPacket(cli *client) error {
	log.Debug().
		Str("ip", cli.remoteAddr()).
		Str("mac", cli.macAddr.String()).
		Msg("[ctl] disconnect: handing packet")

	if cli.group == nil {
		return errors.New("disconnect: client is not connected to any group")
	}
	gid := cli.group.id

	cli.group.remove(cli)
	cli.group = nil

	log.Info().
		Str("ip", cli.remoteAddr()).
		Str("mac", cli.macAddr.String()).
		Str("groupId", gid.String()).
		Msg("[ctl] disconnect: disconnected client from group")
	return nil
}

func handleScanPacket(cli *client) error {
	if cli.group != nil {
		return errors.New("scan: client tried to scan while already connected to a group")
	}
	// finds all the available groups for the game and then reply with repeat([name] [founderMacAddr])
	// then send scan_complete packet
	// send scan complete

	log.Info().
		Str("ip", cli.remoteAddr()).
		Str("mac", cli.macAddr.String()).
		Str("titleId", cli.titleId.String()).
		Msg("[ctl] scan: requested scan")

	cli.server.scanGroups(cli.titleId, cli)

	return nil
}

func handleChatPacket(cli *client, payload []byte) error {
	log.Debug().
		Str("ip", cli.remoteAddr()).
		Str("mac", cli.macAddr.String()).
		Msg("[ctl] chat: handing packet")

	if cli.group == nil {
		return errors.New("chat: client is not connected to any group")
	}
	// TODO: Validate the message? aemu doesn't

	packet := &s2cChat{
		basePacket: basePacket{
			packetTypeId: packetTypeChat.id,
		},
		message:     payload,
		displayName: cli.name,
	}

	cli.group.broadcast(packet, func(c *client) bool {
		return c != cli
	})
	return nil
}

func (c *client) writer() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case packet := <-c.txQueue:
			if err := binary.Write(c.conn, binary.LittleEndian, packet); err != nil {
				log.Warn().
					Err(err).
					Str("ip", c.remoteAddr()).
					Str("mac", c.macAddr.String()).
					Msg("[ctl] writer: failed to write data to the client")
			}
		}
	}
}
