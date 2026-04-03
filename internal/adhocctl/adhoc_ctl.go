package adhocctl

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const loginPacketTimeout = 5 * time.Second

const writePacketTimeout = 5 * time.Second

type Server interface {
	Shutdown() error
}
type ctlServer struct {
	sync.RWMutex
	ctx        context.Context
	cancelFunc context.CancelFunc
	listener   net.Listener
	groups     map[titleId]map[groupId]*group
	unregister chan *group
}

func (srv *ctlServer) Shutdown() error {
	srv.cancelFunc()
	return srv.listener.Close()
}

func NewCtlServer(addr string, port string) (Server, error) {
	ctx, ctxCancel := context.WithCancel(context.Background())

	listenConfig := net.ListenConfig{
		KeepAliveConfig: net.KeepAliveConfig{
			Enable: true,
		},
	}
	listener, err := listenConfig.Listen(ctx, "tcp", addr+":"+port)

	if err != nil {
		ctxCancel()
		return nil, err
	}

	srv := &ctlServer{
		listener:   listener,
		ctx:        ctx,
		cancelFunc: ctxCancel,
		groups:     make(map[titleId]map[groupId]*group),
		unregister: make(chan *group, 16),
	}

	go srv.connAcceptor()
	go srv.houseKeeper()
	return srv, nil
}

func (srv *ctlServer) houseKeeper() {
	for {
		select {
		case <-srv.ctx.Done():
			return
		case g := <-srv.unregister:
			srv.Lock()
			if groups, ok := srv.groups[g.tid]; ok {
				delete(groups, g.id)
			}
			srv.Unlock()
		}
	}
}

func (srv *ctlServer) joinGroup(cli *client, tid titleId, id groupId) {
	srv.Lock()
	defer srv.Unlock()

	var grp *group

	if groups, ok := srv.groups[tid]; ok {
		if g, ok := groups[id]; ok {
			grp = g
		}
	}

	// create a new group if it doesn't exist
	if grp == nil {
		grp = &group{
			srv:            srv,
			id:             id,
			tid:            tid,
			founderMacAddr: cli.macAddr,
			clients:        make(map[*client]struct{}),
		}
		if srv.groups[tid] == nil {
			srv.groups[tid] = make(map[groupId]*group)
		}
		srv.groups[tid][id] = grp

		log.Info().
			Str("groupId", grp.id.String()).
			Str("founder", grp.founderMacAddr.String()).
			Msg("join-group: formed a new group")
	}
	grp.add(cli)
	cli.group = grp
}

func (srv *ctlServer) disbandGroup(g *group) {
	srv.Lock()
	defer srv.Unlock()

	if groups, ok := srv.groups[g.tid]; ok {
		delete(groups, g.id)
		log.Info().
			Str("groupId", g.id.String()).
			Msg("join-group: disbanded group")
	}
}

func (srv *ctlServer) scanGroups(tid titleId, requester *client) {
	srv.RLock()
	defer srv.RUnlock()

	if groups, ok := srv.groups[tid]; ok {
		for _, g := range groups {
			scanReply := s2cScanReply{
				s2cBasePacket: s2cBasePacket{
					packetTypeId: packetTypeScan.id,
				},
				gid:         g.id,
				founderAddr: g.founderMacAddr,
			}

			if err := requester.write(scanReply, writePacketTimeout); err != nil {
				requester.close()
				return
			}

		}
	}
	scanComplete := s2cScanComplete{
		s2cBasePacket: s2cBasePacket{
			packetTypeId: packetTypeScanComplete.id,
		},
	}
	if err := requester.write(scanComplete, writePacketTimeout); err != nil {
		requester.close()
		return
	}
}

func (srv *ctlServer) connAcceptor() {
	for {
		conn, err := srv.listener.Accept()

		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Warn().Err(err).Msg("failed to accept connection")
			}
			continue
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("accepted new connection")

		go srv.waitForLoginPacket(conn)
	}
}

func (srv *ctlServer) waitForLoginPacket(conn net.Conn) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().
				Str("ip", conn.RemoteAddr().String()).
				Err(err).
				Msg("failed to close connection")
		}
		log.Debug().Str("ip", conn.RemoteAddr().String()).Msg("init: closed connection")
	}
	if err := conn.SetReadDeadline(time.Now().Add(loginPacketTimeout)); err != nil {
		go connCloser(conn)
		return
	}
	var packetTypeId uint8

	if err := binary.Read(conn, binary.LittleEndian, &packetTypeId); err != nil {
		go connCloser(conn)
		return
	}

	if packetTypeId != packetTypeLogin.id {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Msgf("init: unexpected packet type during login: %d", packetTypeId)
		go connCloser(conn)
		return
	}
	loginBuff := make([]byte, packetTypeLogin.size)

	if _, err := io.ReadFull(conn, loginBuff); err != nil {
		go connCloser(conn)
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	var loginPacket struct {
		ClientAddr macAddr
		Name       displayName
		Tid        titleId
	}

	if err := binary.Read(bytes.NewReader(loginBuff), binary.LittleEndian, &loginPacket); err != nil {
		log.Debug().Err(err).Msg("init: failed to read packet")
		go connCloser(conn)
		return
	}

	if !loginPacket.Name.valid() || !loginPacket.Tid.valid() {
		log.Debug().
			Any("mac", loginPacket.ClientAddr).
			Any("name", loginPacket.Name).
			Any("tid", loginPacket.Tid).
			Msg("init: invalid login packet")
		go connCloser(conn)
		return
	}
	registerClient(srv, conn, loginPacket.ClientAddr, loginPacket.Name, loginPacket.Tid)

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("mac", loginPacket.ClientAddr.String()).
		Str("name", loginPacket.Name.String()).
		Str("tid", loginPacket.Tid.String()).
		Msg("client logged in")
}
