package adhocctl

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sprawl-relay/internal/titledb"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const loginPacketTimeout = 5 * time.Second

const writePacketTimeout = 5 * time.Second

type GroupStatus struct {
	ID      string   `json:"id"`
	Players []string `json:"players"`
}

type TitleStatus struct {
	TitleID     string        `json:"title_id"`
	TitleRegion string        `json:"title_region"`
	Name        string        `json:"name"`
	Groups      []GroupStatus `json:"groups"`
}

type Server interface {
	Shutdown() error
	GetActiveGroups() []TitleStatus
}
type ctlServer struct {
	sync.RWMutex
	ctx        context.Context
	cancelFunc context.CancelFunc
	listener   net.Listener
	groups     map[titledb.TitleId]map[groupId]*group
	unregister chan *group
}

func (srv *ctlServer) GetActiveGroups() []TitleStatus {
	srv.RLock()
	defer srv.RUnlock()

	result := make([]TitleStatus, 0, len(srv.groups))

	for tid, groups := range srv.groups {
		if len(groups) == 0 {
			continue
		}

		titleStatus := TitleStatus{
			TitleID: tid.String(),
			Groups:  make([]GroupStatus, 0, len(groups)),
		}

		if title, ok := titledb.Titles[tid]; ok {
			titleStatus.Name = title.Name
			titleStatus.TitleRegion = title.Region
		}

		for gid, g := range groups {
			g.RLock()
			groupStatus := GroupStatus{
				ID:      gid.String(),
				Players: make([]string, 0, len(g.clients)),
			}
			for c := range g.clients {
				groupStatus.Players = append(groupStatus.Players, c.name.String())
			}
			g.RUnlock()
			titleStatus.Groups = append(titleStatus.Groups, groupStatus)
		}
		result = append(result, titleStatus)
	}

	return result
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
		groups:     make(map[titledb.TitleId]map[groupId]*group),
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

func (srv *ctlServer) joinGroup(cli *client, tid titledb.TitleId, id groupId) error {
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
			Msg("[ctl] join-group: formed a new group")
	}
	if err := grp.add(cli); err != nil {
		return err
	}
	cli.group = grp
	return nil
}

func (srv *ctlServer) disbandGroup(g *group) {
	srv.Lock()
	defer srv.Unlock()

	if groups, ok := srv.groups[g.tid]; ok {
		delete(groups, g.id)
		log.Info().
			Str("groupId", g.id.String()).
			Msg("[ctl] join-group: disbanded group")
	}
}

func (srv *ctlServer) scanGroups(tid titledb.TitleId, requester *client) {
	srv.RLock()
	defer srv.RUnlock()

	if groups, ok := srv.groups[tid]; ok {
		for _, g := range groups {
			scanReply := s2cScanReply{
				basePacket: basePacket{
					packetTypeId: packetTypeScan.id,
				},
				gid:         g.id,
				founderAddr: g.founderMacAddr,
			}

			log.Debug().Msgf("[ctl] scanGroups: sending group %s (founder: %s) to %s", g.id.String(), g.founderMacAddr.String(), requester.macAddr.String())
			if err := requester.write(scanReply, writePacketTimeout); err != nil {
				requester.close()
				return
			}

		}
	}
	scanComplete := s2cScanComplete{
		basePacket: basePacket{
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
				log.Warn().Err(err).Msg("[ctl] failed to accept connection")
			}
			continue
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("[ctl] accepted new connection")

		go srv.waitForLoginPacket(conn)
	}
}

func (srv *ctlServer) waitForLoginPacket(conn net.Conn) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().
				Str("ip", conn.RemoteAddr().String()).
				Err(err).
				Msg("[ctl] failed to close connection")
		}
		log.Debug().Str("ip", conn.RemoteAddr().String()).Msg("[ctl] init: closed connection")
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
			Msgf("[ctl] init: unexpected packet type during login: %d", packetTypeId)
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
		Tid        titledb.TitleId
	}

	if err := binary.Read(bytes.NewReader(loginBuff), binary.LittleEndian, &loginPacket); err != nil {
		log.Debug().Err(err).Msg("[ctl] init: failed to read packet")
		go connCloser(conn)
		return
	}

	if !loginPacket.Name.valid() || !loginPacket.Tid.Valid() {
		log.Debug().
			Any("mac", loginPacket.ClientAddr).
			Any("name", loginPacket.Name).
			Any("tid", loginPacket.Tid).
			Msg("[ctl] init: invalid login packet")
		go connCloser(conn)
		return
	}
	titleId, replaced := titledb.CompatibleTitleId(loginPacket.Tid)

	if replaced {
		log.Debug().Msgf("[ctl] init: replaced title id %s with %s", loginPacket.Tid.String(), titleId.String())
	} else {
		titleId = loginPacket.Tid
	}
	registerClient(srv, conn, loginPacket.ClientAddr, loginPacket.Name, titleId)

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("mac", loginPacket.ClientAddr.String()).
		Str("name", loginPacket.Name.String()).
		Str("tid", loginPacket.Tid.String()).
		Msg("[ctl] client logged in")
}
