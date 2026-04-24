package adhocctl

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sprawl/internal/titledb"
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
	PlayerCount int           `json:"player_count,omitzero"`
}

type Server interface {
	Shutdown() error
	Addr() net.Addr
	GetActiveTitles() []TitleStatus
}
type ctlServer struct {
	sync.RWMutex
	ctx              context.Context
	cancelFunc       context.CancelFunc
	listener         net.Listener
	clients          map[macAddr]*client
	groups           map[titledb.TitleId]map[groupId]*group
	unregisterClient chan *client
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
		listener:         listener,
		ctx:              ctx,
		cancelFunc:       ctxCancel,
		clients:          make(map[macAddr]*client),
		groups:           make(map[titledb.TitleId]map[groupId]*group),
		unregisterClient: make(chan *client, 16),
	}

	go srv.connAcceptor()
	go srv.houseKeeper()
	return srv, nil
}

func (srv *ctlServer) Shutdown() error {
	srv.cancelFunc()
	return srv.listener.Close()
}

func (srv *ctlServer) Addr() net.Addr {
	return srv.listener.Addr()
}

func (srv *ctlServer) GetActiveTitles() []TitleStatus {
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

		if name, region, ok := titledb.GetTitle(tid); ok {
			titleStatus.Name = name
			titleStatus.TitleRegion = region
		} else {
			titleStatus.Name = tid.String()
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
			titleStatus.PlayerCount += len(groupStatus.Players)
			titleStatus.Groups = append(titleStatus.Groups, groupStatus)
		}
		result = append(result, titleStatus)
	}

	return result
}

func (srv *ctlServer) houseKeeper() {
	for {
		select {
		case <-srv.ctx.Done():
			return
		case cli := <-srv.unregisterClient:
			srv.Lock()
			if _, ok := srv.clients[cli.macAddr]; ok {
				delete(srv.clients, cli.macAddr)
				clientsCount.Dec()
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

func (srv *ctlServer) scanGroups(tid titledb.TitleId, consumer func(*group)) {
	srv.RLock()
	defer srv.RUnlock()

	if groups, ok := srv.groups[tid]; ok {
		for _, g := range groups {
			consumer(g)
		}
	}
}

func (srv *ctlServer) connAcceptor() {
	for {
		conn, err := srv.listener.Accept()

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Warn().Err(err).Msg("[ctl] failed to accept connection")
			continue
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("[ctl] accepted new connection")

		go srv.waitForLoginPacket(conn)
	}
}

func (srv *ctlServer) waitForLoginPacket(conn net.Conn) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().Err(err).Str("ip", conn.RemoteAddr().String()).Msg("[ctl] failed to close connection")
		}
		log.Debug().Str("ip", conn.RemoteAddr().String()).Msg("[ctl] init: closed connection")
	}
	if err := conn.SetReadDeadline(time.Now().Add(loginPacketTimeout)); err != nil {
		connCloser(conn)
		return
	}
	var packetTypeId uint8

	if err := binary.Read(conn, binary.LittleEndian, &packetTypeId); err != nil {
		connCloser(conn)
		return
	}

	if packetTypeId != packetTypeLogin.id {
		log.Warn().
			Str("ip", conn.RemoteAddr().String()).
			Msgf("[ctl] init: unexpected packet type during login: %d", packetTypeId)
		connCloser(conn)
		return
	}
	loginBuff := make([]byte, packetTypeLogin.size)

	if _, err := io.ReadFull(conn, loginBuff); err != nil {
		connCloser(conn)
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
		connCloser(conn)
		return
	}

	if !loginPacket.Name.valid() || !loginPacket.Tid.Valid() {
		log.Warn().
			Str("mac", loginPacket.ClientAddr.String()).
			Str("name", loginPacket.Name.String()).
			Str("tid", loginPacket.Tid.String()).
			Msg("[ctl] init: invalid login packet")
		connCloser(conn)
		return
	}
	titleId, replaced := titledb.CompatibleTitleId(loginPacket.Tid)

	if replaced {
		log.Debug().Msgf("[ctl] init: replaced title id %s with %s", loginPacket.Tid.String(), titleId.String())
	} else {
		titleId = loginPacket.Tid
	}
	srv.registerClient(conn, loginPacket.ClientAddr, loginPacket.Name, titleId)

	log.Info().
		Str("ip", conn.RemoteAddr().String()).
		Str("mac", loginPacket.ClientAddr.String()).
		Str("name", loginPacket.Name.String()).
		Str("tid", loginPacket.Tid.String()).
		Msg("[ctl] client logged in")
}

func (srv *ctlServer) registerClient(conn net.Conn, mac macAddr, name displayName, tid titledb.TitleId) {
	srv.Lock()
	defer srv.Unlock()

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
		logger: log.With().
			Str("ip", conn.RemoteAddr().String()).
			Str("mac", mac.String()).
			Str("name", name.String()).
			Logger(),
	}

	if _, ok := srv.clients[mac]; ok {
		c.logger.Warn().Msg("[ctl] duplicate client connection detected")
		c.close()
		return
	}

	srv.clients[mac] = c
	clientsCount.Inc()

	go c.reader()
	go c.writer()
}
