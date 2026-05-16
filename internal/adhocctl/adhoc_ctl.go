package adhocctl

import (
	"context"
	"errors"
	"net"
	"sprawl/internal/packet"
	"sprawl/internal/pspnet"
	"sprawl/internal/titledb"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const maxPacketSize = 1024
const loginPacketTimeout = 5 * time.Second

type GroupStatus struct {
	ID        string   `json:"id"`
	Players   []string `json:"players"`
	CreatedAt int64    `json:"created_at"`
}

type TitleStatus struct {
	TitleID     string        `json:"title_id"`
	TitleRegion string        `json:"title_region"`
	Name        string        `json:"name"`
	Groups      []GroupStatus `json:"groups"`
	PlayerCount int           `json:"player_count,omitzero"`
}

type CtlClientInfo struct {
	MAC      string `json:"mac"`
	Name     string `json:"name"`
	IP       string `json:"ip"`
	TitleID  string `json:"title_id"`
	GroupID  string `json:"group_id"`
	IsLeader bool   `json:"is_leader"`
}

type Server interface {
	Shutdown() error
	Addr() net.Addr
	GetActiveTitles() []TitleStatus
	GetClients() []CtlClientInfo
}
type ctlServer struct {
	sync.RWMutex
	ctx              context.Context
	cancelFunc       context.CancelFunc
	listener         net.Listener
	clients          map[pspnet.MacAddr]*client
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
		clients:          make(map[pspnet.MacAddr]*client),
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
				ID:        gid.String(),
				Players:   make([]string, 0, len(g.clients)),
				CreatedAt: g.createdAt,
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

func (srv *ctlServer) GetClients() []CtlClientInfo {
	srv.RLock()
	defer srv.RUnlock()

	result := make([]CtlClientInfo, 0, len(srv.clients))
	for _, cli := range srv.clients {
		info := CtlClientInfo{
			MAC:     cli.macAddr.String(),
			Name:    cli.name.String(),
			IP:      cli.conn.RemoteAddr().String(),
			TitleID: cli.titleId.String(),
		}
		if cli.group != nil {
			info.GroupID = cli.group.id.String()
			info.IsLeader = cli.macAddr == cli.group.founderMacAddr
		}
		result = append(result, info)
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
			createdAt:      time.Now().Unix(),
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
	decoder := packet.NewDecoder(conn, maxPacketSize)

	pkt, err := decoder.Next()

	if err != nil {
		connCloser(conn)
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	loginPacket, ok := pkt.(*c2sLogin)
	if !ok {
		log.Debug().Msg("[ctl] init: packet is not a login packet")
		connCloser(conn)
		return
	}

	if !loginPacket.Name.Valid() || !loginPacket.Tid.Valid() {
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

func (srv *ctlServer) registerClient(conn net.Conn, mac pspnet.MacAddr, name pspnet.DisplayName, tid titledb.TitleId) {
	srv.Lock()
	defer srv.Unlock()

	ctx, cancelFunc := context.WithCancel(srv.ctx)

	c := &client{
		ctx:       ctx,
		ctxCancel: cancelFunc,
		closeOnce: sync.Once{},
		conn:      conn,
		decoder:   packet.NewDecoder(conn, maxPacketSize),
		encoder:   packet.NewEncoder(conn, maxPacketSize),
		macAddr:   mac,
		server:    srv,
		titleId:   tid,
		name:      name,
		txQueue:   make(chan packet.OutboundPacket, 32),
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
