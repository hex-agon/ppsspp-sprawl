package sprawl

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"sprawl/internal/exchange"
	"sprawl/internal/packet"
	"sprawl/internal/peerhub"
	"time"

	"github.com/rs/zerolog/log"
)

const initPacketSize = 24
const initPacketTimeout = 5 * time.Second

type initType int32

const initTypePdp initType = 0
const initTypeP2pListen initType = 1
const initTypeP2pConnect initType = 2
const initTypeP2pAccept initType = 3

func (t initType) String() string {
	switch t {
	case initTypePdp:
		return "PDP"
	case initTypeP2pListen:
		return "P2P_LISTEN"
	case initTypeP2pConnect:
		return "P2P_CONNECT"
	case initTypeP2pAccept:
		return "P2P_ACCEPT"
	default:
		return fmt.Sprintf("INVALID(%d)", t)
	}
}

type Server interface {
	Shutdown() error
	GetExchange() *exchange.Exchange
	GetHub() *peerhub.PeerHub
}

type relayServer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	listener   net.Listener
	exchange   *exchange.Exchange
	hub        *peerhub.PeerHub
}

func NewRelay(addr string, port string) (Server, error) {
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
	ex := exchange.NewExchange(ctx)
	hub := peerhub.NewHub(ctx)

	relay := &relayServer{
		listener:   listener,
		ctx:        ctx,
		cancelFunc: ctxCancel,
		exchange:   ex,
		hub:        hub,
	}

	go relay.connAcceptor()
	return relay, nil
}

func (srv *relayServer) Shutdown() error {
	srv.cancelFunc()
	return srv.listener.Close()
}

func (srv *relayServer) GetExchange() *exchange.Exchange {
	return srv.exchange
}

func (srv *relayServer) GetHub() *peerhub.PeerHub {
	return srv.hub
}

func (srv *relayServer) connAcceptor() {
	for {
		conn, err := srv.listener.Accept()

		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Warn().Err(err).Msg("[relay] failed to accept connection")
			continue
		}

		go srv.handleConn(conn)
	}
}

func (srv *relayServer) handleConn(conn net.Conn) {
	connCloser := func(conn net.Conn) {
		if err := conn.Close(); err != nil {
			log.Warn().
				Str("ip", conn.RemoteAddr().String()).
				Err(err).
				Msg("[relay] failed to close connection")
		}
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("closed connection")
	}

	log.Info().Str("ip", conn.RemoteAddr().String()).Msg("[relay] accepted new connection")
	if err := conn.SetReadDeadline(time.Now().Add(initPacketTimeout)); err != nil {
		go connCloser(conn)
		return
	}
	initBuf := make([]byte, initPacketSize)

	if _, err := io.ReadFull(conn, initBuf); err != nil {
		go connCloser(conn)
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	r := packet.NewSliceReader(initBuf)
	initPktType := initType(r.Uint32())
	srcAddr := r.MacAddr()
	r.Skip(2) // padding
	srcPort := r.Uint16()
	dstAddr := r.MacAddr()
	r.Skip(2) // padding
	dstPort := r.Uint16()

	log.Debug().
		Str("ip", conn.RemoteAddr().String()).
		Str("init-type", initPktType.String()).
		Str("src-addr", hex.EncodeToString(srcAddr[:])).
		Uint16("src-port", srcPort).
		Str("dst-addr", hex.EncodeToString(dstAddr[:])).
		Uint16("dst-port", dstPort).
		Msg("[relay] received init packet")

	switch initPktType {
	case initTypePdp:
		srv.exchange.RegisterClient(conn, srcAddr, srcPort)
	case initTypeP2pListen:
		srv.hub.HandleListeningPeer(conn, srcAddr, srcPort)
	case initTypeP2pConnect:
		srv.hub.HandleConnectingPeer(conn, srcAddr, srcPort, dstAddr, dstPort)
	case initTypeP2pAccept:
		srv.hub.HandleAcceptingPeer(conn, srcAddr, srcPort, dstAddr, dstPort)
	default:
		log.Error().Any("init-type", initPktType).Msg("unsupported init type")
		go connCloser(conn)
	}
}
