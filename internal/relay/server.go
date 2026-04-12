package relay

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sprawl/internal/exchange"
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

type Server interface {
	Shutdown() error
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
		log.Info().Str("ip", conn.RemoteAddr().String()).Msg("[relay] accepted new connection")

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
		log.Debug().Str("ip", conn.RemoteAddr().String()).Msg("closed connection")
	}
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

	initPktType := initType(binary.LittleEndian.Uint32(initBuf[0:4]))
	srcAddr := initBuf[4:10]
	// padding
	srcPort := binary.LittleEndian.Uint16(initBuf[12:14])
	dstAddr := initBuf[14:20]
	// padding
	dstPort := binary.LittleEndian.Uint16(initBuf[22:24])

	log.Debug().
		Str("ip", conn.RemoteAddr().String()).
		Any("init-type", initPktType).
		Str("src-addr", hex.EncodeToString(srcAddr[:])).
		Uint16("src-port", srcPort).
		Str("dst-addr", hex.EncodeToString(dstAddr[:])).
		Uint16("dst-port", dstPort).
		Msg("[relay] received init packet")

	switch initPktType {
	case initTypePdp:
		exchange.RegisterClient(srv.ctx, srv.exchange, conn, srcAddr, srcPort)
	case initTypeP2pListen:
		peerhub.HandleListeningPeer(srv.ctx, srv.hub, conn, srcAddr, srcPort)
	case initTypeP2pConnect:
		peerhub.HandleConnectingPeer(srv.ctx, srv.hub, conn, srcAddr, srcPort, dstAddr, dstPort)
	case initTypeP2pAccept:
		peerhub.HandleAcceptingPeer(srv.ctx, srv.hub, conn, srcAddr, srcPort, dstAddr, dstPort)
	default:
		log.Error().Any("init-type", initPktType).Msg("unsupported init type")
		go connCloser(conn)
	}
}
