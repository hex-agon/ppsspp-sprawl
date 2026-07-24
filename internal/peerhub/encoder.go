package peerhub

import (
	"net"
	"sprawl/internal/telemetry"
	"time"
)

const writeTimeout = 2 * time.Second

type encoder struct {
	conn     net.Conn
	netStats *telemetry.NetStats
	buffer   []byte
}

func newEncoder(conn net.Conn) *encoder {
	return &encoder{
		conn:     conn,
		netStats: telemetry.NewNetStats(5),
		buffer:   make([]byte, p2pHeaderSize+p2pMaxPacketSize),
	}
}

func (e *encoder) writeAndFlush(pkt p2pOutboundPacket) error {
	n, err := pkt.writeTo(e.buffer[:])

	if err != nil {
		return err
	}
	peersTxBytes.Add(float64(n))
	e.netStats.ObservePacket(uint32(n))

	if err := e.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}

	if _, err = e.conn.Write(e.buffer[:n]); err != nil {
		return err
	}
	return nil
}
