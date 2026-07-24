package exchange

import (
	"net"
	"sprawl/internal/pspnet"
	"sprawl/internal/telemetry"
	"time"
)

const writeTimeout = 2 * time.Second

type encoder struct {
	conn     net.Conn
	netStats *telemetry.NetStats
	buffer   []byte
}

func newEncoder(conn net.Conn, maxPacketSize int) *encoder {
	return &encoder{
		conn:     conn,
		netStats: telemetry.NewNetStats(5),
		buffer:   make([]byte, maxPacketSize),
	}
}

func (e *encoder) writeAndFlush(pkt *pdpPacket) error {
	writer := pspnet.NewSliceWriter(e.buffer)

	writer.Bytes(pkt.macAddr[:])
	writer.Skip(2)
	writer.Uint16(pkt.port)
	writer.Uint32(pkt.packetSize)
	writer.Bytes(pkt.payload[:pkt.packetSize])

	n := writer.Pos()
	e.netStats.ObservePacket(uint32(n))

	if err := e.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}

	if _, err := e.conn.Write(e.buffer[:n]); err != nil {
		return err
	}
	return nil
}
