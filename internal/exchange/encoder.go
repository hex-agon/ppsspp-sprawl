package exchange

import (
	"bufio"
	"io"
	"sprawl/internal/pspnet"
	"sprawl/internal/telemetry"
)

type encoder struct {
	writer   *bufio.Writer
	netStats *telemetry.NetStats
	buffer   []byte
}

func newEncoder(writer io.Writer, maxPacketSize int) *encoder {
	return &encoder{
		writer:   bufio.NewWriter(writer),
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

	if _, err := e.writer.Write(e.buffer[:n]); err != nil {
		return err
	}
	return e.writer.Flush()
}
