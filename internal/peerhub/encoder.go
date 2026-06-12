package peerhub

import (
	"bufio"
	"io"
	"sprawl/internal/telemetry"
)

type encoder struct {
	writer   *bufio.Writer
	netStats *telemetry.NetStats
	buffer   []byte
}

func newEncoder(writer io.Writer) *encoder {
	return &encoder{
		writer:   bufio.NewWriter(writer),
		netStats: telemetry.NewNetStats(5),
		buffer:   make([]byte, p2pHeaderSize+p2pMaxPacketSize),
	}
}

func (e *encoder) writeAndFlush(pkt p2pOutboundPacket) error {
	n, err := pkt.writeTo(e.buffer[:])

	if err != nil {
		return err
	}
	if _, err = e.writer.Write(e.buffer[:n]); err != nil {
		return err
	}
	peersTxBytes.Add(float64(n))
	e.netStats.ObservePacket(uint32(n))
	return e.writer.Flush()
}
