package exchange

import (
	"bufio"
	"io"
	"sprawl/internal/packet"
)

type encoder struct {
	writer *bufio.Writer
	buffer []byte
}

func newEncoder(writer io.Writer, maxPacketSize int) *encoder {
	return &encoder{
		writer: bufio.NewWriter(writer),
		buffer: make([]byte, maxPacketSize),
	}
}

func (e *encoder) writeAndFlush(pkt *pdpPacket) error {
	writer := packet.NewSliceWriter(e.buffer)

	writer.Bytes(pkt.macAddr[:])
	writer.Skip(2)
	writer.Uint16(pkt.port)
	writer.Uint32(pkt.packetSize)
	writer.Bytes(pkt.payload)

	n := writer.Pos()

	if _, err := e.writer.Write(e.buffer[:n]); err != nil {
		return err
	}
	return e.writer.Flush()
}
