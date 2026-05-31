package adhocctl

import (
	"bufio"
	"fmt"
	"io"
)

type outboundPacketType uint8

type outboundPacketMeta struct {
	size int
}

type outboundPacket interface {
	Type() outboundPacketType
	WriteTo(dst []byte) (int, error)
}

type encoder struct {
	writer *bufio.Writer
	buffer []byte
}

func newEncoder(writer io.Writer, maxPacketSize int) *encoder {
	return &encoder{
		writer: bufio.NewWriter(writer),
		buffer: make([]byte, maxPacketSize+1),
	}
}

func (e *encoder) WriteAndFlush(packet outboundPacket) error {
	pktType := byte(packet.Type())
	pktMeta := outboundPacketRegistry[pktType]

	e.buffer[0] = pktType
	n, err := packet.WriteTo(e.buffer[1:])

	if n != pktMeta.size {
		return fmt.Errorf("packet wrote %d byte(s), expected %d", n, pktMeta.size)
	}

	if err != nil {
		return err
	}
	if _, err = e.writer.Write(e.buffer[:n+1]); err != nil {
		return err
	}
	return e.writer.Flush()
}
