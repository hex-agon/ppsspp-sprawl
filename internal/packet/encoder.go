package packet

import (
	"bufio"
	"fmt"
	"io"
)

type OutboundPacketType uint8

type OutboundPacketMeta struct {
	size int
}

type OutboundPacket interface {
	Type() OutboundPacketType
	WriteTo(dst []byte) (int, error)
}

type Encoder struct {
	writer *bufio.Writer
	buffer []byte
}

func NewEncoder(writer io.Writer, maxPacketSize int) *Encoder {
	return &Encoder{
		writer: bufio.NewWriter(writer),
		buffer: make([]byte, maxPacketSize+1),
	}
}

func (e *Encoder) WriteAndFlush(packet OutboundPacket) error {
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
