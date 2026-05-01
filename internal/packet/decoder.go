package packet

import (
	"bufio"
	"fmt"
	"io"
)

type InboundPacketType uint8
type InboundPacketMeta struct {
	size    int
	decoder InboundPacketDecoder
}
type InboundPacketDecoder func([]byte) (InboundPacket, error)

type InboundPacket interface {
	Type() InboundPacketType
}

type Decoder struct {
	reader *bufio.Reader
	buffer []byte
}

func NewDecoder(reader io.Reader, maxPacketSize int) *Decoder {
	return &Decoder{
		reader: bufio.NewReader(reader),
		buffer: make([]byte, maxPacketSize),
	}
}

func (d *Decoder) Next() (InboundPacket, error) {
	packetTypeId, err := d.reader.ReadByte()

	if err != nil {
		return nil, err
	}
	pktType := inboundPacketRegistry[packetTypeId]

	if pktType.decoder == nil {
		return nil, fmt.Errorf("unknown packet type: %d", packetTypeId)
	}
	pktBuff := d.buffer[:pktType.size]

	if _, err := io.ReadFull(d.reader, pktBuff); err != nil {
		return nil, err
	}
	return pktType.decoder(pktBuff)
}
