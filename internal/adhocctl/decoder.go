package adhocctl

import (
	"bufio"
	"fmt"
	"io"
)

type inboundPacketType uint8
type inboundPacketMeta struct {
	size    int
	decoder inboundPacketDecoder
}
type inboundPacketDecoder func([]byte) (inboundPacket, error)

type inboundPacket interface {
	Type() inboundPacketType
}

type decoder struct {
	reader *bufio.Reader
	buffer []byte
}

func newDecoder(reader io.Reader, maxPacketSize int) *decoder {
	return &decoder{
		reader: bufio.NewReader(reader),
		buffer: make([]byte, maxPacketSize),
	}
}

func (d *decoder) next() (inboundPacket, error) {
	packetTypeId, err := d.reader.ReadByte()

	if err != nil {
		return nil, err
	}

	if int(packetTypeId) >= len(inboundPacketRegistry) {
		return nil, fmt.Errorf("packet type id out of bounds: %d", packetTypeId)
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
