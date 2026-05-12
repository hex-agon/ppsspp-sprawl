package exchange

import (
	"bufio"
	"fmt"
	"io"
	"sprawl/internal/packet"
)

type decoder struct {
	reader *bufio.Reader
	buffer []byte
}

func newDecoder(reader io.Reader) *decoder {
	return &decoder{
		reader: bufio.NewReader(reader),
		buffer: make([]byte, pdpHeaderSize),
	}
}

func (d *decoder) next() (*pdpPacket, error) {
	header := d.buffer[:pdpHeaderSize]

	if _, err := io.ReadFull(d.reader, header); err != nil {
		return nil, err
	}
	headerReader := packet.NewSliceReader(header)

	targetMacAddr := headerReader.MacAddr()
	headerReader.Skip(2) // padding
	targetPort := headerReader.Uint16()
	packetSize := headerReader.Uint32()

	if packetSize > pdpMaxPacketSize {
		return nil, fmt.Errorf("packet size %d exceeds limit %d", packetSize, pdpMaxPacketSize)
	}
	payload := make([]byte, packetSize)

	if _, err := io.ReadFull(d.reader, payload); err != nil {
		return nil, err
	}
	packetSizeHistogram.Observe(float64(pdpHeaderSize + packetSize))
	return &pdpPacket{
		macAddr:    targetMacAddr,
		port:       targetPort,
		packetSize: packetSize,
		payload:    payload,
	}, nil
}
