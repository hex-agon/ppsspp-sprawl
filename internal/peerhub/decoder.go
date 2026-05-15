package peerhub

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
		buffer: make([]byte, p2pHeaderSize),
	}
}

func (d *decoder) next() (*p2pPacket, error) {
	header := d.buffer[:p2pHeaderSize]
	if _, err := io.ReadFull(d.reader, header); err != nil {
		return nil, err
	}

	packetSize := packet.NewSliceReader(header).Uint32()

	if packetSize > p2pMaxPacketSize {
		return nil, fmt.Errorf("packet size %d exceeds limit %d", packetSize, p2pMaxPacketSize)
	}
	pkt := p2pPacketPool.Get().(*p2pPacket)

	if _, err := io.ReadFull(d.reader, pkt.payload[:packetSize]); err != nil {
		pkt.release()
		return nil, err
	}
	totalSize := float64(p2pHeaderSize + packetSize)
	packetSizeHistogram.Observe(totalSize)
	peersRxBytes.Add(totalSize)

	pkt.packetSize = packetSize
	return pkt, nil
}
