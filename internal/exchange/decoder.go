package exchange

import (
	"bufio"
	"fmt"
	"io"
	"sprawl/internal/pspnet"
	"sprawl/internal/telemetry"
)

type decoder struct {
	reader   *bufio.Reader
	netStats *telemetry.NetStats
	buffer   []byte
}

func newDecoder(reader io.Reader) *decoder {
	return &decoder{
		reader:   bufio.NewReader(reader),
		netStats: telemetry.NewNetStats(5),
		buffer:   make([]byte, pdpHeaderSize),
	}
}

func (d *decoder) next() (*pdpPacket, error) {
	header := d.buffer[:pdpHeaderSize]

	if _, err := io.ReadFull(d.reader, header); err != nil {
		return nil, err
	}
	headerReader := pspnet.NewSliceReader(header)

	targetMacAddr := headerReader.MacAddr()
	headerReader.Skip(2) // padding
	targetPort := headerReader.Uint16()
	packetSize := headerReader.Uint32()

	if packetSize > pdpMaxPacketSize {
		return nil, fmt.Errorf("packet size %d exceeds limit %d", packetSize, pdpMaxPacketSize)
	}
	pkt := pdpPacketPool.Get().(*pdpPacket)
	if _, err := io.ReadFull(d.reader, pkt.payload[:packetSize]); err != nil {
		pkt.release()
		return nil, err
	}
	pkt.macAddr = targetMacAddr
	pkt.port = targetPort
	pkt.packetSize = packetSize

	d.netStats.ObservePacket(packetSize)
	return pkt, nil
}
