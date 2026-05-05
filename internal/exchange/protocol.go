package exchange

import (
	"encoding/binary"
	"io"
	"sprawl/internal/pspnet"
)

type pdpPacket struct {
	macAddr    pspnet.MacAddr
	port       uint16
	packetSize uint32
	payload    []byte
}

func (p *pdpPacket) writeTo(w io.Writer) error {
	header := make([]byte, pdpHeaderSize)
	copy(header[0:6], p.macAddr[:])
	binary.LittleEndian.PutUint16(header[8:10], p.port)
	binary.LittleEndian.PutUint32(header[10:14], p.packetSize)

	if _, err := w.Write(header); err != nil {
		return err
	}
	if _, err := w.Write(p.payload); err != nil {
		return err
	}
	return nil
}
