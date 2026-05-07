package peerhub

import (
	"sprawl/internal/packet"
	"sprawl/internal/pspnet"
)

type p2pOutboundPacket interface {
	writeTo([]byte) (int, error)
}

type p2pPacket struct {
	payload []byte
}

func (p *p2pPacket) writeTo(bytes []byte) (int, error) {
	writer := packet.NewSliceWriter(bytes)

	writer.Uint32(uint32(len(p.payload)))
	writer.Bytes(p.payload)

	return writer.Pos(), nil
}

type peerAnnouncePacket struct {
	macAddr pspnet.MacAddr
	port    uint16
}

func (p *peerAnnouncePacket) writeTo(bytes []byte) (int, error) {
	writer := packet.NewSliceWriter(bytes)
	writer.Bytes(p.macAddr[:])
	writer.Skip(2)
	writer.Uint16(p.port)
	return writer.Pos(), nil
}
