package peerhub

import (
	"sync"

	"sprawl/internal/packet"
	"sprawl/internal/pspnet"
)

type p2pOutboundPacket interface {
	writeTo([]byte) (int, error)
	release()
}

var p2pPacketPool = sync.Pool{
	New: func() any {
		return &p2pPacket{
			payload: make([]byte, p2pMaxPacketSize),
		}
	},
}

type p2pPacket struct {
	packetSize uint32
	payload    []byte
}

func (p *p2pPacket) release() {
	p2pPacketPool.Put(p)
}

func (p *p2pPacket) writeTo(bytes []byte) (int, error) {
	writer := packet.NewSliceWriter(bytes)

	writer.Uint32(p.packetSize)
	writer.Bytes(p.payload[:p.packetSize])

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

func (p *peerAnnouncePacket) release() {
	// NO-OP
}
