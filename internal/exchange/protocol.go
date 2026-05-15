package exchange

import (
	"sync"

	"sprawl/internal/pspnet"
)

var pdpPacketPool = sync.Pool{
	New: func() any {
		return &pdpPacket{
			payload: make([]byte, pdpMaxPacketSize),
		}
	},
}

type pdpPacket struct {
	macAddr    pspnet.MacAddr
	port       uint16
	packetSize uint32
	payload    []byte
}

func (p *pdpPacket) release() {
	pdpPacketPool.Put(p)
}
