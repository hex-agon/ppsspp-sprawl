package exchange

import (
	"sprawl/internal/pspnet"
)

type pdpPacket struct {
	macAddr    pspnet.MacAddr
	port       uint16
	packetSize uint32
	payload    []byte
}
