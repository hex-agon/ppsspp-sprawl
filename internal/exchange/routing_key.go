package exchange

import (
	"encoding/binary"
	"encoding/hex"
	"sprawl/internal/pspnet"
)

// RoutingKey is composed of the client's informed mac address (6 bytes) and it's port
type routingKey [8]byte

func newRoutingKey(mac pspnet.MacAddr, port uint16) routingKey {
	bytes := make([]byte, 8)
	copy(bytes[:6], mac[:])
	binary.LittleEndian.PutUint16(bytes[6:8], port)
	return routingKey(bytes[:])
}

func (key routingKey) String() string {
	return hex.EncodeToString(key[:])
}
