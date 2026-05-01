package packet

import "fmt"

var inboundPacketRegistry [16]InboundPacketMeta

func RegisterInboundPacket(id InboundPacketType, size int, decoder InboundPacketDecoder) {
	if inboundPacketRegistry[id].decoder != nil {
		panic(fmt.Sprintf("packet with id %d already registered", id))
	}
	inboundPacketRegistry[id] = InboundPacketMeta{
		size:    size,
		decoder: decoder,
	}
}

var outboundPacketRegistry [16]OutboundPacketMeta

func RegisterOutboundPacket(id OutboundPacketType, size int) {
	outboundPacketRegistry[id] = OutboundPacketMeta{
		size: size,
	}
}
