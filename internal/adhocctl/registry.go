package adhocctl

import "fmt"

var inboundPacketRegistry [16]inboundPacketMeta

func registerInboundPacket(id inboundPacketType, size int, decoder inboundPacketDecoder) {
	if inboundPacketRegistry[id].decoder != nil {
		panic(fmt.Sprintf("packet with id %d already registered", id))
	}
	inboundPacketRegistry[id] = inboundPacketMeta{
		size:    size,
		decoder: decoder,
	}
}

var outboundPacketRegistry [16]outboundPacketMeta

func registerOutboundPacket(id outboundPacketType, size int) {
	outboundPacketRegistry[id] = outboundPacketMeta{
		size: size,
	}
}
