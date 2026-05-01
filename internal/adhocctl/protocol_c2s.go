package adhocctl

import (
	"sprawl/internal/packet"
	"sprawl/internal/titledb"
)

const (
	inboundPacketPing       packet.InboundPacketType = 0
	inboundPacketLogin      packet.InboundPacketType = 1
	inboundPacketConnect    packet.InboundPacketType = 2
	inboundPacketDisconnect packet.InboundPacketType = 3
	inboundPacketScan       packet.InboundPacketType = 4
	inboundPacketChat       packet.InboundPacketType = 7

	inboundPacketLoginSize   = 143
	inboundPacketConnectSize = 9
	inboundPacketChatSize    = 65
)

func init() {
	packet.RegisterInboundPacket(inboundPacketPing, 0, constPacketDecoder(&c2sPing{}))
	packet.RegisterInboundPacket(inboundPacketLogin, inboundPacketLoginSize, loginPacketDecoder)
	packet.RegisterInboundPacket(inboundPacketConnect, inboundPacketConnectSize, connectPacketDecoder)
	packet.RegisterInboundPacket(inboundPacketDisconnect, 0, constPacketDecoder(&c2sDisconnect{}))
	packet.RegisterInboundPacket(inboundPacketScan, 0, constPacketDecoder(&c2sScan{}))
	packet.RegisterInboundPacket(inboundPacketChat, inboundPacketChatSize, chatPacketDecoder)
}

func constPacketDecoder(pkt packet.InboundPacket) func(bytes []byte) (packet.InboundPacket, error) {
	return func(bytes []byte) (packet.InboundPacket, error) {
		return pkt, nil
	}
}

type c2sPing struct {
}

func (c *c2sPing) Type() packet.InboundPacketType {
	return inboundPacketPing
}

type c2sLogin struct {
	ClientAddr macAddr
	Name       displayName
	Tid        titledb.TitleId
}

func (c *c2sLogin) Type() packet.InboundPacketType {
	return inboundPacketLogin
}

func (c *c2sLogin) WriteTo(dst []byte) (int, error) {
	writer := packet.NewSliceWriter(dst)
	writer.Bytes(c.ClientAddr[:])
	writer.Bytes(c.Name[:])
	writer.Bytes(c.Tid[:])
	return writer.Pos(), nil
}

func loginPacketDecoder(buffer []byte) (packet.InboundPacket, error) {
	reader := packet.NewSliceReader(buffer)

	return &c2sLogin{
		ClientAddr: macAddr(reader.Bytes(macAddrLen)),
		Name:       displayName(reader.Bytes(displayNameLen)),
		Tid:        titledb.TitleId(reader.Bytes(titledb.TitleIdLen)),
	}, nil
}

type c2sConnect struct {
	gid groupId
}

func (c *c2sConnect) Type() packet.InboundPacketType {
	return inboundPacketConnect
}

func connectPacketDecoder(buffer []byte) (packet.InboundPacket, error) {
	reader := packet.NewSliceReader(buffer)

	return &c2sConnect{
		gid: groupId(reader.Bytes(groupIdLen)),
	}, nil
}

type c2sDisconnect struct {
}

func (c *c2sDisconnect) Type() packet.InboundPacketType {
	return inboundPacketDisconnect
}

type c2sScan struct {
}

func (c *c2sScan) Type() packet.InboundPacketType {
	return inboundPacketScan
}

type c2sChat struct {
	message []byte
}

func (c *c2sChat) Type() packet.InboundPacketType {
	return inboundPacketChat
}

func chatPacketDecoder(buffer []byte) (packet.InboundPacket, error) {
	reader := packet.NewSliceReader(buffer)
	return &c2sChat{
		message: reader.Bytes(inboundPacketChatSize),
	}, nil
}
