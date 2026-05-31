package adhocctl

import (
	"sprawl/internal/pspnet"
	"sprawl/internal/titledb"
)

const (
	inboundPacketPing       inboundPacketType = 0
	inboundPacketLogin      inboundPacketType = 1
	inboundPacketConnect    inboundPacketType = 2
	inboundPacketDisconnect inboundPacketType = 3
	inboundPacketScan       inboundPacketType = 4
	inboundPacketChat       inboundPacketType = 7

	inboundPacketLoginSize   = 143
	inboundPacketConnectSize = 9
	inboundPacketChatSize    = 65
)

func init() {
	registerInboundPacket(inboundPacketPing, 0, constPacketDecoder(&c2sPing{}))
	registerInboundPacket(inboundPacketLogin, inboundPacketLoginSize, loginPacketDecoder)
	registerInboundPacket(inboundPacketConnect, inboundPacketConnectSize, connectPacketDecoder)
	registerInboundPacket(inboundPacketDisconnect, 0, constPacketDecoder(&c2sDisconnect{}))
	registerInboundPacket(inboundPacketScan, 0, constPacketDecoder(&c2sScan{}))
	registerInboundPacket(inboundPacketChat, inboundPacketChatSize, chatPacketDecoder)
}

func constPacketDecoder(pkt inboundPacket) func(bytes []byte) (inboundPacket, error) {
	return func(bytes []byte) (inboundPacket, error) {
		return pkt, nil
	}
}

type c2sPing struct {
}

func (c *c2sPing) Type() inboundPacketType {
	return inboundPacketPing
}

type c2sLogin struct {
	ClientAddr pspnet.MacAddr
	Name       pspnet.DisplayName
	Tid        titledb.TitleId
}

func (c *c2sLogin) Type() inboundPacketType {
	return inboundPacketLogin
}

func (c *c2sLogin) WriteTo(dst []byte) (int, error) {
	writer := pspnet.NewSliceWriter(dst)
	writer.Bytes(c.ClientAddr[:])
	writer.Bytes(c.Name[:])
	writer.Bytes(c.Tid[:])
	return writer.Pos(), nil
}

func loginPacketDecoder(buffer []byte) (inboundPacket, error) {
	reader := pspnet.NewSliceReader(buffer)

	return &c2sLogin{
		ClientAddr: pspnet.MacAddr(reader.Bytes(pspnet.MacAddrLen)),
		Name:       pspnet.DisplayName(reader.Bytes(pspnet.DisplayNameLen)),
		Tid:        titledb.TitleId(reader.Bytes(titledb.TitleIdLen)),
	}, nil
}

type c2sConnect struct {
	gid groupId
}

func (c *c2sConnect) Type() inboundPacketType {
	return inboundPacketConnect
}

func connectPacketDecoder(buffer []byte) (inboundPacket, error) {
	reader := pspnet.NewSliceReader(buffer)

	return &c2sConnect{
		gid: groupId(reader.Bytes(groupIdLen)),
	}, nil
}

type c2sDisconnect struct {
}

func (c *c2sDisconnect) Type() inboundPacketType {
	return inboundPacketDisconnect
}

type c2sScan struct {
}

func (c *c2sScan) Type() inboundPacketType {
	return inboundPacketScan
}

type c2sChat struct {
	message []byte
}

func (c *c2sChat) Type() inboundPacketType {
	return inboundPacketChat
}

func chatPacketDecoder(buffer []byte) (inboundPacket, error) {
	reader := pspnet.NewSliceReader(buffer)
	return &c2sChat{
		message: reader.Bytes(inboundPacketChatSize),
	}, nil
}
