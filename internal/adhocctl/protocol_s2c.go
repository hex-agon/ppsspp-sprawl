package adhocctl

import (
	"sprawl/internal/packet"
)

const (
	outboundPacketClientConnected    packet.OutboundPacketType = 2
	outboundPacketClientDisconnected packet.OutboundPacketType = 3
	outboundPacketScanResponse       packet.OutboundPacketType = 4
	outboundPacketScanComplete       packet.OutboundPacketType = 5
	outboundPacketConnectBssid       packet.OutboundPacketType = 6
	outboundPacketChat               packet.OutboundPacketType = 7
)

func init() {
	packet.RegisterOutboundPacket(outboundPacketClientConnected, 138)
	packet.RegisterOutboundPacket(outboundPacketClientDisconnected, 4)
	packet.RegisterOutboundPacket(outboundPacketScanResponse, 14)
	packet.RegisterOutboundPacket(outboundPacketScanComplete, 0)
	packet.RegisterOutboundPacket(outboundPacketConnectBssid, 6)
	packet.RegisterOutboundPacket(outboundPacketChat, 193)
}

type s2cScanReply struct {
	gid         groupId
	founderAddr macAddr
}

func (s *s2cScanReply) Type() packet.OutboundPacketType {
	return outboundPacketScanResponse
}

func (s *s2cScanReply) WriteTo(dst []byte) (int, error) {
	writer := packet.NewSliceWriter(dst)
	writer.Bytes(s.gid[:])
	writer.Bytes(s.founderAddr[:])
	return writer.Pos(), nil
}

type s2cScanComplete struct {
}

func (s *s2cScanComplete) Type() packet.OutboundPacketType {
	return outboundPacketScanComplete
}

func (s *s2cScanComplete) WriteTo(_ []byte) (int, error) {
	return 0, nil
}

type s2cConnectBssid struct {
	macAddr macAddr
}

func (s *s2cConnectBssid) Type() packet.OutboundPacketType {
	return outboundPacketConnectBssid
}

func (s *s2cConnectBssid) WriteTo(dst []byte) (int, error) {
	writer := packet.NewSliceWriter(dst)
	writer.Bytes(s.macAddr[:])
	return writer.Pos(), nil
}

type s2cClientConnected struct {
	displayName displayName
	macAddr     macAddr
	ipAddr      uint32
}

func (s *s2cClientConnected) Type() packet.OutboundPacketType {
	return outboundPacketClientConnected
}

func (s *s2cClientConnected) WriteTo(dst []byte) (int, error) {
	writer := packet.NewSliceWriter(dst)
	writer.Bytes(s.displayName[:])
	writer.Bytes(s.macAddr[:])
	writer.Uint32(s.ipAddr)
	return writer.Pos(), nil
}

type s2cDisconnected struct {
	ipAddr uint32
}

func (s *s2cDisconnected) Type() packet.OutboundPacketType {
	return outboundPacketClientDisconnected
}

func (s *s2cDisconnected) WriteTo(dst []byte) (int, error) {
	writer := packet.NewSliceWriter(dst)
	writer.Uint32(s.ipAddr)
	return writer.Pos(), nil
}

type s2cChat struct {
	message     []byte
	displayName displayName
}

func (s *s2cChat) Type() packet.OutboundPacketType {
	return outboundPacketChat
}

func (s *s2cChat) WriteTo(dst []byte) (int, error) {
	writer := packet.NewSliceWriter(dst)
	writer.Bytes(s.displayName[:])
	writer.Bytes(s.message)
	return writer.Pos(), nil
}
