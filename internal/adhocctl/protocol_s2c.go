package adhocctl

import (
	"sprawl/internal/pspnet"
)

const (
	outboundPacketClientConnected    outboundPacketType = 2
	outboundPacketClientDisconnected outboundPacketType = 3
	outboundPacketScanResponse       outboundPacketType = 4
	outboundPacketScanComplete       outboundPacketType = 5
	outboundPacketConnectBssid       outboundPacketType = 6
	outboundPacketChat               outboundPacketType = 7
)

func init() {
	registerOutboundPacket(outboundPacketClientConnected, 138)
	registerOutboundPacket(outboundPacketClientDisconnected, 4)
	registerOutboundPacket(outboundPacketScanResponse, 14)
	registerOutboundPacket(outboundPacketScanComplete, 0)
	registerOutboundPacket(outboundPacketConnectBssid, 6)
	registerOutboundPacket(outboundPacketChat, 193)
}

type s2cScanReply struct {
	gid         groupId
	founderAddr pspnet.MacAddr
}

func (s *s2cScanReply) Type() outboundPacketType {
	return outboundPacketScanResponse
}

func (s *s2cScanReply) WriteTo(dst []byte) (int, error) {
	writer := pspnet.NewSliceWriter(dst)
	writer.Bytes(s.gid[:])
	writer.Bytes(s.founderAddr[:])
	return writer.Pos(), nil
}

type s2cScanComplete struct {
}

func (s *s2cScanComplete) Type() outboundPacketType {
	return outboundPacketScanComplete
}

func (s *s2cScanComplete) WriteTo(_ []byte) (int, error) {
	return 0, nil
}

type s2cConnectBssid struct {
	macAddr pspnet.MacAddr
}

func (s *s2cConnectBssid) Type() outboundPacketType {
	return outboundPacketConnectBssid
}

func (s *s2cConnectBssid) WriteTo(dst []byte) (int, error) {
	writer := pspnet.NewSliceWriter(dst)
	writer.Bytes(s.macAddr[:])
	return writer.Pos(), nil
}

type s2cClientConnected struct {
	displayName pspnet.DisplayName
	macAddr     pspnet.MacAddr
	ipAddr      uint32
}

func (s *s2cClientConnected) Type() outboundPacketType {
	return outboundPacketClientConnected
}

func (s *s2cClientConnected) WriteTo(dst []byte) (int, error) {
	writer := pspnet.NewSliceWriter(dst)
	writer.Bytes(s.displayName[:])
	writer.Bytes(s.macAddr[:])
	writer.Uint32(s.ipAddr)
	return writer.Pos(), nil
}

type s2cClientDisconnected struct {
	ipAddr uint32
}

func (s *s2cClientDisconnected) Type() outboundPacketType {
	return outboundPacketClientDisconnected
}

func (s *s2cClientDisconnected) WriteTo(dst []byte) (int, error) {
	writer := pspnet.NewSliceWriter(dst)
	writer.Uint32(s.ipAddr)
	return writer.Pos(), nil
}

type s2cChat struct {
	message     []byte
	displayName pspnet.DisplayName
}

func (s *s2cChat) Type() outboundPacketType {
	return outboundPacketChat
}

func (s *s2cChat) WriteTo(dst []byte) (int, error) {
	writer := pspnet.NewSliceWriter(dst)
	writer.Bytes(s.displayName[:])
	writer.Bytes(s.message)
	return writer.Pos(), nil
}
