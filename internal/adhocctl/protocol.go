package adhocctl

import (
	"bytes"
	"encoding/hex"
	"strings"
)

type packetType struct {
	id   uint8
	size uint8
}

var (
	packetTypePing         = packetType{id: 0, size: 0}
	packetTypeLogin        = packetType{id: 1, size: 144}
	packetTypeConnect      = packetType{id: 2, size: 9}
	packetTypeDisconnect   = packetType{id: 3, size: 0}
	packetTypeScan         = packetType{id: 4, size: 0}
	packetTypeScanComplete = packetType{id: 5, size: 0}
	packetTypeConnectBssid = packetType{id: 6, size: 0}
	packetTypeChat         = packetType{id: 7, size: 65}
)

var c2sPacketTypes = []packetType{
	packetTypePing,
	{}, // login is a c2s packet, but it is handled differently
	packetTypeConnect,
	packetTypeDisconnect,
	packetTypeScan,
	{},
	{},
	packetTypeChat,
}

const macAddrLen = 6

type macAddr [macAddrLen]byte

func (m *macAddr) String() string {
	parts := make([]string, macAddrLen)
	for i := 0; i < 6; i++ {
		parts[i] = hex.EncodeToString(m[i : i+1])
	}
	return strings.Join(parts, ":")
}

const groupIdLen = 8

type groupId [groupIdLen]byte

func (g *groupId) String() string {
	terminator := bytes.IndexByte(g[:], 0)
	if terminator == -1 {
		return string(g[:])
	}
	return string(g[:terminator])
}

func (g *groupId) valid() bool {
	length := bytes.IndexByte(g[:], 0)

	if length == -1 {
		length = groupIdLen
	}

	for i := 0; i < length; i++ {
		char := g[i]
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

const displayNameLen = 128

// displayName is a null-terminated string composed of a-z, A-Z, 0-9 characters with a maximum length of 128 bytes.
type displayName [displayNameLen]byte

func (d *displayName) String() string {
	terminator := bytes.IndexByte(d[:], 0)
	if terminator == -1 {
		terminator = displayNameLen
	}
	return string(d[:terminator])
}

func (d *displayName) valid() bool {
	length := bytes.IndexByte(d[:], 0)

	if length < 1 {
		return false
	}

	for i := 0; i < length; i++ {
		char := d[i]
		// TODO: I think there's some special characters that are allowed here too (- and _ comes to mind)
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

type basePacket struct {
	packetTypeId uint8
}

type s2cScanReply struct {
	basePacket
	gid         groupId
	founderAddr macAddr
}
type s2cScanComplete struct {
	basePacket
}

type s2cConnectBssid struct {
	basePacket
	macAddr macAddr
}

type s2cClientConnected struct {
	basePacket
	displayName displayName
	macAddr     macAddr
	ipAddr      uint32
}

type s2cDisconnected struct {
	basePacket
	ipAddr uint32
}
