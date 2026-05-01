package pspnet

import (
	"encoding/hex"
	"errors"
	"strings"
)

const MacAddrLen = 6

type MacAddr [MacAddrLen]byte

var ErrInvalidMacAddr = errors.New("invalid macaddr")

// MustParseMacAddr parses a MAC address string in the format "xx:xx:xx:xx:xx:xx"
// and returns a MacAddr. Panics if the string format is invalid.
func MustParseMacAddr(str string) MacAddr {
	addr, err := ParseMacAddr(str)
	if err != nil {
		panic(err)
	}
	return addr
}

// ParseMacAddr parses a MAC address string in the format "xx:xx:xx:xx:xx:xx"
// and returns a MacAddr. Returns an error if the string format is invalid.
func ParseMacAddr(str string) (MacAddr, error) {
	var addr MacAddr
	parts := strings.Split(str, ":")
	if len(parts) != MacAddrLen {
		return addr, ErrInvalidMacAddr
	}
	for i, part := range parts {
		b, err := hex.DecodeString(part)
		if err != nil || len(b) != 1 {
			return addr, err
		}
		addr[i] = b[0]
	}
	return addr, nil
}

func (m *MacAddr) String() string {
	parts := make([]string, MacAddrLen)
	for i := 0; i < 6; i++ {
		parts[i] = hex.EncodeToString(m[i : i+1])
	}
	return strings.Join(parts, ":")
}
