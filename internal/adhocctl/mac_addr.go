package adhocctl

import (
	"encoding/hex"
	"strings"
)

const macAddrLen = 6

type macAddr [macAddrLen]byte

func (m *macAddr) String() string {
	parts := make([]string, macAddrLen)
	for i := 0; i < 6; i++ {
		parts[i] = hex.EncodeToString(m[i : i+1])
	}
	return strings.Join(parts, ":")
}
