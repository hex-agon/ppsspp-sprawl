package peerhub

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeerKey(t *testing.T) {
	mac, _ := hex.DecodeString("701e7be12c05")
	port := uint16(28468)
	key := newPeerKey(mac, port)

	t.Run("New", func(t *testing.T) {
		assert.Equal(t, mac, key[:6])
		assert.Equal(t, uint8(0x34), key[6])
		assert.Equal(t, uint8(0x6f), key[7])
	})

	t.Run("Encode", func(t *testing.T) {
		encoded := key.encode()
		assert.Len(t, encoded, 10, "Encoded bytes should be 10 bytes")

		assert.Equal(t, key[:6], encoded[:6], "Encoded MAC address should match")
		assert.Equal(t, uint8(0x34), encoded[8], "Encoded port should match")
		assert.Equal(t, uint8(0x6f), encoded[9], "Encoded port should match")
	})
}
