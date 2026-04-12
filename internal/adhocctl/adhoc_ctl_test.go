package adhocctl

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"os"
	"sprawl/internal/titledb"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdhocCtl(t *testing.T) {
	server, err := NewCtlServer("127.0.0.1", "0")
	require.NoError(t, err)
	defer func() { assert.NoError(t, server.Shutdown()) }()

	connectClient := func(t *testing.T, name displayName, tid titledb.TitleId) net.Conn {
		conn, err := net.Dial("tcp", server.Addr().String())
		require.NoError(t, err)

		var randomMac macAddr
		_, err = rand.Read(randomMac[:])
		assert.NoError(t, err)

		loginPacket := struct {
			PacketId   uint8
			ClientAddr macAddr
			Name       displayName
			Tid        titledb.TitleId
		}{
			PacketId:   packetTypeLogin.id,
			ClientAddr: randomMac,
			Name:       name,
			Tid:        tid,
		}
		assert.NoError(t, binary.Write(conn, binary.BigEndian, loginPacket))

		return conn
	}

	assertDisconnected := func(t *testing.T, conn net.Conn) {
		buffer := make([]byte, 1)
		_, err := conn.Read(buffer)
		require.Equal(t, io.EOF, err)
	}

	t.Run("ValidClient", func(t *testing.T) {
		conn := connectClient(t, newDisplayName("displayname"), titledb.NewTitleId("ULUS12345"))

		// read to ensure we haven't been disconnected (no EOF)
		buffer := make([]byte, 1)
		assert.NoError(t, conn.SetReadDeadline(time.Now().Add(50*time.Millisecond)))
		_, err := conn.Read(buffer)
		// If we read timeout, connection is still alive
		require.Error(t, err)
		require.True(t, os.IsTimeout(err))

		defer func() { assert.NoError(t, conn.Close()) }()
	})

	t.Run("InvalidDisplayName", func(t *testing.T) {
		conn := connectClient(t, newDisplayName("invalid-display-name"), titledb.NewTitleId("ULUS12345"))

		assertDisconnected(t, conn)

		defer func() { assert.NoError(t, conn.Close()) }()
	})

	t.Run("InvalidTitleId", func(t *testing.T) {
		conn := connectClient(t, newDisplayName("displayname"), titledb.NewTitleId("badtitleid"))

		assertDisconnected(t, conn)

		defer func() { assert.NoError(t, conn.Close()) }()
	})
}
