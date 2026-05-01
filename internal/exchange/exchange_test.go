package exchange

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sprawl/internal/pspnet"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestExchange(t *testing.T) {
	defer goleak.VerifyNone(t)

	t.Run("RoutePacket", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ex := NewExchange(ctx)

		srvConnA, clientConnA := net.Pipe()
		defer func() { assert.NoError(t, srvConnA.Close()) }()
		defer func() { assert.NoError(t, clientConnA.Close()) }()

		srvConnB, clientConnB := net.Pipe()
		defer func() { assert.NoError(t, srvConnB.Close()) }()
		defer func() { assert.NoError(t, clientConnB.Close()) }()

		clientMacA := pspnet.MustParseMacAddr("00:01:02:03:04:05")
		clientPortA := uint16(1001)

		clientMacB := pspnet.MustParseMacAddr("00:01:02:03:04:06")
		clientPortB := uint16(1002)

		RegisterClient(ctx, ex, clientConnA, clientMacA, clientPortA)
		RegisterClient(ctx, ex, clientConnB, clientMacB, clientPortB)

		// wait for client registration
		assert.Eventually(t, func() bool {
			return ex.findClient(newRoutingKey(clientMacA, clientPortA)) != nil &&
				ex.findClient(newRoutingKey(clientMacB, clientPortB)) != nil
		}, 1*time.Second, 10*time.Millisecond)

		packetSize := uint32(5)
		payload := []byte("hello")

		header := make([]byte, pdpHeaderSize)
		copy(header[0:6], clientMacB[:])
		binary.LittleEndian.PutUint16(header[8:10], clientPortB)
		binary.LittleEndian.PutUint32(header[10:14], packetSize)

		_, _ = srvConnA.Write(append(header, payload...))

		received := make([]byte, pdpHeaderSize+packetSize)
		_ = srvConnB.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, err := io.ReadFull(srvConnB, received)

		require.NoError(t, err)

		assert.Equal(t, clientMacA[:], received[0:6], "Expected clientA's mac address")
		assert.Equal(t, clientPortA, binary.LittleEndian.Uint16(received[8:10]), "Expected clientA's port")
		assert.Equal(t, packetSize, binary.LittleEndian.Uint32(received[10:14]))
		assert.Equal(t, payload, received[pdpHeaderSize:])
	})

	t.Run("RouteUnknownTarget", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ex := NewExchange(ctx)

		mac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
		ex.routePacket(newRoutingKey(mac, 0), []byte("data"))
		// should just do nothing
	})
}
