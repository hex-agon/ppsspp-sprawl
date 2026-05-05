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
	"go.uber.org/goleak"
)

func TestClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	t.Run("TXQueueFull", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		srvConn, clientConn := net.Pipe()
		defer func() { assert.NoError(t, srvConn.Close()) }()
		defer func() { assert.NoError(t, clientConn.Close()) }()

		cli := &client{
			conn:      srvConn,
			decoder:   newDecoder(srvConn),
			encoder:   newEncoder(srvConn, pdpHeaderSize+pdpMaxPacketSize),
			ctx:       ctx,
			ctxCancel: cancel,
			txQueue:   make(chan *pdpPacket, 1),
		}
		err := cli.write(&pdpPacket{payload: []byte("fill tx queue")}, writePacketTimeout)
		assert.NoError(t, err)

		err = cli.write(&pdpPacket{payload: []byte("write timeout")}, 10*time.Millisecond)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tx queue full")
	})

	t.Run("OversizedPacket", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ex := NewExchange(ctx)
		srvConn, clientConn := net.Pipe()
		defer func() { assert.NoError(t, srvConn.Close()) }()
		defer func() { assert.NoError(t, clientConn.Close()) }()

		clientMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
		clientPort := uint16(1001)
		key := newRoutingKey(clientMac, clientPort)

		ex.RegisterClient(ctx, clientConn, clientMac, clientPort)

		assert.Eventually(t, func() bool {
			return ex.findClient(key) != nil
		}, 1*time.Second, 10*time.Millisecond)

		header := make([]byte, pdpHeaderSize)
		binary.LittleEndian.PutUint32(header[10:14], pdpMaxPacketSize+1)

		_, _ = srvConn.Write(header)

		assert.Eventually(t, func() bool {
			return ex.findClient(key) == nil
		}, 1*time.Second, 100*time.Millisecond, "Client should be unregistered due to oversized packet")

		_ = srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := clientConn.Read([]byte{})
		assert.Error(t, io.EOF, err, "Client connection should be closed after being unregistered")
	})

	t.Run("ReadError", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ex := NewExchange(ctx)
		srvConn, clientConn := net.Pipe()

		clientMac := pspnet.MustParseMacAddr("00:01:02:03:04:05")
		clientPort := uint16(1001)
		key := newRoutingKey(clientMac, clientPort)

		ex.RegisterClient(ctx, clientConn, clientMac, clientPort)

		assert.Eventually(t, func() bool {
			return ex.findClient(key) != nil
		}, 1*time.Second, 100*time.Millisecond)

		// closing the srv conn to trigger a read error on the clientConn
		assert.NoError(t, srvConn.Close())

		assert.Eventually(t, func() bool {
			return ex.findClient(key) == nil
		}, 1*time.Second, 100*time.Millisecond, "Client should be unregistered on connection close")

		_ = srvConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		_, err := clientConn.Read([]byte{})
		assert.Error(t, io.EOF, err, "Client connection should be closed after being unregistered")
	})
}
