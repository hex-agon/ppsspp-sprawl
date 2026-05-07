package peerhub

import (
	"bufio"
	"io"
)

type encoder struct {
	writer *bufio.Writer
	buffer []byte
}

func newEncoder(writer io.Writer) *encoder {
	return &encoder{
		writer: bufio.NewWriter(writer),
		buffer: make([]byte, p2pHeaderSize+p2pMaxPacketSize),
	}
}

func (e *encoder) writeAndFlush(pkt p2pOutboundPacket) error {
	n, err := pkt.writeTo(e.buffer[:])

	if err != nil {
		return err
	}
	if _, err = e.writer.Write(e.buffer[:n]); err != nil {
		return err
	}
	peersTxBytes.Add(float64(n))
	return e.writer.Flush()
}
