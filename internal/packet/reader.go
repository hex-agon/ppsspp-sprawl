package packet

import "encoding/binary"

type SliceReader struct {
	b   []byte
	off int
}

func NewSliceReader(b []byte) *SliceReader {
	return &SliceReader{b: b}
}

func (r *SliceReader) Uint32() uint32 {
	v := binary.LittleEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}

func (r *SliceReader) Uint16() uint16 {
	v := binary.LittleEndian.Uint16(r.b[r.off:])
	r.off += 2
	return v
}

func (r *SliceReader) Uint8() uint8 {
	v := r.b[r.off]
	r.off++
	return v
}

func (r *SliceReader) Bytes(n int) []byte {
	v := r.b[r.off : r.off+n]
	r.off += n
	return v
}
