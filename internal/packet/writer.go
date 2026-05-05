package packet

import "encoding/binary"

type SliceWriter struct {
	dst []byte
	pos int
}

func NewSliceWriter(dst []byte) *SliceWriter {
	return &SliceWriter{dst: dst}
}

func (w *SliceWriter) Pos() int {
	return w.pos
}

func (w *SliceWriter) Uint32(v uint32) {
	binary.LittleEndian.PutUint32(w.dst[w.pos:], v)
	w.pos += 4
}

func (w *SliceWriter) Uint16(v uint16) {
	binary.LittleEndian.PutUint16(w.dst[w.pos:], v)
	w.pos += 2
}

func (w *SliceWriter) Uint8(v uint8) {
	w.dst[w.pos] = v
	w.pos++
}

func (w *SliceWriter) Bytes(v []byte) {
	copy(w.dst[w.pos:], v)
	w.pos += len(v)
}

func (w *SliceWriter) Skip(n int) {
	for i := 0; i < n; i++ {
		w.dst[w.pos+i] = 0
	}
	w.pos += n
}
