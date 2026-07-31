package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "internal/titledb/titledb.sqlite")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open sqlite")
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	rows, err := db.Query("SELECT title_id, name, compatible_with FROM title ORDER BY title_id")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to query titles")
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	var titles []Title
	for rows.Next() {
		var t Title
		var titleId []byte
		var compatibleWith *string
		if err := rows.Scan(&titleId, &t.Name, &compatibleWith); err != nil {
			log.Fatal().Err(err).Msg("failed to scan row")
		}
		copy(t.TitleId[:], titleId)
		if compatibleWith != nil {
			var tid TitleId
			copy(tid[:], *compatibleWith)
			t.CompatibleWith = &tid
		} else {
			t.CompatibleWith = nil
		}
		titles = append(titles, t)
	}

	packed, err := pack(titles)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to pack titles")
	}

	if err = os.WriteFile("internal/titledb/titledb.bin", packed, 0644); err != nil {
		log.Fatal().Err(err).Msg("failed to write titledb.bin")
	}
	log.Info().Msg("successfully packed titles")
}

type TitleId [9]byte

type Title struct {
	TitleId        TitleId
	Name           string
	CompatibleWith *TitleId
}

// pack encodes titles into a compact custom binary format, laid out as
// a fixed-size header, a fixed-size entry table, and a variable-length
// string blob:
//
//	┌─────────────────────────────────────────────────────────────────┐
//	│  HEADER  (4 bytes)                                              │
//	├─────────────────────────────────────────────────────────────────┤
//	│  title_count   uint32 LE     number of entries, N               │
//	└─────────────────────────────────────────────────────────────────┘
//
//	┌────────────────────────────────────────────────────────────────┐
//	│  ENTRY TABLE  (N x 16 bytes, sorted by title_id)               │
//	├────────────────────────────────────────────────────────────────┤
//	│  title_id           9 bytes    raw title id                    │
//	│  compatible_with    uint16 LE  index of the compatible entry,  │
//	│                                 or 0xFFFF if there is none     │
//	│  name_offset        uint32 LE  offset of the name in the blob  │
//	│  name_len           1 byte     length of the name, in bytes    │
//	└────────────────────────────────────────────────────────────────┘
//
//	┌────────────────────────────────────────────────────────────────┐
//	│  STRING BLOB  (variable length)                                │
//	├────────────────────────────────────────────────────────────────┤
//	│  name(0) name(1) name(2) ... name(N-1)                         │
//	│  raw UTF-8 bytes, back-to-back, no separators or terminators   │
//	└────────────────────────────────────────────────────────────────┘
//
// Putting it all together, the bytes on disk look like this:
//
//	 0                                                     N-1
//	┌────────┬─────────┬─────────┬─────┬──────────┬─────────────────┐
//	│ header │ entry[0]│ entry[1]│ ... │entry[N-1]│  string blob    │
//	│  4 B   │  16 B   │  16 B   │     │   16 B   │  variable size  │
//	└────────┴─────────┴─────────┴─────┴──────────┴─────────────────┘
func pack(titles []Title) ([]byte, error) {
	if len(titles) > 0xffff {
		return nil, fmt.Errorf("too many titles: %d (max 65535)", len(titles))
	}

	// reverse lookup, titleId -> idx
	index := make(map[TitleId]uint16, len(titles))
	for i, t := range titles {
		index[t.TitleId] = uint16(i)
	}

	// Pre-compute the offsets for each title
	titleOffsets := make([]uint32, len(titles))
	var currentOffset uint32

	for i, t := range titles {
		if len(t.Name) > 0xff {
			return nil, fmt.Errorf("title name '%q' exceeds 255 bytes", t.Name)
		}
		titleOffsets[i] = currentOffset
		currentOffset += uint32(len(t.Name))
	}
	w := NewBufWriter()

	// Starts here
	w.Uint32(uint32(len(titles)))

	// Entries
	for i, t := range titles {
		var compatIdx uint16 = 0xFFFF
		if t.CompatibleWith != nil {
			idx, ok := index[*t.CompatibleWith]
			if !ok {
				return nil, fmt.Errorf("compatible title not found for %q", t.Name)
			}
			compatIdx = idx
		}
		// weird order due to memory-alignment
		w.Uint32(titleOffsets[i])  // NameOffset
		w.Uint16(compatIdx)        // CompatibleWith
		w.Uint8(byte(len(t.Name))) // NameLen
		w.Bytes(t.TitleId[:])      // TitleId
	}

	// Title names
	for _, t := range titles {
		w.Bytes([]byte(t.Name))
	}
	return w.BytesResult(), nil
}

type BufWriter struct {
	buf bytes.Buffer
}

func NewBufWriter() *BufWriter {
	return &BufWriter{}
}

func (w *BufWriter) Pos() int {
	return w.buf.Len()
}

func (w *BufWriter) Uint32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.buf.Write(b[:])
}

func (w *BufWriter) Uint16(v uint16) {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	w.buf.Write(b[:])
}

func (w *BufWriter) Uint8(v uint8) {
	w.buf.WriteByte(v)
}

func (w *BufWriter) Bytes(v []byte) {
	w.buf.Write(v)
}

func (w *BufWriter) Skip(n int) {
	w.buf.Write(make([]byte, n))
}

func (w *BufWriter) BytesResult() []byte {
	return w.buf.Bytes()
}
