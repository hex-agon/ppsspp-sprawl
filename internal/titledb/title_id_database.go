package titledb

import (
	"bytes"
	"embed"
	"encoding/binary"
	"sort"
	"unsafe"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
)

//go:embed titledb.bin
var embedFS embed.FS

var (
	dbData        []byte
	entries       []titleEntry
	strTableStart int
)

const (
	dbHeaderSize        = 4
	dbEntrySize         = 16
	noCompatibleTitleId = 0xffff
)

type titleEntry struct {
	// Do NOT modify the field order to not break memory alignment
	NameOffset        uint32
	CompatibleWithIdx uint16
	NameLen           uint8
	TitleId           TitleId
}

func (t *titleEntry) TitleName() string {
	nameOffset := int(t.NameOffset)
	nameLength := int(t.NameLen)

	start := strTableStart + nameOffset
	return string(dbData[start : start+nameLength])
}

func init() {
	var err error
	dbData, err = embedFS.ReadFile("titledb.bin")

	if err != nil {
		log.Fatal().Err(err).Msg("failed to read embedded titledb.bin")
	}

	if len(dbData) < dbHeaderSize {
		log.Fatal().Msg("invalid/corrupted titledb.bin")
	}
	entryCount := binary.LittleEndian.Uint32(dbData[:dbHeaderSize])

	if entryCount == 0 {
		entries = []titleEntry{}
		return
	}
	entryTableSize := int(entryCount * dbEntrySize)

	entries = unsafe.Slice(
		(*titleEntry)(unsafe.Pointer(&dbData[dbHeaderSize])),
		int(entryCount),
	)
	strTableStart = dbHeaderSize + entryTableSize

	for i, entry := range entries {
		compatIdx := int(entry.CompatibleWithIdx)

		if compatIdx != 0xFFFF && compatIdx >= len(entries) {
			log.Fatal().Msgf("titledb: entry %d points to an invalid compatible title index: %d", i, compatIdx)
		}

		if int(entry.NameOffset) > len(dbData)-strTableStart {
			log.Fatal().Msgf("titledb: entry %d has invalid name offset: %d (out of range)", i, entry.NameOffset)
		}
	}
}

func GetTitle(tid TitleId) (name string, found bool) {
	entry, ok := findTitle(tid)
	if !ok {
		return "", false
	}
	return entry.TitleName(), true
}

func CompatibleTitleId(tid TitleId) (TitleId, bool) {
	entry, ok := findTitle(tid)
	if !ok {
		return tid, false
	}
	compatibleIndex := entry.CompatibleWithIdx

	if compatibleIndex == noCompatibleTitleId {
		return tid, false
	}
	return entries[compatibleIndex].TitleId, true
}

func findTitle(tid TitleId) (entry *titleEntry, ok bool) {
	index := sort.Search(len(entries), func(index int) bool {
		return bytes.Compare(entries[index].TitleId[:], tid[:]) >= 0
	})
	if index == len(entries) {
		return nil, false
	}
	entry = &entries[index]

	if !bytes.Equal(entry.TitleId[:], tid[:]) {
		return nil, false
	}
	return entry, true
}
