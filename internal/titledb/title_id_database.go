package titledb

import (
	"database/sql"
	"embed"
	"errors"

	"github.com/rs/zerolog/log"
	_ "modernc.org/sqlite"
	"modernc.org/sqlite/vfs"
)

// Certain games released in different regions are protocol compatible with each other despite having different titleIds.

//go:embed titledb.sqlite
var embedFS embed.FS

type Title struct {
	Name   string
	Region string
}

var db *sql.DB

func init() {
	vfsName, _, err := vfs.New(embedFS)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create SQLite VFS")
	}
	db, err = sql.Open("sqlite", "titledb.sqlite?vfs="+vfsName+"&mode=ro")

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open titles database")
	}
}

func GetTitle(tid TitleId) (name string, region string, found bool) {
	err := db.QueryRow("SELECT name, region FROM title WHERE title_id = ?", tid.String()).Scan(&name, &region)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return name, region, false
		}
		log.Error().Err(err).Msg("Failed to query title")
		return name, region, false
	}
	return name, region, true
}

func CompatibleTitleId(tid TitleId) (TitleId, bool) {
	var parentId *string
	err := db.QueryRow("SELECT compatible_with FROM title WHERE title_id = ?", tid.String()).Scan(&parentId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tid, false
		}
		log.Error().Err(err).Msg("Failed to query compatible title")
		return tid, false
	}
	if parentId == nil {
		return tid, false
	}
	return NewTitleId(*parentId), true
}
