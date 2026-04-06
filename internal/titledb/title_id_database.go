package titledb

import (
	"embed"

	"github.com/BurntSushi/toml"
	"github.com/rs/zerolog/log"
)

// Certain games released in different regions are protocol compatible with each other despite having different titleIds.

//go:embed titles.toml
var embedFS embed.FS

type GameEntry struct {
	Name           string   `toml:"name"`
	Region         string   `toml:"region"`
	CompatibleWith []string `toml:"compatible_with,omitempty"`
}

type Title = GameEntry

var Titles = make(map[TitleId]Title)
var compatibilityMap = make(map[TitleId]TitleId)

func init() {
	data, err := embedFS.ReadFile("titles.toml")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load embedded titles")
	}
	var rawTitles map[string]Title

	if err := toml.Unmarshal(data, &rawTitles); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse titles")
	}

	for titleId, entry := range rawTitles {
		tid := NewTitleId(titleId)
		Titles[tid] = entry

		for _, compatibleId := range entry.CompatibleWith {
			compatibilityMap[NewTitleId(compatibleId)] = tid
		}
	}
}

// CompatibleTitleId replaces the given titleId with the compatible titleId
func CompatibleTitleId(tid TitleId) (TitleId, bool) {
	id, ok := compatibilityMap[tid]
	return id, ok
}
