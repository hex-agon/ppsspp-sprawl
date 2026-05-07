package adhocctl

import (
	"embed"
	"encoding/json"
	"html/template"
	"net/http"
	"sort"

	"github.com/rs/zerolog/log"
)

//go:embed status.gohtml
var statusTemplateFS embed.FS

var statusTemplate = template.Must(template.ParseFS(statusTemplateFS, "status.gohtml"))

func StatusHandler(srv Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		titles := srv.GetActiveTitles()
		sort.Slice(titles, func(i, j int) bool {
			return titles[i].TitleID < titles[j].TitleID
		})

		format := r.URL.Query().Get("format")
		switch format {
		case "aemu":
			statusAEMU(w, titles)
		case "json":
			statusJSON(w, titles)
		default:
			statusHTML(w, titles)
		}
	}
}

func statusAEMU(w http.ResponseWriter, titles []TitleStatus) {
	type user struct {
		Name string `json:"name"`
	}

	type group struct {
		Name      string `json:"name"`
		UserCount int    `json:"usercount"`
		Users     []user `json:"users"`
	}

	type game struct {
		Name      string   `json:"name"`
		UserCount int      `json:"usercount"`
		GameIDs   []string `json:"game_ids"`
		Groups    []group  `json:"groups"`
	}

	type response struct {
		UserCount int    `json:"usercount"`
		Games     []game `json:"games"`
	}

	totalPlayers := 0
	for _, t := range titles {
		totalPlayers += t.PlayerCount
	}

	aemu := response{
		UserCount: totalPlayers,
		Games:     make([]game, 0, len(titles)),
	}

	for _, t := range titles {
		g := game{
			Name:      t.Name,
			UserCount: t.PlayerCount,
			GameIDs:   []string{t.TitleID},
			Groups:    make([]group, 0, len(t.Groups)),
		}
		for _, gs := range t.Groups {
			gr := group{
				Name:      gs.ID,
				UserCount: len(gs.Players),
				Users:     make([]user, 0, len(gs.Players)),
			}
			for _, p := range gs.Players {
				gr.Users = append(gr.Users, user{
					Name: p,
				})
			}
			g.Groups = append(g.Groups, gr)
		}
		aemu.Games = append(aemu.Games, g)
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(aemu); err != nil {
		log.Error().Err(err).Msg("[ctl] failed to encode aemu response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func statusJSON(w http.ResponseWriter, titles []TitleStatus) {
	totalPlayers := 0
	for _, t := range titles {
		totalPlayers += t.PlayerCount
	}

	response := struct {
		Titles       []TitleStatus `json:"titles"`
		TotalPlayers int           `json:"total_players"`
	}{
		Titles:       titles,
		TotalPlayers: totalPlayers,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error().Err(err).Msg("[ctl] failed to encode status response")
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

func statusHTML(w http.ResponseWriter, titles []TitleStatus) {
	totalPlayers := 0
	for _, t := range titles {
		totalPlayers += t.PlayerCount
	}

	var data = map[string]interface{}{
		"Titles":       titles,
		"TotalPlayers": totalPlayers,
	}

	w.Header().Set("Content-Type", "text/html")
	if err := statusTemplate.Execute(w, data); err != nil {
		log.Error().Err(err).Msg("[ctl] failed to execute status template")
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}
