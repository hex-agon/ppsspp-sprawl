package adhocctl

import (
	"embed"
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/rs/zerolog/log"
)

//go:embed status.gohtml
var statusTemplateFS embed.FS

var statusTemplate = template.Must(template.ParseFS(statusTemplateFS, "status.gohtml"))

type StatusResponse struct {
	Titles       []TitleStatus `json:"titles"`
	TotalPlayers int           `json:"total_players"`
}

func StatusHandler(srv Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		titles := srv.GetActiveTitles()
		totalPlayers := 0
		for _, t := range titles {
			totalPlayers += t.PlayerCount
		}

		status := StatusResponse{
			Titles:       titles,
			TotalPlayers: totalPlayers,
		}
		format := r.URL.Query().Get("format")
		
		if format == "json" {
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(status); err != nil {
				log.Error().Err(err).Msg("[ctl] failed to encode status response")
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
			return
		}

		w.Header().Set("Content-Type", "text/html")
		if err := statusTemplate.Execute(w, status); err != nil {
			log.Error().Err(err).Msg("[ctl] failed to execute status template")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}
