package adhocctl

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog/log"
)

type StatusResponse struct {
	Titles []TitleStatus `json:"titles"`
}

func StatusHandler(srv Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := StatusResponse{
			Titles: srv.GetActiveGroups(),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(status); err != nil {
			log.Error().Err(err).Msg("[ctl] failed to encode status response")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}
