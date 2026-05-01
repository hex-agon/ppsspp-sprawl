package debug

import (
	"embed"
	"html/template"
	"net/http"
	"sprawl/internal/adhocctl"
	"sprawl/internal/exchange"
	"sprawl/internal/peerhub"

	"github.com/rs/zerolog/log"
)

//go:embed debug.gohtml
var debugTemplateFS embed.FS

var debugTemplate = template.Must(template.ParseFS(debugTemplateFS, "debug.gohtml"))

type DebugData struct {
	CtlClients      []adhocctl.CtlClientInfo
	ExchangeClients []exchange.ClientInfo
	ListeningPeers  []peerhub.PeerInfo
	ConnectedPeers  []peerhub.PeerInfo
}

func DebugHandler(srv adhocctl.Server, ex *exchange.Exchange, hub *peerhub.PeerHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		listening, connected := hub.GetPeers()
		data := DebugData{
			CtlClients:      srv.GetClients(),
			ExchangeClients: ex.GetClients(),
			ListeningPeers:  listening,
			ConnectedPeers:  connected,
		}

		w.Header().Set("Content-Type", "text/html")
		if err := debugTemplate.Execute(w, data); err != nil {
			log.Error().Err(err).Msg("[ctl] failed to execute debug template")
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}
