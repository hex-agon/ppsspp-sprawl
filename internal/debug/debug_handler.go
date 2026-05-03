package debug

import (
	"cmp"
	"embed"
	"html/template"
	"net/http"
	"slices"
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
		ctlClients := srv.GetClients()
		exchangeClients := ex.GetClients()

		slices.SortFunc(ctlClients, func(a, b adhocctl.CtlClientInfo) int {
			return cmp.Compare(a.MAC, b.MAC)
		})
		slices.SortFunc(exchangeClients, func(a, b exchange.ClientInfo) int {
			return cmp.Compare(a.MAC, b.MAC)
		})
		slices.SortFunc(listening, func(a, b peerhub.PeerInfo) int {
			return cmp.Compare(a.MAC, b.MAC)
		})
		slices.SortFunc(connected, func(a, b peerhub.PeerInfo) int {
			return cmp.Compare(a.MAC, b.MAC)
		})

		data := DebugData{
			CtlClients:      ctlClients,
			ExchangeClients: exchangeClients,
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
