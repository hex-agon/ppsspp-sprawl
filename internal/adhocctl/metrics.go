package adhocctl

import "github.com/prometheus/client_golang/prometheus"

var (
	clientsCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sprawl_ctl_clients_count",
		Help: "The number of clients currently connected to the ctl server.",
	}, []string{"titleId"})
)

func init() {
	prometheus.MustRegister(
		clientsCount,
	)
}
