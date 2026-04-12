package adhocctl

import "github.com/prometheus/client_golang/prometheus"

var (
	clientsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sprawl_ctl_clients_count",
		Help: "The number of clients currently connected to the ctl server.",
	})
)

func init() {
	prometheus.MustRegister(
		clientsCount,
	)
}
