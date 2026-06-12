package exchange

import "github.com/prometheus/client_golang/prometheus"

var (
	clientsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sprawl_exchange_clients_count",
		Help: "The number of clients currently in the exchange.",
	})
	clientsRxBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sprawl_exchange_clients_rx_bytes",
		Help: "The number of bytes read by clients.",
	})
	clientsTxBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sprawl_exchange_clients_tx_bytes",
		Help: "The number of bytes written by clients.",
	})
)

func init() {
	prometheus.MustRegister(
		clientsCount,
		clientsRxBytes,
		clientsTxBytes,
	)
}
