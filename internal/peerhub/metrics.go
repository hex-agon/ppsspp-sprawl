package peerhub

import "github.com/prometheus/client_golang/prometheus"

var (
	listeningPeersCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sprawl_hub_listening_peers_count",
		Help: "The number of listening peers in the peer hub.",
	})
	connectedPeersCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sprawl_hub_connected_peers_count",
		Help: "The number of connected peers in the peer hub.",
	})
	peersRxBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sprawl_hub_peers_rx_bytes",
		Help: "The number of bytes read by peers.",
	})
	peersTxBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sprawl_hub_peers_tx_bytes",
		Help: "The number of bytes written by peers.",
	})
)

func init() {
	prometheus.MustRegister(
		listeningPeersCount,
		connectedPeersCount,
		peersRxBytes,
		peersTxBytes,
	)
}
