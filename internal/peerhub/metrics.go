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
	packetSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sprawl_hub_packet_size_bytes",
		Help:    "The size of packets flowing through the peerhub decoder.",
		Buckets: []float64{32, 64, 128, 256, 512, 1024, 2048, 4096},
	})
)

func init() {
	prometheus.MustRegister(
		listeningPeersCount,
		connectedPeersCount,
		peersRxBytes,
		peersTxBytes,
		packetSizeHistogram,
	)
}
