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
	packetSizeHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "sprawl_exchange_packet_size_bytes",
		Help:    "The size of packets flowing through the exchange decoder.",
		Buckets: []float64{32, 64, 128, 256, 512, 1024, 2048, 4096},
	})
	packetEntropyHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "sprawl_exchange_packet_entropy_bits",
			Help: "Entropy of payloads in bits per byte",
			Buckets: []float64{
				0, 1, 2, 3, 4, 5, 6, 7, 8,
			},
		},
	)
)

func init() {
	prometheus.MustRegister(
		clientsCount,
		clientsRxBytes,
		clientsTxBytes,
		packetSizeHistogram,
		packetEntropyHistogram,
	)
}
