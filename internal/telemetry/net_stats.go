package telemetry

import (
	"sync"
	"time"
)

type bucket struct {
	packets uint32
	bytes   uint32
}

type NetStats struct {
	mu sync.Mutex

	buckets []bucket
	index   int

	lastTick time.Time
	window   time.Duration
}

func NewNetStats(seconds int) *NetStats {
	return &NetStats{
		buckets:  make([]bucket, seconds),
		lastTick: time.Now(),
		window:   time.Second,
	}
}

func (n *NetStats) tick() {
	now := time.Now()
	elapsed := now.Sub(n.lastTick)
	steps := int(elapsed / n.window)

	if steps <= 0 {
		return
	}
	if steps > len(n.buckets) {
		steps = len(n.buckets)
	}

	for range steps {
		n.index = (n.index + 1) % len(n.buckets)
		n.buckets[n.index] = bucket{}
	}
	n.lastTick = n.lastTick.Add(time.Duration(steps) * n.window)
}

func (n *NetStats) ObservePacket(size uint32) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.tick()

	n.buckets[n.index].packets++
	n.buckets[n.index].bytes += size
}

func (n *NetStats) Stats() (pps float64, bps float64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.tick()

	var totalPackets uint32
	var totalBytes uint32

	for _, b := range n.buckets {
		totalPackets += b.packets
		totalBytes += b.bytes
	}

	seconds := float64(len(n.buckets))

	pps = float64(totalPackets) / seconds
	bps = float64(totalBytes) / seconds

	return
}
