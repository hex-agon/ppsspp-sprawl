//nolint:all
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	initPacketSize = 24
	pdpHeaderSize  = 14
	p2pHeaderSize  = 4
)

type initType int32

const (
	initTypePdp        initType = 0
	initTypeP2pListen  initType = 1
	initTypeP2pConnect initType = 2
	initTypeP2pAccept  initType = 3
)

// loadtestConfig holds the parameters shared by every simulated client.
type loadtestConfig struct {
	addr       string
	packetSize int

	// churn enables simulating clients connecting, playing for a while and
	// then disconnecting, instead of keeping every connection alive for the
	// whole duration of the test.
	churn      bool
	minSession time.Duration
	maxSession time.Duration
	minIdle    time.Duration
	maxIdle    time.Duration

	// slowConsumerChance is the probability (0-1) that a given client will
	// behave as a slow consumer for its whole lifetime, reading data much
	// slower than it arrives. This is used to stress-test server-side
	// handling of slow/unresponsive clients (e.g. the server should
	// eventually detect and drop them).
	slowConsumerChance float64
	slowConsumerDelay  time.Duration
}

// This loadtest is 100% vibecoded
func main() {
	addr := flag.String("addr", "localhost:27313", "relay server address")
	connections := flag.Int("conns", 10, "number of connections per side (total 2x conns)")
	duration := flag.Duration("duration", 30*time.Second, "loadtest duration")
	testType := flag.String("type", "pdp", "test type (pdp or p2p)")
	packetSize := flag.Int("size", 256, "payload size")
	verbose := flag.Bool("v", false, "verbose output")
	churn := flag.Bool("churn", true, "simulate clients connecting and disconnecting instead of keeping connections alive for the whole test")
	minSession := flag.Duration("min-session", 5*time.Second, "minimum time a client stays connected before disconnecting (churn mode)")
	maxSession := flag.Duration("max-session", 120*time.Second, "maximum time a client stays connected before disconnecting (churn mode)")
	minIdle := flag.Duration("min-idle", 1*time.Second, "minimum time a disconnected client waits before reconnecting (churn mode)")
	maxIdle := flag.Duration("max-idle", 5*time.Second, "maximum time a disconnected client waits before reconnecting (churn mode)")
	slowConsumerChance := flag.Float64("slow-consumer-chance", 0.05, "probability (0-1) that a given client behaves as a slow consumer for its whole lifetime, to stress-test server-side handling of slow clients")
	slowConsumerDelay := flag.Duration("slow-consumer-delay", 1000*time.Millisecond, "extra delay a slow consumer client waits before processing (reading) each received packet")
	flag.Parse()

	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05", NoColor: true})

	if *maxSession < *minSession {
		log.Fatal().Msg("max-session must be >= min-session")
	}
	if *maxIdle < *minIdle {
		log.Fatal().Msg("max-idle must be >= min-idle")
	}
	if *slowConsumerChance < 0 || *slowConsumerChance > 1 {
		log.Fatal().Msg("slow-consumer-chance must be between 0 and 1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	if *churn {
		fmt.Printf("Starting %s loadtest: %d pairs, %d bytes/packet, duration %v, churn enabled (session %v-%v, idle %v-%v)\n",
			*testType, *connections, *packetSize, *duration, *minSession, *maxSession, *minIdle, *maxIdle)
	} else {
		fmt.Printf("Starting %s loadtest: %d pairs, %d bytes/packet, duration %v, churn disabled\n", *testType, *connections, *packetSize, *duration)
	}
	if *slowConsumerChance > 0 {
		fmt.Printf("Slow consumers: ~%.0f%% of clients, +%v delay per read\n", *slowConsumerChance*100, *slowConsumerDelay)
	}

	cfg := loadtestConfig{
		addr:               *addr,
		packetSize:         *packetSize,
		churn:              *churn,
		minSession:         *minSession,
		maxSession:         *maxSession,
		minIdle:            *minIdle,
		maxIdle:            *maxIdle,
		slowConsumerChance: *slowConsumerChance,
		slowConsumerDelay:  *slowConsumerDelay,
	}

	var wg sync.WaitGroup
	var sentPackets, recvPackets, errors, reconnects, slowConsumers int64
	var mu sync.Mutex

	for i := range *connections {
		wg.Add(2)
		client1MAC := generateMAC()
		client2MAC := generateMAC()
		client1Port := uint16(10000 + i)
		client2Port := uint16(20000 + i)

		if *testType == "pdp" {
			go runPDPClient(ctx, &wg, cfg, client1MAC, client1Port, client2MAC, client2Port, &sentPackets, &recvPackets, &errors, &reconnects, &slowConsumers, &mu)
			go runPDPClient(ctx, &wg, cfg, client2MAC, client2Port, client1MAC, client1Port, &sentPackets, &recvPackets, &errors, &reconnects, &slowConsumers, &mu)
		} else if *testType == "p2p" {
			go runP2PPeer(ctx, &wg, cfg, client1MAC, client1Port, client2MAC, client2Port, true, &sentPackets, &recvPackets, &errors, &reconnects, &slowConsumers, &mu)
			go runP2PPeer(ctx, &wg, cfg, client2MAC, client2Port, client1MAC, client1Port, false, &sentPackets, &recvPackets, &errors, &reconnects, &slowConsumers, &mu)
		} else {
			log.Fatal().Msgf("unknown test type: %s", *testType)
		}
	}

	wg.Wait()

	fmt.Printf("\nLoadtest complete:\n")
	fmt.Printf("Sent:           %d\n", sentPackets)
	fmt.Printf("Received:       %d\n", recvPackets)
	fmt.Printf("Errors:         %d\n", errors)
	fmt.Printf("Reconnects:     %d\n", reconnects)
	fmt.Printf("Slow consumers: %d\n", slowConsumers)
}

func generateMAC() [6]byte {
	var mac [6]byte
	for i := range 6 {
		mac[i] = byte(rand.Uint32())
	}
	return mac
}

// randomDuration returns a random duration uniformly picked from [lo, hi].
// If hi <= lo, lo is returned.
func randomDuration(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rand.Int64N(int64(hi-lo)+1))
}

// sleepCtx sleeps for d, returning early with false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// closeOnDone closes conn as soon as ctx is done, so any blocking reads/writes
// on conn get interrupted. This is what actually terminates a "session" when
// simulating a client disconnecting.
func closeOnDone(ctx context.Context, conn net.Conn) {
	<-ctx.Done()
	conn.Close()
}

// runPDPClient drives a PDP client for the whole duration of the loadtest.
// When churn is enabled, it repeatedly connects, plays for a random session
// length, disconnects and idles for a while before reconnecting again, to
// simulate real players joining and leaving instead of a single connection
// living for the whole test.
func runPDPClient(ctx context.Context, wg *sync.WaitGroup, cfg loadtestConfig, myMAC [6]byte, myPort uint16, targetMAC [6]byte, targetPort uint16, sent, recv, errs, reconnects, slowConsumers *int64, mu *sync.Mutex) {
	defer wg.Done()

	isSlow := rand.Float64() < cfg.slowConsumerChance
	if isSlow {
		increment(slowConsumers, mu)
		log.Debug().Msgf("PDP client %x is a slow consumer (+%v per read)", myMAC, cfg.slowConsumerDelay)
	}

	for first := true; ; first = false {
		if ctx.Err() != nil {
			return
		}

		if !first {
			increment(reconnects, mu)
			log.Debug().Msgf("PDP client %x reconnecting", myMAC)
		}

		sessionCtx := ctx
		var cancelSession context.CancelFunc
		if cfg.churn {
			sessionCtx, cancelSession = context.WithTimeout(ctx, randomDuration(cfg.minSession, cfg.maxSession))
		}

		runPDPSession(sessionCtx, cfg.addr, myMAC, myPort, targetMAC, targetPort, cfg.packetSize, isSlow, cfg.slowConsumerDelay, sent, recv, errs, mu)

		if cancelSession != nil {
			cancelSession()
		}

		if !cfg.churn || ctx.Err() != nil {
			return
		}

		log.Debug().Msgf("PDP client %x disconnected, idling before reconnect", myMAC)

		if !sleepCtx(ctx, randomDuration(cfg.minIdle, cfg.maxIdle)) {
			return
		}
	}
}

// runPDPSession runs a single connect-play-disconnect cycle for a PDP client.
// It returns once ctx is done (either the session ended or the whole test did)
// or a connection error occurs.
func runPDPSession(ctx context.Context, addr string, myMAC [6]byte, myPort uint16, targetMAC [6]byte, targetPort uint16, payloadSize int, slow bool, slowDelay time.Duration, sent, recv, errs *int64, mu *sync.Mutex) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		increment(errs, mu)
		return
	}
	defer conn.Close()
	go closeOnDone(ctx, conn)

	// Handshake
	initBuf := make([]byte, initPacketSize)
	binary.LittleEndian.PutUint32(initBuf[0:4], uint32(initTypePdp))
	copy(initBuf[4:10], myMAC[:])
	binary.LittleEndian.PutUint16(initBuf[12:14], myPort)
	if _, err := conn.Write(initBuf); err != nil {
		increment(errs, mu)
		return
	}

	// Reader. A slow consumer waits an extra delay before every read, so it
	// consumes data much slower than it arrives, simulating a struggling
	// client and building up backpressure on the server side.
	go func() {
		buf := make([]byte, pdpHeaderSize+payloadSize)
		for {
			if slow && !sleepCtx(ctx, slowDelay) {
				return
			}
			if _, err := io.ReadFull(conn, buf); err != nil {
				return
			}
			increment(recv, mu)
		}
	}()

	// Writer
	packet := make([]byte, pdpHeaderSize+payloadSize)
	copy(packet[0:6], targetMAC[:])
	binary.LittleEndian.PutUint16(packet[8:10], targetPort)
	binary.LittleEndian.PutUint32(packet[10:14], uint32(payloadSize))

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := conn.Write(packet); err != nil {
				increment(errs, mu)
				return
			}
			increment(sent, mu)
		}
	}
}

// runP2PPeer drives a P2P peer for the whole duration of the loadtest. When
// churn is enabled, it repeatedly performs the full P2P handshake, plays for a
// random session length, disconnects and idles for a while before
// reconnecting again, to simulate real players joining and leaving.
func runP2PPeer(ctx context.Context, wg *sync.WaitGroup, cfg loadtestConfig, myMAC [6]byte, myPort uint16, targetMAC [6]byte, targetPort uint16, isInitiator bool, sent, recv, errs, reconnects, slowConsumers *int64, mu *sync.Mutex) {
	defer wg.Done()

	isSlow := rand.Float64() < cfg.slowConsumerChance
	if isSlow {
		increment(slowConsumers, mu)
		log.Debug().Msgf("P2P peer %x is a slow consumer (+%v per read)", myMAC, cfg.slowConsumerDelay)
	}

	for first := true; ; first = false {
		if ctx.Err() != nil {
			return
		}

		if !first {
			increment(reconnects, mu)
			log.Debug().Msgf("P2P peer %x reconnecting", myMAC)
		}

		sessionCtx := ctx
		var cancelSession context.CancelFunc
		if cfg.churn {
			sessionCtx, cancelSession = context.WithTimeout(ctx, randomDuration(cfg.minSession, cfg.maxSession))
		}

		runP2PSession(sessionCtx, cfg.addr, myMAC, myPort, targetMAC, targetPort, isInitiator, cfg.packetSize, isSlow, cfg.slowConsumerDelay, sent, recv, errs, mu)

		if cancelSession != nil {
			cancelSession()
		}

		if !cfg.churn || ctx.Err() != nil {
			return
		}

		log.Debug().Msgf("P2P peer %x disconnected, idling before reconnect", myMAC)

		if !sleepCtx(ctx, randomDuration(cfg.minIdle, cfg.maxIdle)) {
			return
		}
	}
}

// runP2PSession runs a single connect-play-disconnect cycle for a P2P peer.
// It returns once ctx is done (either the session ended or the whole test did)
// or a connection/handshake error occurs.
func runP2PSession(ctx context.Context, addr string, myMAC [6]byte, myPort uint16, targetMAC [6]byte, targetPort uint16, isInitiator bool, payloadSize int, slow bool, slowDelay time.Duration, sent, recv, errs *int64, mu *sync.Mutex) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		increment(errs, mu)
		return
	}
	defer conn.Close()
	go closeOnDone(ctx, conn)

	if isInitiator {
		// P2P Connect
		initBuf := make([]byte, initPacketSize)
		binary.LittleEndian.PutUint32(initBuf[0:4], uint32(initTypeP2pConnect))
		copy(initBuf[4:10], myMAC[:])
		binary.LittleEndian.PutUint16(initBuf[12:14], myPort)
		copy(initBuf[14:20], targetMAC[:])
		binary.LittleEndian.PutUint16(initBuf[22:24], targetPort)

		// Wait a bit for the listener to be ready
		if !sleepCtx(ctx, 200*time.Millisecond) {
			return
		}

		if _, err := conn.Write(initBuf); err != nil {
			increment(errs, mu)
			return
		}
		log.Debug().Msg("Initiator: P2P Connect sent")

		// Wait for acceptance notification
		acceptNotif := make([]byte, 10)
		if _, err := io.ReadFull(conn, acceptNotif); err != nil {
			increment(errs, mu)
			return
		}
		log.Debug().Msg("Initiator: P2P Acceptance received")
	} else {
		// P2P Listen
		initBuf := make([]byte, initPacketSize)
		binary.LittleEndian.PutUint32(initBuf[0:4], uint32(initTypeP2pListen))
		copy(initBuf[4:10], myMAC[:])
		binary.LittleEndian.PutUint16(initBuf[12:14], myPort)
		if _, err := conn.Write(initBuf); err != nil {
			increment(errs, mu)
			return
		}
		log.Debug().Msg("Listener: P2P Listen sent")

		// Wait for notification
		notif := make([]byte, 10)
		if _, err := io.ReadFull(conn, notif); err != nil {
			increment(errs, mu)
			return
		}
		log.Debug().Msg("Listener: P2P Notification received")
		// Close listen connection and open Accept connection
		conn.Close()

		conn, err = net.Dial("tcp", addr)
		if err != nil {
			increment(errs, mu)
			return
		}
		defer conn.Close()
		go closeOnDone(ctx, conn)

		binary.LittleEndian.PutUint32(initBuf[0:4], uint32(initTypeP2pAccept))
		copy(initBuf[4:10], myMAC[:])
		binary.LittleEndian.PutUint16(initBuf[12:14], myPort)
		copy(initBuf[14:20], targetMAC[:])
		binary.LittleEndian.PutUint16(initBuf[22:24], targetPort)
		if _, err := conn.Write(initBuf); err != nil {
			increment(errs, mu)
			return
		}
		log.Debug().Msg("Listener: P2P Accept sent")

		// Wait for acceptance notification
		acceptNotif := make([]byte, 10)
		if _, err := io.ReadFull(conn, acceptNotif); err != nil {
			increment(errs, mu)
			return
		}
		log.Debug().Msg("Listener: P2P Acceptance received")
	}

	// Data exchange. A slow consumer waits an extra delay before every read,
	// so it consumes data much slower than it arrives, simulating a
	// struggling client and building up backpressure on the server side.
	go func() {
		header := make([]byte, p2pHeaderSize)
		for {
			if slow && !sleepCtx(ctx, slowDelay) {
				return
			}
			if _, err := io.ReadFull(conn, header); err != nil {
				return
			}
			size := binary.LittleEndian.Uint32(header)
			payload := make([]byte, size)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
			increment(recv, mu)
		}
	}()

	packet := make([]byte, p2pHeaderSize+payloadSize)
	binary.LittleEndian.PutUint32(packet[0:4], uint32(payloadSize))

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := conn.Write(packet); err != nil {
				increment(errs, mu)
				return
			}
			increment(sent, mu)
		}
	}
}

func increment(v *int64, mu *sync.Mutex) {
	mu.Lock()
	*v++
	mu.Unlock()
}
