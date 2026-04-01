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

// This loadtest is 100% vibecoded
func main() {
	addr := flag.String("addr", "localhost:27313", "relay server address")
	connections := flag.Int("conns", 10, "number of connections per side (total 2x conns)")
	duration := flag.Duration("duration", 30*time.Second, "loadtest duration")
	testType := flag.String("type", "pdp", "test type (pdp or p2p)")
	packetSize := flag.Int("size", 1024, "payload size")
	verbose := flag.Bool("v", false, "verbose output")
	flag.Parse()

	if *verbose {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05", NoColor: true})

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	fmt.Printf("Starting %s loadtest: %d pairs, %d bytes/packet, duration %v\n", *testType, *connections, *packetSize, *duration)

	var wg sync.WaitGroup
	var sentPackets, recvPackets, errors int64
	var mu sync.Mutex

	for i := range *connections {
		wg.Add(2)
		client1MAC := generateMAC()
		client2MAC := generateMAC()
		client1Port := uint16(10000 + i)
		client2Port := uint16(20000 + i)

		if *testType == "pdp" {
			go runPDPClient(ctx, &wg, *addr, client1MAC, client1Port, client2MAC, client2Port, *packetSize, &sentPackets, &recvPackets, &errors, &mu)
			go runPDPClient(ctx, &wg, *addr, client2MAC, client2Port, client1MAC, client1Port, *packetSize, &sentPackets, &recvPackets, &errors, &mu)
		} else if *testType == "p2p" {
			go runP2PPeer(ctx, &wg, *addr, client1MAC, client1Port, client2MAC, client2Port, true, *packetSize, &sentPackets, &recvPackets, &errors, &mu)
			go runP2PPeer(ctx, &wg, *addr, client2MAC, client2Port, client1MAC, client1Port, false, *packetSize, &sentPackets, &recvPackets, &errors, &mu)
		} else {
			log.Fatal().Msgf("unknown test type: %s", *testType)
		}
	}

	wg.Wait()

	fmt.Printf("\nLoadtest complete:\n")
	fmt.Printf("Sent:     %d\n", sentPackets)
	fmt.Printf("Received: %d\n", recvPackets)
	fmt.Printf("Errors:   %d\n", errors)
}

func generateMAC() [6]byte {
	var mac [6]byte
	for i := range 6 {
		mac[i] = byte(rand.Uint32())
	}
	return mac
}

func runPDPClient(ctx context.Context, wg *sync.WaitGroup, addr string, myMAC [6]byte, myPort uint16, targetMAC [6]byte, targetPort uint16, payloadSize int, sent, recv, errs *int64, mu *sync.Mutex) {
	defer wg.Done()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		increment(errs, mu)
		return
	}
	defer conn.Close()

	// Handshake
	initBuf := make([]byte, initPacketSize)
	binary.LittleEndian.PutUint32(initBuf[0:4], uint32(initTypePdp))
	copy(initBuf[4:10], myMAC[:])
	binary.LittleEndian.PutUint16(initBuf[12:14], myPort)
	if _, err := conn.Write(initBuf); err != nil {
		increment(errs, mu)
		return
	}

	// Reader
	go func() {
		buf := make([]byte, pdpHeaderSize+payloadSize)
		for {
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

func runP2PPeer(ctx context.Context, wg *sync.WaitGroup, addr string, myMAC [6]byte, myPort uint16, targetMAC [6]byte, targetPort uint16, isInitiator bool, payloadSize int, sent, recv, errs *int64, mu *sync.Mutex) {
	defer wg.Done()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		increment(errs, mu)
		return
	}
	defer conn.Close()

	if isInitiator {
		// P2P Connect
		initBuf := make([]byte, initPacketSize)
		binary.LittleEndian.PutUint32(initBuf[0:4], uint32(initTypeP2pConnect))
		copy(initBuf[4:10], myMAC[:])
		binary.LittleEndian.PutUint16(initBuf[12:14], myPort)
		copy(initBuf[14:20], targetMAC[:])
		binary.LittleEndian.PutUint16(initBuf[22:24], targetPort)

		// Wait a bit for the listener to be ready
		time.Sleep(200 * time.Millisecond)

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

	// Data exchange
	go func() {
		header := make([]byte, p2pHeaderSize)
		for {
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
