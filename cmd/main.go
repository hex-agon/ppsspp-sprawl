package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sprawl-relay/internal/relay"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	addr := flag.String("addr", "0.0.0.0", "address to listen on")
	port := flag.String("port", "27313", "port to listen on")
	metricsPort := flag.String("metrics-port", "8081", "port to listen on for metrics")
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"})

	var rel relay.Server
	var err error

	go func() {
		if rel, err = relay.NewRelay(*addr, *port); err != nil {
			log.Fatal().Err(err).Msg("error while starting relay server")
		}
	}()
	log.Info().Str("addr", *addr).Str("port", *port).Msg("started relay server successfully")

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    "0.0.0.0:" + *metricsPort,
		Handler: mux,
	}

	go func() {
		log.Info().Str("port", *metricsPort).Msg("starting metrics server")
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("error while starting metrics server")
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Info().Msg("shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("error while shutting down metrics server")
	}

	if err := rel.Shutdown(); err != nil {
		log.Warn().Err(err).Msg("error while shutting relay gracefully")
	}
	log.Info().Msg("shutdown complete")
}
