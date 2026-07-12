package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sprawl/internal/adhocctl"
	"sprawl/internal/debug"
	"sprawl/internal/sprawl"
	"sprawl/internal/telemetry"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	env := flag.String("env", "dev", "the environment to use")
	ctlAddr := flag.String("ctl-addr", "0.0.0.0", "address for the ctl server to listen on")
	ctlPort := flag.String("ctl-port", "27312", "port for the ctl server to listen on")
	relayAddr := flag.String("relay-addr", "0.0.0.0", "address for the relay server to listen on")
	relayPort := flag.String("relay-port", "27313", "port for the relay server to listen on")
	metricsPort := flag.String("metrics-port", "8081", "port to listen on for metrics")
	nodeName := flag.String("node-name", "local", "node name")
	amqpURL := flag.String("amqp-url", "", "amqp url for telemetry")
	logLevel := flag.String("log-level", "info", "log level")
	flag.Parse()

	var amqpCollector *telemetry.AmqpCollector
	var defaultLogLevel = zerolog.DebugLevel

	if *env == "prod" {
		if *nodeName == "local" {
			log.Fatal().Msg("node name must not be local in prov env")
		}
		defaultLogLevel = zerolog.InfoLevel
	} else {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"})
	}
	switch *logLevel {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	default:
		zerolog.SetGlobalLevel(defaultLogLevel)
	}

	if *amqpURL != "" {
		amqpCollector = telemetry.NewAmqpCollector(*amqpURL, *nodeName)
		telemetry.Init(amqpCollector)
	}
	var ctlServer adhocctl.Server
	var relayServer sprawl.Server

	var initWg sync.WaitGroup
	initWg.Add(2)

	go func() {
		defer initWg.Done()
		var err error
		if ctlServer, err = adhocctl.NewCtlServer(*ctlAddr, *ctlPort); err != nil {
			log.Fatal().Err(err).Msg("error while starting ctl server")
		}
		log.Info().Str("addr", *ctlAddr).Str("port", *ctlPort).Msg("started ctl server successfully")
	}()

	go func() {
		defer initWg.Done()
		var err error
		if relayServer, err = sprawl.NewRelay(*relayAddr, *relayPort); err != nil {
			log.Fatal().Err(err).Msg("error while starting relay server")
		}
		log.Info().Str("addr", *relayAddr).Str("port", *relayPort).Msg("started relay server successfully")
	}()

	initWg.Wait()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("GET /status", adhocctl.StatusHandler(ctlServer))
	mux.HandleFunc("GET /debug", debug.DebugHandler(ctlServer, relayServer.GetExchange(), relayServer.GetHub()))
	metricsServer := &http.Server{
		Addr:    "0.0.0.0:" + *metricsPort,
		Handler: mux,
	}

	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("error while starting metrics server")
		}
	}()
	log.Info().Str("port", *metricsPort).Msg("started metrics server")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Info().Msg("shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Warn().Err(err).Msg("error while shutting down metrics server")
	}

	if err := ctlServer.Shutdown(); err != nil {
		log.Warn().Err(err).Msg("error while shutting ctl server gracefully")
	}
	if err := relayServer.Shutdown(); err != nil {
		log.Warn().Err(err).Msg("error while shutting down relay server gracefully")
	}

	if amqpCollector != nil {
		amqpCollector.Close()
	}
	log.Info().Msg("shutdown complete")
}
