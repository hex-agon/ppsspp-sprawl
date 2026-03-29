package main

import (
	"os"
	"os/signal"
	"sprawl-relay/internal/relay"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"})

	var rel relay.Server
	var err error

	go func() {
		if rel, err = relay.NewRelay("", "27313"); err != nil {
			log.Fatal().Err(err).Msg("error while starting relay server")
		}
	}()
	log.Info().Msg("started relay server successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Info().Msg("shutting down gracefully...")

	if err := rel.Shutdown(); err != nil {
		log.Warn().Err(err).Msg("error while shutting relay gracefully")
	}
	log.Info().Msg("shutdown complete")
}
