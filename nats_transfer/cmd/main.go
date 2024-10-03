package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// app name, used for environment and metrics prefix
const app = "nats_transfer"

type Config struct {
	NatsURLs string

	// optional nats .nk seed file for auth
	NatsSeedFile string

	// http address to serve metrics
	HTTPAddress string

	LogLevel slog.Level
	LogJSON  bool
}

var defaultConfig = Config{
	HTTPAddress: ":8300",
}

func readArgs() (Config, error) {
	envPrefix := strings.ToUpper(app)
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.Usage = func() {
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "Options may also be set from the environment. Prefix with %s_, use all caps. and replace any - with _\n", envPrefix)
		os.Exit(1)
	}

	cfg := defaultConfig
	fs.StringVar(&cfg.NatsURLs, "nats-urls", cfg.NatsURLs, "Nats urls separated with ,. Required")
	fs.StringVar(&cfg.NatsSeedFile, "nats-seed-file", cfg.NatsSeedFile, "Optional nats .nk seed file auth")
	fs.StringVar(&cfg.HTTPAddress, "http-address", cfg.HTTPAddress, "HTTP address to serve metrics etc.")

	return cfg, nil
}

func main() {

}
