package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/bredtape/gateway"
	"github.com/bredtape/gateway/nats_sync"
	"github.com/bredtape/retry"
	"github.com/bredtape/slogging"
	"github.com/peterbourgon/ff/v3"
	"golang.org/x/net/context"
)

// app name, used for environment and metrics prefix
const app = "nats_sync"

var defaultCommSettings = nats_sync.CommunicationSettings{
	AckTimeoutPrSubscription:                             5 * time.Second,
	AckRetryPrSubscription:                               retry.Must(retry.NewExp(0.4, 10*time.Second, 60*time.Second)),
	HeartbeatIntervalPrSubscription:                      time.Minute,
	PendingAcksPrSubscriptionMax:                         100,
	MaxAccumulatedPayloadSizeBytes:                       4 << 20, // 4 MB
	PendingIncomingMessagesPrSubscriptionMaxBuffered:     1e3,
	PendingIncomingMessagesPrSubscriptionDeleteThreshold: 400,
	NatsOperationTimeout:                                 5 * time.Second,
	BatchFlushTimeout:                                    time.Second,
	ExchangeOperationTimeout:                             5 * time.Second}

type Config struct {
	NatsURLs string

	// optional nats .nk seed file for auth
	NatsSeedFile string

	// http address to serve metrics
	HTTPAddress string

	LogLevel slog.Level
	LogJSON  bool

	From, To gateway.Deployment
	nats_sync.CommunicationSettings

	FileExchangeEnabled bool
	FileExchangeConfig  nats_sync.FileExchangeConfig
}

var defaultConfig = Config{
	HTTPAddress: ":8300",
}

func readArgs() Config {
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

	fs.TextVar(&cfg.From, "from", gateway.Deployment(""), "From deployment. Required")
	fs.TextVar(&cfg.To, "to", gateway.Deployment(""), "To deployment. Required")

	// --communication settings--
	fs.DurationVar(&cfg.AckTimeoutPrSubscription, "ack-timeout", defaultCommSettings.AckTimeoutPrSubscription, "Ack timeout per subscription")
	var ackRetryStep, ackRetryMax time.Duration
	fs.DurationVar(&ackRetryStep, "ack-retry-step", 10*time.Second, "Ack retry step size. Should be greater than ack-timeout")
	fs.DurationVar(&ackRetryMax, "ack-retry-max", 60*time.Second, "Ack retry max duration. Should be much greater than ack-retry-step")
	fs.DurationVar(&cfg.HeartbeatIntervalPrSubscription, "heartbeat-interval", defaultCommSettings.HeartbeatIntervalPrSubscription, "Heartbeat interval per subscription")
	fs.IntVar(&cfg.PendingAcksPrSubscriptionMax, "pending-acks-max", defaultCommSettings.PendingAcksPrSubscriptionMax, "Max pending acks per subscription")
	fs.IntVar(&cfg.PendingIncomingMessagesPrSubscriptionDeleteThreshold, "pending-incoming-messages-delete-threshold", defaultCommSettings.PendingIncomingMessagesPrSubscriptionDeleteThreshold, "Pending incoming messages delete threshold")
	fs.IntVar(&cfg.PendingIncomingMessagesPrSubscriptionMaxBuffered, "pending-incoming-messages-max-buffered", defaultCommSettings.PendingIncomingMessagesPrSubscriptionMaxBuffered, "Max pending incoming messages buffered")
	fs.IntVar(&cfg.MaxAccumulatedPayloadSizeBytes, "max-accumulated-payload-size", defaultCommSettings.MaxAccumulatedPayloadSizeBytes, "Max accumulated payload size in bytes")
	fs.DurationVar(&cfg.NatsOperationTimeout, "nats-operation-timeout", defaultCommSettings.NatsOperationTimeout, "Nats operation timeout")
	fs.DurationVar(&cfg.BatchFlushTimeout, "batch-flush-timeout", defaultCommSettings.BatchFlushTimeout, "Batch flush timeout")
	fs.DurationVar(&cfg.ExchangeOperationTimeout, "exchange-operation-timeout", defaultCommSettings.ExchangeOperationTimeout, "Exchange operation timeout")

	// --file exchange settings--
	fs.BoolVar(&cfg.FileExchangeEnabled, "file-exchange-enabled", false, "Enable file exchange")
	fs.StringVar(&cfg.FileExchangeConfig.IncomingDir, "file-incoming-dir", "", "File exchange incoming directory")
	fs.StringVar(&cfg.FileExchangeConfig.OutgoingDir, "file-outgoing-dir", "", "File exchange outgoing directory")
	fs.DurationVar(&cfg.FileExchangeConfig.PollingStartDelay, "file-polling-start-delay", 0, "File exchange polling start delay")
	fs.DurationVar(&cfg.FileExchangeConfig.PollingInterval, "file-polling-interval", 60*time.Second, "File exchange polling interval. Incoming directory will have file watch, so changes are picked up immediately")
	fs.DurationVar(&cfg.FileExchangeConfig.PollingRetryInterval, "file-polling-retry-interval", 30*time.Second, "File exchange polling retry interval")

	var logLevel slog.Level
	fs.TextVar(&logLevel, "log-level", slog.LevelDebug, "Log level")
	var logJSON bool
	fs.BoolVar(&logJSON, "log-json", false, "Log in JSON format")
	var help bool
	fs.BoolVar(&help, "help", false, "Show help")

	err := ff.Parse(fs, os.Args[1:], ff.WithEnvVarPrefix(envPrefix))
	if err != nil {
		bail(fs, "parse error: "+err.Error())
		os.Exit(2)
	}

	if help {
		fs.Usage()
		os.Exit(0)
	}

	slogging.SetDefaults(slog.HandlerOptions{Level: logLevel}, logJSON)
	slogging.LogBuildInfo()

	return cfg
}

func main() {
	cfg := readArgs()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

}

func bail(fs *flag.FlagSet, format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	fs.Usage()
}
