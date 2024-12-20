package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/bredtape/gateway"
	"github.com/bredtape/gateway/sync"
	"github.com/bredtape/retry"
	"github.com/bredtape/slogging"
	"github.com/nats-io/nats.go"
	"github.com/peterbourgon/ff/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/context"
	"gopkg.in/yaml.v3"
)

// app name, used for environment and metrics prefix
const app = "sync"

type Config struct {
	NatsURLs string

	// optional nats .nk seed file for auth
	NatsSeedFile string

	// http address to serve metrics
	HTTPAddress string

	LogLevel slog.Level
	LogJSON  bool

	SyncConfigFile string
}

func readArgs() Config {
	envPrefix := strings.ToUpper(app)
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.Usage = func() {
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "Options may also be set from the environment. Prefix with %s_, use all caps. and replace any - with _\n", envPrefix)
		os.Exit(1)
	}

	var cfg Config
	fs.StringVar(&cfg.NatsURLs, "nats-urls", "", "Nats urls separated with ,. Required")
	fs.StringVar(&cfg.NatsSeedFile, "nats-seed-file", "", "Optional nats .nk seed file auth")
	fs.StringVar(&cfg.HTTPAddress, "http-address", ":8900", "HTTP address to serve metrics etc.")
	fs.StringVar(&cfg.SyncConfigFile, "config-file", "config.yml", "Config file in YAML format")

	var logLevel slog.Level
	fs.TextVar(&logLevel, "log-level", slog.LevelDebug-3, "Log level")
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

	if cfg.NatsURLs == "" {
		bail(fs, "'nats-urls' not specified")
	}

	if fileNotExists(cfg.SyncConfigFile) {
		bail(fs, "'config-file' does not exist (%s)", cfg.SyncConfigFile)
	}

	slogging.SetDefaults(slog.HandlerOptions{Level: logLevel}, logJSON)
	slogging.LogBuildInfo()

	return cfg
}

func main() {
	cfg := readArgs()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.With("module", "sync/cmd")

	mux := http.NewServeMux()
	// http exhanges are served under path "httpExhange/<from>/in"
	syncConf, err := loadNatsSyncConfigFromFile(mux, cfg.SyncConfigFile)
	if err != nil {
		slogging.Fatal(log, "failed to load/parse/validate sync config", "err", err)
	}

	jsConf := sync.JSConfig{URLs: cfg.NatsURLs, Options: []nats.Option{nats.MaxReconnects(-1)}}
	if cfg.NatsSeedFile != "" {
		err = jsConf.WithSeedFile(cfg.NatsSeedFile)
		if err != nil {
			slogging.Fatal(log, "failed to set seed file", "err", err)
		}
	}
	js := sync.NewJSConn(jsConf)
	err = sync.StartNatsSync(ctx, js, syncConf)
	if err != nil {
		slogging.Fatal(log, "failed to start nats sync", "err", err)
	}

	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/heap", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	// catch all (must be last)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Debug("http request at unmatched path", "method", r.Method, "url", r.URL)
	})

	log.Info("starting http server", "address", cfg.HTTPAddress)
	err = http.ListenAndServe(cfg.HTTPAddress, mux)
	if err != nil {
		slogging.Fatal(log, "failed to start http server", "err", err)
	}
}

func loadNatsSyncConfigFromFile(mux *http.ServeMux, filename string) (sync.NatsSyncConfig, error) {
	f, err := os.Open(filename)
	if err != nil {
		return sync.NatsSyncConfig{}, errors.Wrap(err, "failed to open config file")
	}
	defer f.Close()
	var cfg NatsSyncConfigSerialize
	err = yaml.NewDecoder(f).Decode(&cfg)
	if err != nil {
		return sync.NatsSyncConfig{}, errors.Wrap(err, "failed to load config from file")
	}

	return cfg.ToNatsSyncConfig(mux)
}

func bail(fs *flag.FlagSet, format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	fs.Usage()
}

type NatsSyncConfigSerialize struct {
	Deployment gateway.Deployment `yaml:"deployment"`
	// indicates if the 'sync' stream originates from this deployment. Exactly one deployment must have this set to true, either this or in the DeploymentSettings
	IsSyncSource       bool                                     `yaml:"isSyncSource"`
	SyncStream         string                                   `yaml:"syncStream"`
	DefaultSettings    DeploymentSetting                        `yaml:"defaultSettings"`
	DeploymentSettings map[gateway.Deployment]DeploymentSetting `yaml:"deploymentSettings"`
}

// convert to NatsSyncConfig and validate
func (c NatsSyncConfigSerialize) ToNatsSyncConfig(mux *http.ServeMux) (sync.NatsSyncConfig, error) {
	result := sync.NatsSyncConfig{
		Deployment:            c.Deployment,
		IsSyncSource:          c.IsSyncSource,
		SyncStream:            c.SyncStream,
		CommunicationSettings: make(map[gateway.Deployment]sync.CommunicationSettings),
		Exchanges:             make(map[gateway.Deployment]sync.Exchange)}

	for d, v := range c.DeploymentSettings {
		v.WithDefault(c.DefaultSettings)
		cs, err := v.ToCommunicationSettings()
		if err != nil {
			return result, errors.Wrapf(err, "failed to convert DeploymentSetting for %s", d)
		}
		result.CommunicationSettings[d] = cs

		if v.FileExchangeEnabled {
			ex, err := sync.NewFileExchange(v.FileExchangeConfig)
			if err != nil {
				return result, err
			}
			result.Exchanges[d] = ex
		}

		if v.HttpExchangeEnabled {
			v.HttpExchangeConfig.From = c.Deployment
			v.HttpExchangeConfig.To = d
			ex, err := sync.NewHTTPExchange(v.HttpExchangeConfig)
			if err != nil {
				return result, err
			}

			log := slog.With("module", "sync/cmd", "op", "ToNatsSyncConfig",
				"from", c.Deployment, "to", d, "clientURL", v.HttpExchangeConfig.ClientURL)
			expectedSuffix := fmt.Sprintf("/httpExchange/%s/in", c.Deployment.String())
			if !strings.HasSuffix(v.HttpExchangeConfig.ClientURL, expectedSuffix) {
				log.Warn("unexpected clientURL suffix", "expected.suffix", expectedSuffix)
			}

			path := fmt.Sprintf("/httpExchange/%s/in", d)
			mux.Handle(path, ex)
			result.Exchanges[d] = ex

			log.Debug("registered HTTPExchange", "path", path)
		}
	}

	if ve := result.Validate(); ve != nil {
		return result, ve
	}
	return result, nil
}

type DeploymentSetting struct {
	// indicates if the 'sync' stream originates from this deployment. Exactly one deployment must have this set to true
	IsSyncSource                                         bool                    `yaml:"isSyncSource"`
	AckTimeoutPrSubscription                             time.Duration           `yaml:"ackTimeout"`
	AckRetryPrSubscriptionJitter                         float64                 `yaml:"ackRetryJitter"`
	AckRetryPrSubscriptionStep                           time.Duration           `yaml:"ackRetryStep"`
	AckRetryPrSubscriptionMax                            time.Duration           `yaml:"ackRetryMax"`
	HeartbeatIntervalPrSubscription                      time.Duration           `yaml:"heartbeatInterval"`
	PendingAcksPrSubscriptionMax                         int                     `yaml:"pendingAcksMax"`
	PendingIncomingMessagesPrSubscriptionMaxBuffered     int                     `yaml:"pendingIncomingMessagesMaxBuffered"`
	PendingIncomingMessagesPrSubscriptionDeleteThreshold int                     `yaml:"pendingIncomingMessagesDeleteThreshold"`
	MaxAccumulatedPayloadSizeBytes                       int                     `yaml:"maxAccumulatedPayloadSize"`
	NatsOperationTimeout                                 time.Duration           `yaml:"natsOperationTimeout"`
	BatchFlushTimeout                                    time.Duration           `yaml:"batchFlushTimeout"`
	ExchangeOperationTimeout                             time.Duration           `yaml:"exchangeOperationTimeout"`
	FileExchangeEnabled                                  bool                    `yaml:"fileExchangeEnabled"`
	FileExchangeConfig                                   sync.FileExchangeConfig `yaml:"fileExchangeConfig"`
	HttpExchangeEnabled                                  bool                    `yaml:"httpExchangeEnabled"`
	HttpExchangeConfig                                   sync.HTTPExchangeConfig `yaml:"httpExchangeConfig"`
}

func (d DeploymentSetting) ToCommunicationSettings() (sync.CommunicationSettings, error) {
	ackRetry, err := retry.NewExp(d.AckRetryPrSubscriptionJitter, d.AckRetryPrSubscriptionStep, d.AckRetryPrSubscriptionMax)
	if err != nil {
		return sync.CommunicationSettings{}, errors.Wrap(err, "failed to create AckRetryPrSubscription")
	}
	result := sync.CommunicationSettings{
		IsSyncSource:                                         d.IsSyncSource,
		AckTimeoutPrSubscription:                             d.AckTimeoutPrSubscription,
		AckRetryPrSubscription:                               ackRetry,
		HeartbeatIntervalPrSubscription:                      d.HeartbeatIntervalPrSubscription,
		PendingAcksPrSubscriptionMax:                         d.PendingAcksPrSubscriptionMax,
		PendingIncomingMessagesPrSubscriptionMaxBuffered:     d.PendingIncomingMessagesPrSubscriptionMaxBuffered,
		PendingIncomingMessagesPrSubscriptionDeleteThreshold: d.PendingIncomingMessagesPrSubscriptionDeleteThreshold,
		MaxAccumulatedPayloadSizeBytes:                       d.MaxAccumulatedPayloadSizeBytes,
		NatsOperationTimeout:                                 d.NatsOperationTimeout,
		BatchFlushTimeout:                                    d.BatchFlushTimeout,
		ExchangeOperationTimeout:                             d.ExchangeOperationTimeout,
	}

	return result, nil
}

// apply default settings when unspecified/default value
func (d *DeploymentSetting) WithDefault(def DeploymentSetting) {
	if d.AckTimeoutPrSubscription == 0 {
		d.AckTimeoutPrSubscription = def.AckTimeoutPrSubscription
	}
	if d.AckRetryPrSubscriptionJitter == 0 {
		d.AckRetryPrSubscriptionJitter = def.AckRetryPrSubscriptionJitter
	}
	if d.AckRetryPrSubscriptionStep == 0 {
		d.AckRetryPrSubscriptionStep = def.AckRetryPrSubscriptionStep
	}
	if d.AckRetryPrSubscriptionMax == 0 {
		d.AckRetryPrSubscriptionMax = def.AckRetryPrSubscriptionMax
	}
	if d.HeartbeatIntervalPrSubscription == 0 {
		d.HeartbeatIntervalPrSubscription = def.HeartbeatIntervalPrSubscription
	}
	if d.PendingAcksPrSubscriptionMax == 0 {
		d.PendingAcksPrSubscriptionMax = def.PendingAcksPrSubscriptionMax
	}
	if d.PendingIncomingMessagesPrSubscriptionMaxBuffered == 0 {
		d.PendingIncomingMessagesPrSubscriptionMaxBuffered = def.PendingIncomingMessagesPrSubscriptionMaxBuffered
	}
	if d.PendingIncomingMessagesPrSubscriptionDeleteThreshold == 0 {
		d.PendingIncomingMessagesPrSubscriptionDeleteThreshold = def.PendingIncomingMessagesPrSubscriptionDeleteThreshold
	}
	if d.MaxAccumulatedPayloadSizeBytes == 0 {
		d.MaxAccumulatedPayloadSizeBytes = def.MaxAccumulatedPayloadSizeBytes
	}
	if d.NatsOperationTimeout == 0 {
		d.NatsOperationTimeout = def.NatsOperationTimeout
	}
	if d.BatchFlushTimeout == 0 {
		d.BatchFlushTimeout = def.BatchFlushTimeout
	}
	if d.ExchangeOperationTimeout == 0 {
		d.ExchangeOperationTimeout = def.ExchangeOperationTimeout
	}
	if !d.FileExchangeEnabled {
		d.FileExchangeEnabled = def.FileExchangeEnabled
	}
	d.FileExchangeConfig.WithDefault(def.FileExchangeConfig)
}

func fileNotExists(filename string) bool {
	_, err := os.Stat(filename)
	return os.IsNotExist(err)
}
