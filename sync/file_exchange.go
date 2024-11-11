package sync

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type FileExchangeConfig struct {
	IncomingDir          string        `yaml:"incomingDir"`
	OutgoingDir          string        `yaml:"outgoingDir"`
	PollingStartDelay    time.Duration `yaml:"pollingStartDelay"`
	PollingInterval      time.Duration `yaml:"pollingInterval"`
	PollingRetryInterval time.Duration `yaml:"pollingRetryInterval"`
}

func (c FileExchangeConfig) Validate() error {
	if c.IncomingDir == "" {
		return errors.New("incomingDir empty")
	}
	if c.OutgoingDir == "" {
		return errors.New("outgoingDir empty")
	}
	if path.Clean(c.IncomingDir) == path.Clean(c.OutgoingDir) {
		return errors.New("incoming and outgoing dir must not be the same")
	}
	if c.PollingInterval < time.Millisecond {
		return errors.New("pollingInterval must be at least 1 ms")
	}
	if c.PollingRetryInterval < time.Millisecond {
		return errors.New("pollingRetryInterval must be at least 1 ms")
	}
	return nil
}

func (c *FileExchangeConfig) WithDefault(d FileExchangeConfig) {
	if c.IncomingDir == "" {
		c.IncomingDir = d.IncomingDir
	}
	if c.OutgoingDir == "" {
		c.OutgoingDir = d.OutgoingDir
	}
	if c.PollingStartDelay == 0 {
		c.PollingStartDelay = d.PollingStartDelay
	}
	if c.PollingInterval == 0 {
		c.PollingInterval = d.PollingInterval
	}
	if c.PollingRetryInterval == 0 {
		c.PollingRetryInterval = d.PollingRetryInterval
	}
}

var (
	ErrDirectoryDoesNotExists = errors.New("directory does not exists")
)
var _ Exchange = (*FileExchange)(nil)

const channelCapacity = 10
const writeFileMode fs.FileMode = 0644

// FileExchange is a simple file based message exchange
// does not handle any acks or retries
// implements Exchange interface
type FileExchange struct {
	inDir, outDir string
	// monotonic increasing counter used when writing files. Wraps after 9 digits (base 10).
	// points to the last number used (so increment before use)
	counter uint32

	// polling options
	startDelay, interval, retry time.Duration
}

// start file io to watch incoming and write to outgoing directory.
// The directory must exist
// Files starting with . will be ignored.
// Files should be written with a . prefix, then renamed. Alternatively written to another directory, then renamed
// Returns ErrDirectoryDoesNotExists if the directories does not exists
// TODO: Also check that the dirs are read/write-able
func NewFileExchange(c FileExchangeConfig) (*FileExchange, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	ex := &FileExchange{
		inDir:      path.Clean(c.IncomingDir),
		outDir:     path.Clean(c.OutgoingDir),
		startDelay: c.PollingStartDelay,
		interval:   c.PollingInterval,
		retry:      c.PollingRetryInterval}

	return ex, ex.assertAllDirExists()
}

// watch for messages in the incoming directory.
// The files will be deleted after the content has been read.
// Do not assume any order
func (ex *FileExchange) StartReceiving(ctx context.Context) (<-chan *v1.MessageBatch, error) {
	log := slog.With("module", "sync", "operation", "FileExchange/StartReceiving", "incomingDirectory", ex.inDir)

	// Create new watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to start fsnotify watcher")
	}
	resultCh := make(chan *v1.MessageBatch, channelCapacity)

	pollingResult := startPollDirectory(ctx, ex.startDelay, ex.interval, ex.retry, ex.inDir)

	// Start listening for events.
	go func() {
		log.Debug("start watching")
		defer log.Debug("stop watching")
		defer watcher.Close()
		defer close(resultCh)

		for {
			select {
			case <-ctx.Done():
				return
			case filename, ok := <-pollingResult:
				if !ok {
					return
				}

				log2 := log.With("source", "polling", "basename", path.Base(filename))
				log2.Log(ctx, slog.LevelDebug-3, "received filename")
				msg, err := ex.consumeFile(filename)
				if err != nil {
					log2.Error("failed to unmarshal", "err", err)
					continue
				}

				if msg == nil {
					log2.Log(ctx, slog.LevelDebug-3, "no message to relay (check sum failed or not relevant)")
					continue
				}

				select {
				case <-ctx.Done():
					return
				case resultCh <- msg:
					log2.Log(ctx, slog.LevelDebug-3, "relayed message exchange")
				}

			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log2 := log.With("source", "polling", "basename", path.Base(event.Name), "op", event.Op.String())
				log2.Log(ctx, slog.LevelDebug-6, "received event")

				if event.Op.Has(fsnotify.Create) {
					msg, err := ex.consumeFile(event.Name)
					if err != nil {
						log2.Error("failed to unmarshal", "err", err, "filename", event.Name, "op", event.Op.String())
						continue
					}

					if msg == nil {
						log2.Log(ctx, slog.LevelDebug-3, "no message to relay (check sum failed or not relevant)")
						continue
					}

					select {
					case <-ctx.Done():
						return
					case resultCh <- msg:
						log2.Log(ctx, slog.LevelDebug-3, "relayed message exchange", "basename", path.Base(event.Name))
					}
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Error("watch error", "err", err)
			}
		}
	}()

	// start watching incoming directory
	err = watcher.Add(ex.inDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to watch incoming directory")
	}

	return resultCh, nil
}

// write message exhange to underlying file system
// The message will be written with a . prefix first, then renamed
// File name will be <zero padded counter, 9 digits>_<hash>.me
func (ex *FileExchange) Write(_ context.Context, batch *v1.MessageBatch) error {
	log := slog.With("outDir", ex.outDir)
	data, err := proto.Marshal(batch)
	if err != nil {
		return errors.Wrap(err, "failed to marshal")
	}

	base := ex.createFilename(data)
	filename := path.Join(ex.outDir, base)
	tmpFilename := path.Join(ex.outDir, "."+base)

	log = log.With("baseFilename", path.Base(filename), "baseTmpFilename", path.Base(tmpFilename))
	log.Log(context.Background(), slog.LevelDebug-3, "start writing")
	err = os.WriteFile(tmpFilename, data, writeFileMode)
	if err != nil {
		log.Error("failed to write", "err", err)
		return errors.Wrapf(err, "failed to write to tmp file %s", tmpFilename)
	}

	log.Log(context.Background(), slog.LevelDebug-6, "rename")
	err = os.Rename(tmpFilename, filename)
	if err != nil {
		log.Error("failed to rename", "err", err)
		return errors.Wrapf(err, "failed to rename from tmp file '%s' to target '%s'", tmpFilename, base)
	}

	return nil
}

// consume message from file name (read and immediately delete).
// Verifies hash matches content
// Returns nil message if filename is not relevant
func (ex *FileExchange) consumeFile(filename string) (*v1.MessageBatch, error) {
	if !isRelevantFile(filename) {
		return nil, nil
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read file")
	}

	err = os.Remove(filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to delete file")
	}

	if len(data) == 0 {
		return nil, nil
	}

	err = checkHashAgainstFileName(hashSHA256AndBase64(data), filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to verify hash")
	}

	var msg v1.MessageBatch
	err = proto.Unmarshal(data, &msg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal message")
	}
	return &msg, nil
}

// regular expression to match base filename consisting of a counter and a sha256 of data with base64 url encoding
var reBaseMatch = regexp.MustCompile(`^([0-9]{6})_(sha256=X(?:[A-Za-z0-9-_]{4})*(?:[A-Za-z0-9-_]{2}==|[A-Za-z0-9-_]{3}=)?).me$`)

func isRelevantFile(filename string) bool {
	return strings.HasSuffix(path.Base(filename), ".me")
}

// create filename consisting of an incrementing counter and a hash of the data
func (ex *FileExchange) createFilename(data []byte) string {
	counter := ex.nextCounter()
	return fmt.Sprintf("%06d_%s.me", counter, hashSHA256AndBase64(data))
}

// hash data with sha256 and encode to base64
func hashSHA256AndBase64(data []byte) string {
	d := sha256.New()
	d.Write(data) // does not error
	return "sha256=X" + base64.URLEncoding.EncodeToString(d.Sum(nil))
}

func checkHashAgainstFileName(hash, filename string) error {
	matches := reBaseMatch.FindStringSubmatch(path.Base(filename))
	if len(matches) != 3 {
		return fmt.Errorf("filename '%s' does not match expected format", filename)
	}

	if matches[2] != hash {
		return errors.New("hash mismatch")
	}

	return nil
}

func (ex *FileExchange) nextCounter() uint32 {
	ex.counter++
	if ex.counter > 999_999 {
		ex.counter = 0
	}
	return ex.counter
}

func (ex *FileExchange) assertAllDirExists() error {
	if ex.inDir == ex.outDir {
		return errors.New("directories must not be equal")
	}

	if err := assertDirExists(ex.inDir); err != nil {
		return errors.Wrap(err, "incoming directory does not exist")
	}

	if err := assertDirExists(ex.outDir); err != nil {
		return errors.Wrap(err, "outgoing directory does not exist")
	}

	return nil
}

func assertDirExists(d string) error {
	fi, err := os.Stat(d)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrapf(ErrDirectoryDoesNotExists, "directory '%s' does not exists", d)
		}
		return errors.Wrapf(err, "could not determine file info for '%s'", d)
	}

	if !fi.IsDir() {
		return errors.Wrapf(ErrDirectoryDoesNotExists, "path '%s' is not a directory", d)
	}
	return nil
}

// poll directory
func startPollDirectory(ctx context.Context, startDelay, interval, retryInterval time.Duration, dir string) <-chan string {
	log := slog.With("operation", "startPollDirectory", "dir", dir, "interval", interval)
	ch := make(chan string, 10)

	t := time.After(startDelay)

	go func() {
		log.Debug("starting")
		defer log.Debug("stopping")
		defer close(ch)

		for {
		outerLoop:
			select {
			case <-ctx.Done():
				return
			case <-t:
				xs, err := os.ReadDir(dir)
				if err != nil {
					log.Error("failed to read dir", "err", err, "retryInterval", retryInterval)

					// reset timer
					t = time.After(retryInterval)
					continue
				}

				// reset timer
				t = time.After(interval)

				for _, x := range xs {
					if x.IsDir() {
						continue
					}
					select {
					case <-ctx.Done():
						return

					// start over to read dir
					case <-t:
						// time.After will only emit once, so we need to close and create a new channel
						t := make(chan time.Time)
						close(t)
						goto outerLoop

					case ch <- path.Join(dir, x.Name()):
					}
				}
			}
		}
	}()

	return ch
}
