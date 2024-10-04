package nats_sync

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"regexp"
	"time"

	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/cespare/xxhash"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

type FileExchangeConfig struct {
	IncomingDir, OutgoingDir                                 string
	PollingStartDelay, PollingInterval, PollingRetryInterval time.Duration
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

	// to get current time
	now func() time.Time

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
func (ex *FileExchange) StartReceiving(ctx context.Context) (<-chan *v1.MessageExchange, error) {
	log := slog.With("incomingDirectory", ex.inDir)

	// Create new watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to start fsnotify watcher")
	}
	resultCh := make(chan *v1.MessageExchange, channelCapacity)

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

				log2 := log.With("source", "polling", "filename", filename)
				log2.Log(ctx, slog.LevelDebug-3, "received filename")
				msg, err := ex.consumeFile(filename)
				if err != nil {
					log2.Error("failed to unmarshal", "err", err)
				} else if msg != nil {
					select {
					case <-ctx.Done():
						return
					case resultCh <- msg:
						log2.Log(ctx, slog.LevelDebug-3, "relayed message exchange")
					}
				}

			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log2 := log.With("source", "polling", "filename", event.Name, "op", event.Op.String())
				log2.Log(ctx, slog.LevelDebug-3, "received event")

				if event.Op.Has(fsnotify.Create) {
					msg, err := ex.consumeFile(event.Name)
					if err != nil {
						log2.Error("failed to unmarshal", "err", err, "filename", event.Name, "op", event.Op.String())
					} else if msg != nil {
						select {
						case <-ctx.Done():
							return
						case resultCh <- msg:
							log2.Log(ctx, slog.LevelDebug-3, "relayed message exchange", "filename", event.Name)
						}
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

	// Add a path.
	err = watcher.Add(ex.inDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to watch incoming directory")
	}

	return resultCh, nil
}

// write message exhange to underlying file system
// The message will be written with a . prefix first, then renamed
// File name will be <zero padded counter, 9 digits>_<hash>.me
func (ex *FileExchange) Write(_ context.Context, batch *v1.MessageExchange) error {
	log := slog.With("outDir", ex.outDir)
	data, err := proto.Marshal(batch)
	if err != nil {
		return errors.Wrap(err, "failed to marshal")
	}

	base := ex.createFilename(data)
	filename := path.Join(ex.outDir, base)
	tmpFilename := path.Join(ex.outDir, "."+base)

	log = log.With("filename", filename, "tmpFilename", tmpFilename)
	log.Log(context.Background(), slog.LevelDebug-3, "start writing")
	err = os.WriteFile(tmpFilename, data, writeFileMode)
	if err != nil {
		log.Error("failed to write", "err", err)
		return errors.Wrapf(err, "failed to write to tmp file %s", tmpFilename)
	}

	log.Log(context.Background(), slog.LevelDebug-3, "rename")
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
func (ex *FileExchange) consumeFile(filename string) (*v1.MessageExchange, error) {
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

	err = verifyHash(data, filename)
	if err != nil {
		return nil, errors.Wrap(err, "failed to verify hash")
	}

	var msg v1.MessageExchange
	err = proto.Unmarshal(data, &msg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal message")
	}
	return &msg, nil
}

var reFileMatch = regexp.MustCompile(`^([0-9]{6})_([0-9]+).me$`)

func isRelevantFile(filename string) bool {
	return reFileMatch.MatchString(path.Base(filename))
}

func (ex *FileExchange) createFilename(data []byte) string {
	d := xxhash.New()
	d.Write(data) // does not error
	hash := d.Sum64()

	counter := ex.nextCounter()
	return fmt.Sprintf("%06d_%d.me", counter, hash)
}

func verifyHash(data []byte, filename string) error {
	d := xxhash.New()
	d.Write(data) // does not error
	hash := d.Sum64()

	matches := reFileMatch.FindStringSubmatch(path.Base(filename))
	if len(matches) != 3 {
		return errors.New("filename does not match expected format")
	}

	hashStr := matches[2]
	if hashStr != fmt.Sprintf("%d", hash) {
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
		var pe *os.PathError
		if errors.As(err, &pe) {
			if errors.Is(pe.Err, fs.ErrNotExist) {
				return errors.Wrapf(ErrDirectoryDoesNotExists, "directory '%s' does not exists", d)
			}
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
