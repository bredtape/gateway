package nats_transfer

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"regexp"
	"time"

	v1 "github.com/bredtape/gateway/nats_transfer/v1"
	"github.com/cespare/xxhash"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

var (
	ErrDirectoryDoesNotExists = errors.New("directory does not exists")
)

const channelCapacity = 10
const writeFileMode fs.FileMode = 0644

type Config struct {
	IncomingDir, OutgoingDir                                 string
	PollingStartDelay, PollingInterval, PollingRetryInterval time.Duration
	FuncNow                                                  func() time.Time
}

func (c Config) Validate() error {
	if c.IncomingDir == "" {
		return errors.New("incomingDir empty")
	}
	if c.OutgoingDir == "" {
		return errors.New("outgoingDir empty")
	}
	if path.Clean(c.IncomingDir) == path.Clean(c.OutgoingDir) {
		return errors.New("incoming and outgoing dir must not be the same")
	}
	if c.PollingInterval < time.Second {
		return errors.New("pollingInterval must be at least 1 sec")
	}
	if c.PollingRetryInterval < time.Second {
		return errors.New("pollingRetryInterval must be at least 1 sec")
	}
	return nil
}

type FileIO struct {
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
func NewFileIO(c Config) (*FileIO, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	fio := &FileIO{
		inDir:      path.Clean(c.IncomingDir),
		outDir:     path.Clean(c.OutgoingDir),
		now:        c.FuncNow,
		startDelay: c.PollingStartDelay,
		interval:   c.PollingInterval,
		retry:      c.PollingRetryInterval}

	return fio, fio.assertAllDirExists()
}

// watch for messages in the incoming directory.
// The files will be deleted after the content has been read.
// Do not assume any order
func (fio *FileIO) StartWatch(ctx context.Context) (chan *v1.MessageExchange, error) {
	log := slog.With("incomingDirectory", fio.inDir)

	// Create new watcher.
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to start fsnotify watcher")
	}
	resultCh := make(chan *v1.MessageExchange, channelCapacity)

	pollingResult := startPollDirectory(ctx, fio.startDelay, fio.interval, fio.retry, fio.inDir)

	// Start listening for events.
	go func() {
		log.Debug("start watching")
		defer log.Debug("stop watching")
		defer watcher.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case filename, ok := <-pollingResult:
				if !ok {
					return
				}

				log2 := log.With("source", "polling", "filename", filename)
				log2.Log(ctx, slog.LevelDebug-3, "received file")
				msg, err := fio.consumeFile(filename)
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
					msg, err := fio.consumeFile(event.Name)
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
	err = watcher.Add(fio.inDir)
	if err != nil {
		return nil, errors.Wrap(err, "failed to watch incoming directory")
	}

	return resultCh, nil
}

// write message exhange to underlying file system
// The message will be written with a . prefix first, then renamed
// File name will be <zero padded counter, 9 digits>_<hash>.me
func (fio *FileIO) Write(batch *v1.MessageExchange) error {
	log := slog.With("outDir", fio.outDir)
	data, err := proto.Marshal(batch)
	if err != nil {
		return errors.Wrap(err, "failed to marshal")
	}

	base := fio.createFilename(data)
	filename := path.Join(fio.outDir, base)
	tmpFilename := path.Join(fio.outDir, "."+base)

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
func (fio *FileIO) consumeFile(filename string) (*v1.MessageExchange, error) {
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

func (fio *FileIO) createFilename(data []byte) string {
	d := xxhash.New()
	d.Write(data) // does not error
	hash := d.Sum64()

	counter := fio.nextCounter()
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

func (fio *FileIO) nextCounter() uint32 {
	fio.counter++
	if fio.counter > 999_999 {
		fio.counter = 0
	}
	return fio.counter
}

func (fio *FileIO) assertAllDirExists() error {
	if fio.inDir == fio.outDir {
		return errors.New("directories must not be equal")
	}

	if err := assertDirExists(fio.inDir); err != nil {
		return errors.Wrap(err, "incoming directory does not exist")
	}

	if err := assertDirExists(fio.outDir); err != nil {
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
func startPollDirectory(ctx context.Context, startDelay, interval, retryInterval time.Duration, dir string) chan string {
	log := slog.With("operation", "startPollDirectory", "dir", dir, "interval", interval)
	ch := make(chan string, 10)

	t := time.After(startDelay)

	go func() {
		log.Debug("starting")
		defer log.Debug("stopping")

		for {
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

					case ch <- path.Join(dir, x.Name()):
					}
				}
			}
		}
	}()

	return ch
}
