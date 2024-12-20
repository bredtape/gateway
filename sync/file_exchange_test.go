package sync

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFileIODirShouldExists(t *testing.T) {
	c := getConfig(t)
	c.IncomingDir = "./fail"

	_, err := NewFileExchange(c)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrDirectoryDoesNotExists)
}

func TestFileIO(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug - 3})))

	configA := getConfig(t)
	configB := getConfig(t)
	configB.IncomingDir = configA.OutgoingDir
	configB.OutgoingDir = configA.IncomingDir

	exA, err := NewFileExchange(configA)
	assert.NoError(t, err)

	// configure to consume other direction
	exB, err := NewFileExchange(configB)
	assert.NoError(t, err)

	chIncB, err := exB.StartReceiving(ctx)
	assert.NoError(t, err)

	msg1 := &v1.Msg{
		Subject:            "x.y.z",
		Data:               []byte("123"),
		Sequence:           2,
		PublishedTimestamp: timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))}
	batch1 := &v1.MessageBatch{
		ListOfMessages: []*v1.Msgs{
			{
				SourceStreamName: "stream1",
				Messages:         []*v1.Msg{msg1}}}}

	// send batch from A to B
	err = exA.Write(ctx, batch1)
	assert.NoError(t, err)
	t.Log("wrote batch")

	t.Log("wait for incoming at B")
	select {
	case <-ctx.Done():
		assert.FailNow(t, "did not receive msg at B in time")
	case msg, ok := <-chIncB:
		if !ok {
			assert.FailNow(t, "B closed result channel")
		}

		assert.Len(t, msg.ListOfMessages, 1)
	}
}

func TestFileIOWatchAfterWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug - 3})))

	configA := getConfig(t)
	configB := getConfig(t)
	configB.IncomingDir = configA.OutgoingDir
	configB.OutgoingDir = configA.IncomingDir

	exA, err := NewFileExchange(configA)
	assert.NoError(t, err)

	// configure to consume other direction
	exB, err := NewFileExchange(configB)
	assert.NoError(t, err)

	msg1 := &v1.Msg{
		Subject:            "x.y.z",
		Data:               []byte("123"),
		Sequence:           2,
		PublishedTimestamp: timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC))}
	batch1 := &v1.MessageBatch{
		ListOfMessages: []*v1.Msgs{
			{
				SourceStreamName: "stream1",
				Messages:         []*v1.Msg{msg1}}}}

	// send batch from A to B
	err = exA.Write(ctx, batch1)
	assert.NoError(t, err)
	t.Log("wrote batch")

	t.Log("start watch at B")
	chIncB, err := exB.StartReceiving(ctx)
	assert.NoError(t, err)

	t.Log("wait for incoming at B")
	select {
	case <-ctx.Done():
		assert.FailNow(t, "did not receive msg at B in time")
	case msg, ok := <-chIncB:
		if !ok {
			assert.FailNow(t, "B closed result channel")
		}

		assert.Len(t, msg.ListOfMessages, 1)
	}
}

func TestFileExhangeVerifyHash(t *testing.T) {
	filename := "000001_sha256=Xp6jbmSOX-ibQbWf8rXEx6wwixk5PmODzugpf7ud5j-U=.me"
	assert.NoError(t, checkHashAgainstFileName("sha256=Xp6jbmSOX-ibQbWf8rXEx6wwixk5PmODzugpf7ud5j-U=", filename))
}

func getConfig(t *testing.T) FileExchangeConfig {
	return FileExchangeConfig{
		IncomingDir:          t.TempDir(),
		OutgoingDir:          t.TempDir(),
		PollingInterval:      time.Second,
		PollingRetryInterval: time.Second}
}
