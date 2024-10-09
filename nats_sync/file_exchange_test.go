package nats_sync

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/stretchr/testify/assert"
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
		Subject:          "x.y.z",
		Data:             []byte("123"),
		SourceSequence:   2,
		PublishTimestamp: 5}
	batch1 := &v1.MessageExchange{
		Messages: []*v1.MsgBatch{
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

		assert.Len(t, msg.Messages, 1)
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
		Subject:          "x.y.z",
		Data:             []byte("123"),
		SourceSequence:   2,
		PublishTimestamp: 5}
	batch1 := &v1.MessageExchange{
		Messages: []*v1.MsgBatch{
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

		assert.Len(t, msg.Messages, 1)
	}
}

func getConfig(t *testing.T) FileExchangeConfig {
	return FileExchangeConfig{
		IncomingDir:          t.TempDir(),
		OutgoingDir:          t.TempDir(),
		PollingInterval:      time.Second,
		PollingRetryInterval: time.Second}
}
