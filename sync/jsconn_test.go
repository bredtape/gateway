package sync

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

const (
	envNatsURLs      = "TEST_NATS_URLS"
	fallbackNatsURLs = "nats://localhost:4222"
)

func getNatsURLs() string {
	urls := os.Getenv(envNatsURLs)
	if urls == "" {
		urls = fallbackNatsURLs
	}

	return urls
}

// any err handler we can hook into?
// what does the MessageContext handle that Next does not?
// when does MessageContext.Next fail? does it retry on all errors (related to connection and consumer)?

func TestJSConnStartSubscribeOrdereredConsumerNext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	js := NewJSConn(JSConfig{URLs: getNatsURLs()})

	streamName := createRandomStream(ctx, js, t)

	lastSeq := publishMessages(ctx, t, js, streamName, 20, 0)

	go func() {
		select {
		case <-ctx.Done():
			t.Error("context done")
		case <-time.After(10 * time.Second):
			publishMessages(ctx, t, js, streamName, 20, 20)
			log.Info("published more messages")
		}
	}()

	t.Log("subscribe")
	startSeq := lastSeq - 10
	consumerConfig := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: startSeq}
	j, err := js.Connect(ctx)
	assert.NoError(t, err)
	consumer, err := j.OrderedConsumer(ctx, streamName, consumerConfig)
	assert.NoError(t, err)

	t.Log("get messages")
	for {
		msg, err := consumer.Next(jetstream.FetchHeartbeat(time.Second), jetstream.FetchMaxWait(5*time.Second))
		if errors.Is(err, nats.ErrTimeout) {
			log.Info("consumer next err, ignoring", "err", err)
			continue
		}
		if err != nil {
			t.Fatalf("failed to get next message: %v", err)
		}

		if msg == nil {
			t.Error("nil message")
		}

		meta, err := msg.Metadata()
		assert.NoError(t, err)

		assert.GreaterOrEqual(t, meta.Sequence.Stream, startSeq)
		t.Logf("received seq. %d", meta.Sequence.Stream)

		// if meta.Sequence.Stream >= info.State.LastSeq {
		// 	break
		// }
	}
}

func TestJSConnResubscribeWithOptStartSeq(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	js := NewJSConn(JSConfig{URLs: getNatsURLs()})

	streamName := createRandomStream(ctx, js, t)

	lastSeq := publishMessages(ctx, t, js, streamName, 20, 0)

	for startSeq := uint64(1); startSeq < lastSeq; startSeq++ {
		log.Info("subscribe from", "startSeq", startSeq)

		err := func() error {
			consumerConfig := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: startSeq}
			j, err := js.Connect(ctx)
			if err != nil {
				return errors.Wrap(err, "failed to connect")
			}

			consumer, err := j.OrderedConsumer(ctx, streamName, consumerConfig)
			if err != nil {
				return errors.Wrap(err, "failed to create consumer")
			}

			msg, err := consumer.Next(jetstream.FetchHeartbeat(time.Second), jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				return errors.Wrap(err, "failed to get next message")
			}

			if msg == nil {
				t.Error("nil message")
			}

			meta, err := msg.Metadata()
			assert.NoError(t, err)

			t.Logf("received seq. %d", meta.Sequence.Stream)
			if meta.Sequence.Stream != startSeq {
				t.Errorf("expected sequence %d, got %d", startSeq, meta.Sequence.Stream)
			}
			return nil
		}()

		if err != nil {
			t.Errorf("startSeq %d, failed to subscribe: %v", startSeq, err)
		}
	}
}

func TestJSConnSubscribeOrdered(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug - 3}))
	slog.SetDefault(log)

	js := NewJSConn(JSConfig{URLs: getNatsURLs()})

	streamName := createRandomStream(ctx, js, t)

	lastSeq := publishMessages(ctx, t, js, streamName, 20, 0)

	var wg sync.WaitGroup
	for n := 0; n < 10; n++ {
		wg.Add(1)
		startSeq := 1 + rand.Uint64N(lastSeq)

		t.Run(fmt.Sprintf("start seq %d", startSeq), func(t *testing.T) {
			defer wg.Done()
			log.Info("subscribe from", "startSeq", startSeq)

			now := time.Time{}
			consumerConfig := jetstream.OrderedConsumerConfig{
				DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
				OptStartSeq:   startSeq,
				OptStartTime:  &now,
			}
			//ch := make(chan PublishedMessage)
			//js.subscribeConsumer(ctx, ch, streamName, consumerConfig)
			ch := js.StartSubscribeOrderered(ctx, streamName, consumerConfig)

			msg := <-ch
			t.Logf("received seq. %d", msg.Sequence)
			if msg.Sequence != startSeq {
				t.Errorf("expected sequence %d, got %d", startSeq, msg.Sequence)
			}
		})
	}
	wg.Wait()
}

func TestJSConnSubscribeConsumerTest(t *testing.T) {
	outerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug - 3}))
	slog.SetDefault(log)

	js := NewJSConn(JSConfig{URLs: getNatsURLs()})

	streamName := createRandomStream(outerCtx, js, t)

	lastSeq := publishMessages(outerCtx, t, js, streamName, 20, 0)

	var wg sync.WaitGroup
	for n := 0; n < 100; n++ {
		wg.Add(1)
		startSeq := 1 + rand.Uint64N(lastSeq-1)

		t.Run(fmt.Sprintf("index %d, start seq %d", n, startSeq), func(t *testing.T) {
			defer wg.Done()

			log.Info("subscribe from", "startSeq", startSeq)
			ctx, cancel := context.WithCancel(context.Background())

			ch := make(chan PublishedMessage, 1)
			var xs []PublishedMessage
			go func() {
				defer cancel()
				select {
				case <-outerCtx.Done():
					t.Error("outer context done")
					return
				case msg, ok := <-ch:
					if !ok {
						t.Error("channel closed")
						return
					}
					xs = append(xs, msg)
				}
			}()
			consumerConfig := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: startSeq}
			lastSeq, err := js.subscribeConsumer(ctx, ch, streamName, consumerConfig)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("failed to subscribe: %v", err)
				}
			}
			if lastSeq != startSeq {
				t.Errorf("expected sequence %d, got %d", startSeq, lastSeq)
			}
			t.Logf("received %d messages", len(xs))

			if len(xs) != 1 {
				t.Errorf("expected 1 message, got %d", len(xs))
			} else if xs[0].Sequence != startSeq {
				t.Errorf("expected sequence %d, got %d", startSeq, xs[0].Sequence)
			}
		})
	}
	wg.Wait()
}

func TestJSConnStartSubscribeOrdereredMessagesContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	js := NewJSConn(JSConfig{URLs: getNatsURLs()})

	streamName := createRandomStream(ctx, js, t)

	lastSeq := publishMessages(ctx, t, js, streamName, 20, 0)
	go func() {
		select {
		case <-ctx.Done():
			t.Error("context done")
		case <-time.After(10 * time.Second):
			publishMessages(ctx, t, js, streamName, 20, 20)
			log.Info("published more messages")
		}
	}()

	log.Info("subscribe")
	j, err := js.Connect(ctx)
	assert.NoError(t, err)

	startSeq := lastSeq - 10
	consumerConfig := jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: startSeq}
	consumer, err := j.OrderedConsumer(ctx, streamName, consumerConfig)
	assert.NoError(t, err)

	messageContext, err := consumer.Messages(jetstream.PullHeartbeat(time.Second))
	assert.NoError(t, err)
	defer messageContext.Stop()

	t.Log("get messages")
	for {

		msg, err := messageContext.Next()
		if err != nil {
			t.Fatalf("failed to get next message: %v", err)
		}

		// if msg == nil {
		// 	continue
		// }

		meta, err := msg.Metadata()
		assert.NoError(t, err)

		assert.GreaterOrEqual(t, meta.Sequence.Stream, startSeq)
		log.Info("received", "sequence", meta.Sequence.Stream)

		if meta.Sequence.Stream == 10 {
			log.Info("waiting 10 sec before continuing")
			<-time.After(10 * time.Second)
		}
		if meta.Sequence.Stream >= lastSeq {
			log.Info("reached last message", "sequence", meta.Sequence.Stream)
			break
		}
	}
}

func createRandomStream(ctx context.Context, js *JSConn, t *testing.T) string {
	streamName := strings.ReplaceAll(generateRandomString(), "-", "") + "_jsconn_test"
	cfg := jetstream.StreamConfig{
		Name:        streamName,
		Description: "desc",
		Subjects:    []string{streamName + ".*"}}

	err := js.CreateStream(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	t.Logf("created stream: %s", streamName)
	return streamName
}

func publishMessages(ctx context.Context, t *testing.T, js *JSConn, streamName string, total, last int) uint64 {
	t.Log("publish messages")
	for n := 0; n < total; n++ {
		seq, err := js.PublishRaw(ctx, streamName, SinkSequence(n+last), &v1.Msg{
			Subject:  streamName + ".x",
			Sequence: uint64(n + 1 + last),
			Data:     []byte(fmt.Sprintf("%d", n))})
		if err != nil {
			t.Fatalf("failed to publish #%d message: %v", n+1, err)
		}
		t.Logf("published sequence %d", seq.Sequence)
	}

	t.Log("get stream info")
	stream, err := js.GetStream(ctx, streamName)
	assert.NoError(t, err)

	info, err := stream.Info(ctx)
	assert.NoError(t, err)
	t.Logf("stream info, first seq. %d, last seq. %d", info.State.FirstSeq, info.State.LastSeq)
	return info.State.LastSeq
}
