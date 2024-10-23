package sync

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	contentTypeProto      = "application/grpc+proto"
	headerContentType     = "content-type"
	headerGrpcMessageType = "grpc-message-type"
	headerSourceSequence  = "sync-source-sequence"
)

type JSConfig struct {
	// nats urls, separated by ,
	URLs string

	Options []nats.Option

	// optional prefix for jetstream api
	JetstreamAPIPrefix string
}

func (c *JSConfig) WithSeedFile(seedFile string) error {
	opt, err := nats.NkeyOptionFromSeed(seedFile)
	if err != nil {
		return err
	}
	c.Options = append(c.Options, opt)
	return nil
}

func (c *JSConfig) WithSecure(secure *tls.Config) error {
	c.Options = append(c.Options, nats.Secure(secure))
	return nil
}

// nats jetstream connection
type JSConn struct {
	config JSConfig

	// chan with cap 1 to lock js
	jsLock chan struct{}
	js     *jetstream.JetStream
}

func NewJSConn(config JSConfig) *JSConn {
	return &JSConn{
		config: config,
		jsLock: make(chan struct{}, 1),
	}
}

// acquire connection to jetstream. Do not modify returned jetstream reference
// The context is only used to obtain a connection, not for the connection itself.
func (c *JSConn) Connect(ctx context.Context) (jetstream.JetStream, error) {
	// try to acquire lock
	select {
	case c.jsLock <- struct{}{}:
		// acquired lock

		// defer unlock
		defer func() {
			<-c.jsLock
		}()

		// already connected
		if c.js != nil {
			return *c.js, nil
		}

	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// no existing connection, connect
	nc, err := nats.Connect(c.config.URLs, c.config.Options...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect or bad options")
	}

	if len(c.config.JetstreamAPIPrefix) > 0 {
		js, err := jetstream.NewWithAPIPrefix(nc, c.config.JetstreamAPIPrefix)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create jetstream with api prefix")
		}
		c.js = &js
	} else {
		js, err := jetstream.New(nc)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create jetstream")
		}

		c.js = &js
	}
	return *c.js, nil
}

func (c *JSConn) PublishProto(ctx context.Context, subject string, headers map[string][]string, m proto.Message, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal proto message")
	}

	h := map[string][]string{
		headerContentType:     {contentTypeProto},
		headerGrpcMessageType: {string(m.ProtoReflect().Descriptor().FullName())}}

	for k, vs := range headers {
		if _, exists := h[k]; exists {
			return nil, fmt.Errorf("header %s already exists", k)
		}
		h[k] = vs
	}

	msg := &nats.Msg{
		Subject: subject,
		Header:  h,
		Data:    data}

	js, err := c.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	ack, err := js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to publish message")
	}

	return ack, nil
}

// publish raw message with optimistic concurrency. The stream must exists
func (c *JSConn) PublishRaw(ctx context.Context, stream string, lastSequence SinkSequence, m *v1.Msg) (*jetstream.PubAck, error) {

	headers := make(map[string][]string)
	for k, v := range m.GetHeaders() {
		headers[k] = strings.Split(v, ",")
	}

	// must have source sequence header
	headers[headerSourceSequence] = []string{strconv.FormatUint(m.GetSequence(), 10)}

	msg := &nats.Msg{
		Subject: m.Subject,
		Header:  headers,
		Data:    m.GetData()}

	js, err := c.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	ack, err := js.PublishMsg(ctx, msg,
		jetstream.WithExpectLastSequence(uint64(lastSequence)),
		jetstream.WithExpectStream(stream))
	if err != nil {
		return nil, errors.Wrap(err, "failed to publish message")
	}

	return ack, nil
}

func (c *JSConn) GetStream(ctx context.Context, streamName string) (jetstream.Stream, error) {
	js, err := c.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get stream info")
	}

	return stream, nil
}

// get last nats sequence published to stream.
// Returns jetstream.ErrStreamNotFound if stream does not exist
func (c *JSConn) GetLastSequence(ctx context.Context, streamName string) (uint64, error) {
	stream, err := c.GetStream(ctx, streamName)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get stream")
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get stream info")
	}

	return info.State.LastSeq, nil
}

type PublishedMessage struct {
	Subject            string
	Sequence           uint64
	PublishedTimestamp time.Time
	Headers            map[string][]string
	Data               []byte
}

func NewPublishedMessage(msg jetstream.Msg, meta *jetstream.MsgMetadata) PublishedMessage {
	return PublishedMessage{
		Subject:            msg.Subject(),
		Sequence:           meta.Sequence.Stream,
		PublishedTimestamp: meta.Timestamp,
		Headers:            msg.Headers(),
		Data:               msg.Data()}
}

// subscribe to a stream and receive messages in order.
// To unsubscribe, cancel the context.
func (c *JSConn) SubscribeOrderered(ctx context.Context, stream string, cfg jetstream.OrderedConsumerConfig) (<-chan WithError[PublishedMessage], error) {
	log := logWithConsumerConfig(slog.With("module", "nats_sync", "operation", "SubscribeOrderered"), cfg)
	innerCtx, cancel := context.WithCancel(ctx)

	js, err := c.Connect(innerCtx)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	consumer, err := js.OrderedConsumer(innerCtx, stream, cfg)
	if err != nil {
		cancel()
		return nil, errors.Wrapf(err, "failed to create ordered consumer for stream '%s'", stream)
	}

	ch := make(chan WithError[PublishedMessage])
	go func() {
		defer cancel()
		defer close(ch)

		lastSequence := uint64(0)
		for {
			msg, err := consumer.Next(jetstream.FetchHeartbeat(59*time.Second), jetstream.FetchMaxWait(2*time.Minute))
			if err != nil {
				if errors.Is(err, nats.ErrTimeout) {
					continue
				}
				if innerCtx.Err() != nil {
					return
				}
				log.Error("failed to get next message, closing", "err", err)
				ch <- WithError[PublishedMessage]{Error: err}
				return
			}

			meta, err := msg.Metadata()
			if err != nil {
				log.Error("failed to get message metadata, closing", "err", err)
				ch <- WithError[PublishedMessage]{
					Error: err,
					Item:  PublishedMessage{Subject: msg.Subject()}}
				return
			}

			pm := NewPublishedMessage(msg, meta)

			if lastSequence >= pm.Sequence {
				log.Log(ctx, slog.LevelDebug-3, "not relaying message, already processed",
					"sequence", pm.Sequence, "lastSequence", lastSequence)
			}

			select {
			case <-innerCtx.Done():
				return
			case ch <- WithError[PublishedMessage]{Item: pm}:
				// nop
			}

			lastSequence = pm.Sequence
		}
	}()

	return ch, nil
}

// get message with matching sequence from stream. Returns 0..1 messages.
// Will block if no message exists with sequence (or greater)
func (c *JSConn) GetMessageWithSequence(ctx context.Context, stream string, sequence uint64) ([]PublishedMessage, error) {
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   sequence}

	js, err := c.Connect(innerCtx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	consumer, err := js.OrderedConsumer(innerCtx, stream, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ordered consumer")
	}

	msg, err := consumer.Next()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get next message")
	}

	meta, err := msg.Metadata()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get metadata")
	}

	if meta.Sequence.Stream != sequence {
		return nil, nil
	}

	pm := NewPublishedMessage(msg, meta)
	return []PublishedMessage{pm}, nil
}

func (c *JSConn) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) error {
	js, err := c.Connect(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to connect to nats")
	}

	_, err = js.CreateStream(ctx, cfg)
	if err != nil {
		return errors.Wrap(err, "failed to create stream")
	}

	return nil
}

func logWithConsumerConfig(log *slog.Logger, cfg jetstream.OrderedConsumerConfig) *slog.Logger {
	l2 := log.With("deliverPolicy", cfg.DeliverPolicy, "filter", cfg.FilterSubjects)
	switch cfg.DeliverPolicy {
	case jetstream.DeliverByStartSequencePolicy:
		l2 = l2.With("optStartSeq", cfg.OptStartSeq)
	case jetstream.DeliverByStartTimePolicy:
		l2 = l2.With("optStartTime", cfg.OptStartTime)
	}
	return l2
}
