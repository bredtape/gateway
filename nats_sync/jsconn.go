package nats_sync

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	contentTypeProto      = "application/grpc+proto"
	headerContentType     = "content-type"
	headerGrpcMessageType = "grpc-message-type"
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
	js, err := c.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

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

	ack, err := js.PublishMsg(ctx, msg, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to publish message")
	}

	return ack, nil
}

type PublishedMessage struct {
	Subject            string
	Sequence           uint64
	PublishedTimestamp time.Time
	Headers            map[string][]string
	Data               []byte
}

// subscribe to a stream and receive messages in order.
// To unsubscribe, cancel the context.
func (c *JSConn) SubscribeOrderered(ctx context.Context, stream string, cfg jetstream.OrderedConsumerConfig) (<-chan WithError[PublishedMessage], error) {
	log := logWithConsumerConfig(slog.With("module", "nats_sync", "operation", "SubscribeOrderered"), cfg)

	js, err := c.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	consumer, err := js.OrderedConsumer(ctx, stream, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ordered consumer")
	}

	ch := make(chan WithError[PublishedMessage])
	go func() {
		defer close(ch)

		lastSequence := uint64(0)
		for {
			msg, err := consumer.Next(jetstream.FetchHeartbeat(59*time.Second), jetstream.FetchMaxWait(2*time.Minute))
			if err != nil {
				if ctx.Err() != nil {
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
					Item: PublishedMessage{
						Subject: msg.Subject()}}
				return
			}

			pm := PublishedMessage{
				Subject:            msg.Subject(),
				Sequence:           meta.Sequence.Stream,
				PublishedTimestamp: meta.Timestamp,
				Headers:            msg.Headers(),
				Data:               msg.Data()}

			if pm.Sequence <= lastSequence {
				log.Debug("not relaying message, already processed", "sequence", pm.Sequence, "lastSequence", lastSequence)
				err = msg.Ack()
				if err != nil {
					log.Warn("failed to ack message", "err", err)
				}
			}

			select {
			case <-ctx.Done():
				return
			case ch <- WithError[PublishedMessage]{Item: pm}:
				// nop
			}

			lastSequence = pm.Sequence

			err = msg.Ack()
			if err != nil {
				log.Warn("failed to ack message", "err", err)
				// TODO: Consider failure modes. Does failed ack imply redeliver?
			}
		}
	}()

	return ch, nil
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
