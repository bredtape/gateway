package nats_sync

import (
	"context"
	"fmt"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/bredtape/retry"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// subscription stream name placeholder used in published messages.
// The actual stream may have a different name and is configured in the NatsSyncConfig for
// each deployment
const (
	subscriptionStream = "sync_subscriptions"
	contentTypeProto   = "application/grpc+proto"
)

var (
	subscribeRequestFullProtoName = string((&v1.SubscribeRequest{}).ProtoReflect().Descriptor().FullName())
)

type NatsSyncConfig struct {
	Deployment gateway.Deployment
	// subscription stream to persist subscription requests.
	// Should exist, and must be replicated to all deployments participating in the sync.
	// This implies that requests only can be accepted at the deployment that is the
	// source of the stream.
	// Assuming subjects: <target deployment>.<source_deployment>
	// Retention with MaxMsgsPerSubject can be used to limit the number of messages
	SubscriptionStream string

	// communication settings pr deployment
	CommunicationSettings map[gateway.Deployment]CommunicationSettings
}

func (c NatsSyncConfig) Validate() error {
	if c.Deployment == "" {
		return errors.New("deployment empty")
	}
	if c.SubscriptionStream == "" {
		return errors.New("subscriptionStream empty")
	}

	if len(c.CommunicationSettings) == 0 {
		return errors.New("communicationSettings empty")
	}

	for d, s := range c.CommunicationSettings {
		if d == "" {
			return errors.New("empty deployment in communicationSettings")
		}
		if d == c.Deployment {
			return errors.New("deployment in communicationSettings is the same as the main deployment")
		}

		if ve := s.Validate(); ve != nil {
			return errors.Wrapf(ve, "invalid communicationSettings for deployment %s", d)
		}
	}
	return nil
}

type CommunicationSettings struct {
	// exchange implementation
	Exchange Exchange

	// --- settings pr subscription ---

	// timeout waiting for ack, before resending from the lowest source sequence
	// received for the relevant subscription OR restarting the subscription if the lowest
	// source sequence is not available.
	AckTimeoutPrSubscription time.Duration

	// backoff strategy for retrying when nak is received or ack is not received within timeout
	NakBackoffPrSubscription retry.Retryer

	// interval to wait for more messages for the same subscription before sending them in a batch
	// should be much lower than AckTimeout
	FlushIntervalPrSubscription time.Duration

	// heartbeat interval pr subscription.
	// If no messages arrive at the target for a subscription, the target should sent
	// empty Acknowledge at this interval.
	// This may be used to detect if a subscription has stalled at the source
	// (using the lowest source sequence received).
	HeartbeatIntervalPrSubscription time.Duration

	// the maximum number of acks that can be pending across all subscriptions
	MaxPendingAcksPrSubscription int

	// -- settings across all subscriptions --

	// max accumulated payload size in bytes for a MessageExchange message
	MaxAccumulatedPayloadSizeBytes int
}

func (s CommunicationSettings) Validate() error {
	if s.Exchange == nil {
		return errors.New("exchange empty")
	}
	if s.AckTimeoutPrSubscription < time.Millisecond {
		return errors.New("AckTimeoutPrSubscription must be at least 1 ms")
	}
	if s.NakBackoffPrSubscription == nil {
		return errors.New("NakBackoffPrSubscription empty")
	}
	if s.NakBackoffPrSubscription.MaxDuration() < time.Millisecond {
		return errors.New("NakBackoffPrSubscription must be at least 1 ms")
	}
	if s.FlushIntervalPrSubscription >= s.AckTimeoutPrSubscription {
		return errors.New("FlushIntervalPrSubscription must be less than AckTimeoutPrSubscription")
	}
	if s.HeartbeatIntervalPrSubscription < time.Millisecond {
		return errors.New("HeartbeatIntervalPrSubscription must be at least 1 ms")
	}
	if s.MaxAccumulatedPayloadSizeBytes <= 0 {
		return errors.New("MaxAccumulatedPayloadSizeBytes must be positive")
	}
	return nil
}

type NatsSync struct {
	config NatsSyncConfig
	js     *JSConn
}

func NewNatsSync(js *JSConn, config NatsSyncConfig) (*NatsSync, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	ns := &NatsSync{config: config, js: js}
	return ns, nil
}

// create nats stream for subscribe stream. Should only be used for testing
func (ns *NatsSync) CreateSubscriptionStream(ctx context.Context) (jetstream.Stream, error) {
	c, err := ns.js.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	return c.CreateStream(ctx, jetstream.StreamConfig{
		Name:              ns.config.SubscriptionStream,
		Description:       "Stream for persisting Subscribe/Unsubscribe requests. Must exist at all 'deployments' participating in the sync. Subjects: <target deployment>.<source_deployment>",
		Subjects:          []string{ns.config.SubscriptionStream + ".*.*"},
		Retention:         jetstream.LimitsPolicy,
		MaxMsgsPerSubject: 100,
		Discard:           jetstream.DiscardOld})
}

func (ns *NatsSync) PublishBootstrapSubscription(ctx context.Context, source, target gateway.Deployment) (*jetstream.PubAck, error) {
	js, err := ns.js.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	req := &v1.SubscribeRequest{
		SourceDeployment: source.String(),
		ReplyDeployment:  source.String(),
		TargetDeployment: target.String(),
		SourceStreamName: subscriptionStream, // use placeholder
		TargetStreamName: subscriptionStream, // use placeholder
		SubjectFilters:   nil,
		ConsumerConfig: &v1.ConsumerConfig{
			DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal subscribe request")
	}

	msg := &nats.Msg{
		Subject: fmt.Sprintf("%s.%s.%s", ns.config.SubscriptionStream, target, source),
		Header: map[string][]string{
			"content-type":      {contentTypeProto},
			"grpc-message-type": {subscribeRequestFullProtoName}},
		Data: data}

	ack, err := js.PublishMsg(ctx, msg, jetstream.WithExpectStream(ns.config.SubscriptionStream))
	if err != nil {
		return nil, errors.Wrap(err, "failed to publish message")
	}

	return ack, nil
}
