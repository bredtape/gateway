package nats_sync

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/bredtape/retry"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

// subscription stream name placeholder used in published messages.
// The actual stream may have a different name and is configured in the NatsSyncConfig for
// each deployment
const (
	subscriptionStream = "sync_subscriptions"
	contentTypeProto   = "application/grpc+proto"
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

	// exchanges for each deployment. Must match the deployments in CommunicationSettings
	Exchanges map[gateway.Deployment]Exchange
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

	if len(c.Exchanges) == 0 {
		return errors.New("exchanges empty")
	}

	if len(c.Exchanges) != len(c.CommunicationSettings) {
		return errors.New("exchanges and communicationSettings must have the same length")
	}

	for d, e := range c.Exchanges {
		if _, exists := c.CommunicationSettings[d]; !exists {
			return errors.Errorf("exchange for deployment %s does not have communicationSettings", d)
		}

		if e == nil {
			return errors.Errorf("exchange for deployment %s is nil", d)
		}
	}
	return nil
}

type CommunicationSettings struct {
	// --- settings pr subscription ---

	// timeout waiting for ack. Matching Msgs (based on SetID) will be resent.
	AckTimeoutPrSubscription time.Duration

	AckRetryPrSubscription retry.Retryer

	// backoff strategy for retrying when nak is received or ack is not received within timeout
	//NakBackoffPrSubscription retry.Retryer

	// heartbeat interval pr subscription.
	// If no messages arrive at the target for a subscription, the target should sent
	// empty Acknowledge at this interval and the source should send batches with empty messages.
	// This may be used to detect if a subscription has stalled at the source
	// (using the lowest source sequence received).
	HeartbeatIntervalPrSubscription time.Duration

	// the maximum number of messages with pending acks for a subscription.
	// Number of messages buffered at the source also counts towards this limit
	MaxPendingAcksPrSubscription int

	// the maximum number of messages buffered at the target, waiting to be persisted.
	// Must be at least MaxPendingAcksPrSubscription
	MaxPendingIncomingMessagesPrSubscription int

	// -- settings across all subscriptions --

	// max accumulated payload size in bytes for a MessageExchange message.
	// Must be able to hold at least one message
	MaxAccumulatedPayloadSizeBytes int
}

func (s CommunicationSettings) Validate() error {
	if s.AckTimeoutPrSubscription < time.Millisecond {
		return errors.New("AckTimeoutPrSubscription must be at least 1 ms")
	}
	if s.AckRetryPrSubscription == nil {
		return errors.New("AckRetryPrSubscription empty")
	}
	if s.AckRetryPrSubscription.MaxDuration() < time.Millisecond {
		return errors.New("AckRetryPrSubscription must be at least 1 ms")
	}
	if s.AckRetryPrSubscription.MaxDuration() < s.AckTimeoutPrSubscription {
		return errors.New("AckRetryPrSubscription MaxDuration must be at least AckTimeoutPrSubscription")
	}
	if s.HeartbeatIntervalPrSubscription < time.Millisecond {
		return errors.New("HeartbeatIntervalPrSubscription must be at least 1 ms")
	}
	if s.HeartbeatIntervalPrSubscription < 10*s.AckTimeoutPrSubscription {
		return errors.New("HeartbeatIntervalPrSubscription must be at least 10 times AckTimeoutPrSubscription")
	}
	if s.MaxPendingAcksPrSubscription <= 0 {
		return errors.New("MaxPendingAcksPrSubscription must be positive")
	}
	if s.MaxPendingIncomingMessagesPrSubscription <= 0 {
		return errors.New("MaxPendingIncomingMessagesPrSubscription must be positive")
	}
	if s.MaxPendingIncomingMessagesPrSubscription < s.MaxPendingAcksPrSubscription {
		return errors.New("MaxPendingIncomingMessagesPrSubscription must be at least MaxPendingAcksPrSubscription")
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

func StartNatsSync(ctx context.Context, js *JSConn, config NatsSyncConfig) (*NatsSync, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	ns := &NatsSync{config: config, js: js}
	return ns, ns.Start(ctx)
}

func (ns *NatsSync) Start(ctx context.Context) error {

	incoming := make(chan *v1.MessageBatch)

	for d, e := range ns.config.Exchanges {
		ch, err := e.StartReceiving(ctx)
		if err != nil {
			return errors.Wrapf(err, "failed to start receiving for deployment %s", d)
		}

		// forward incoming messages
		go func() {
			for msg := range ch {
				incoming <- msg
			}
		}()
	}

	go func() {
		log := slog.With("operation", "StartNatsSync", "deployment", ns.config.Deployment)
		defer log.Debug("stopping")
		for {
			select {
			case <-ctx.Done():
			case msg, ok := <-incoming:
				if ok {
					return
				}

				if msg.GetToDeployment() != ns.config.Deployment.String() {
					log.Debug("skipping message not for this deployment", "msg", msg)
					continue
				}

				err := ns.handleIncomingMessage(ctx, msg)
				log.Error("failed to handle incoming message, ignoring", "err", err)
			}
		}
	}()

	return nil
}

func (ns *NatsSync) handleIncomingMessage(ctx context.Context, msg *v1.MessageBatch) error {
	log := slog.With("operation", "handleIncomingMessage", "deployment", ns.config.Deployment)
	log.Debug("received message", "msg", msg)

	return nil
}

// create nats stream for subscribe stream. Should only be used for testing
func (ns *NatsSync) CreateSubscriptionStream(ctx context.Context) (jetstream.Stream, error) {
	c, err := ns.js.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	return c.CreateStream(ctx, jetstream.StreamConfig{
		Name:              ns.config.SubscriptionStream,
		Description:       "Stream for persisting Subscribe/Unsubscribe requests. Must exist at all 'deployments' participating in the sync. Subjects: <target deployment>.<source_deployment>. You must unsubscribe then subscribe again if only SubjectFilters or ConsumerConfig is different",
		Subjects:          []string{ns.config.SubscriptionStream + ".*.*"},
		Retention:         jetstream.LimitsPolicy,
		MaxMsgsPerSubject: 5,
		Discard:           jetstream.DiscardOld})
}

// publish Subscription request for the subscription itself from source to target deployment.
// The same request must be published at both the source and target deployment.
func (ns *NatsSync) PublishBootstrapSubscription(ctx context.Context, source, target gateway.Deployment) (*jetstream.PubAck, error) {
	req := &v1.StartSyncRequest{
		SourceDeployment: source.String(),
		ReplyDeployment:  source.String(),
		TargetDeployment: target.String(),
		SourceStreamName: subscriptionStream, // use placeholder
		TargetStreamName: subscriptionStream, // use placeholder
		FilterSubjects:   nil,
		ConsumerConfig: &v1.ConsumerConfig{
			DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}

	return ns.publishSubscribeRequest(ctx, req)
}

func (ns *NatsSync) publishSubscribeRequest(ctx context.Context, req *v1.StartSyncRequest) (*jetstream.PubAck, error) {
	subject := fmt.Sprintf("%s.%s.%s", ns.config.SubscriptionStream,
		req.TargetDeployment, req.SourceDeployment)

	return ns.js.PublishProto(ctx, subject, req, jetstream.WithExpectStream(ns.config.SubscriptionStream))
}
