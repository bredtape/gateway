package nats_sync

import (
	"context"
	"time"

	"github.com/bredtape/gateway"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

type NatsSyncConfig struct {
	// subscription stream to persist subscription requests.
	// Should exist, and must be replicated to all deployments participating in the sync.
	// Assuming subjects: <target deployment>.<source_deployment>.<source_stream>
	// Retention with MaxMsgsPerSubject can be used to limit the number of messages
	SubscriptionStream string

	// communication settings pr deployment
	CommunicationSettings map[gateway.Deployment]CommunicationSettings
}

type CommunicationSettings struct {
	Exchange Exchange

	// timeout waiting for ack, before resending from the lowest source sequence received for the relevant subscription
	AckTimeout time.Duration

	// the maximum number of acks that can be pending across all subscriptions
	MaxPendingAcks int

	// heartbeat interval pr subscription.
	// If no messages arrive at the target for a subscription, the target should sent empty Acknowledge at this interval. This may be used to detect if a subscription has stalled at the source (using the lowest source sequence received).
	HeartbeatInterval time.Duration
}

type NatsSync struct {
	config NatsSyncConfig
	js     *JSConn
}

func NewNatsSync(ctx context.Context, js *JSConn, config NatsSyncConfig) (*NatsSync, error) {
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
		Description:       "Stream for persisting Subscribe/Unsubscribe requests. Must exist at all 'deployments' participating in the sync. Subjects: <target deployment>.<source_deployment>.<source_stream>",
		Subjects:          []string{"*.*.*"},
		Retention:         jetstream.LimitsPolicy,
		MaxMsgsPerSubject: 100,
		Discard:           jetstream.DiscardOld})
}
