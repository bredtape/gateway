package sync

import (
	"time"

	"github.com/bredtape/retry"
	"github.com/pkg/errors"
)

type CommunicationSettings struct {
	// --- settings pr subscription ---

	// timeout waiting for ack. Matching Msgs (based on SetID) will be resent.
	AckTimeoutPrSubscription time.Duration

	// backoff strategy for retrying when ack is not received within timeout
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
	PendingAcksPrSubscriptionMax int

	// the maximum number of messages buffered at the target, waiting to be persisted.
	// Must be at least MaxPendingAcksPrSubscription, but should be much higher (because if
	// messages arrive out-of-order, they must all be NAK'ed unless there is room for them)
	PendingIncomingMessagesPrSubscriptionMaxBuffered     int
	PendingIncomingMessagesPrSubscriptionDeleteThreshold int

	// -- settings across all subscriptions --

	// max accumulated payload size in bytes for a MessageExchange message.
	// Must be able to hold at least one message
	MaxAccumulatedPayloadSizeBytes int

	// common timeout for nats operations (publish and getXYZ requests)
	NatsOperationTimeout time.Duration

	// flush timeout for writing messages and acks to Exchange
	BatchFlushTimeout        time.Duration
	ExchangeOperationTimeout time.Duration
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
	if s.PendingAcksPrSubscriptionMax <= 0 {
		return errors.New("MaxPendingAcksPrSubscription must be positive")
	}
	if s.PendingIncomingMessagesPrSubscriptionMaxBuffered <= 0 {
		return errors.New("PendingIncomingMessagesPrSubscriptionMaxBuffered must be positive")
	}
	if s.PendingIncomingMessagesPrSubscriptionMaxBuffered < s.PendingAcksPrSubscriptionMax {
		return errors.New("PendingIncomingMessagesPrSubscriptionMaxBuffered must be at least MaxPendingAcksPrSubscription")
	}
	if s.PendingIncomingMessagesPrSubscriptionDeleteThreshold <= 0 {
		return errors.New("PendingIncomingMessagesPrSubscriptionDeleteThreshold must be positive")
	}
	if s.PendingIncomingMessagesPrSubscriptionDeleteThreshold > s.PendingIncomingMessagesPrSubscriptionMaxBuffered {
		return errors.New("PendingIncomingMessagesPrSubscriptionDeleteThreshold must not be higher than PendingIncomingMessagesPrSubscriptionMaxBuffered")
	}
	if s.PendingIncomingMessagesPrSubscriptionDeleteThreshold < s.PendingAcksPrSubscriptionMax {
		return errors.New("PendingIncomingMessagesPrSubscriptionDeleteThreshold must be at least MaxPendingAcksPrSubscription")
	}
	if s.MaxAccumulatedPayloadSizeBytes <= 0 {
		return errors.New("MaxAccumulatedPayloadSizeBytes must be positive")
	}
	if s.NatsOperationTimeout < time.Millisecond {
		return errors.New("NatsOperationTimeout must be at least 1 ms")
	}
	if s.BatchFlushTimeout < time.Millisecond {
		return errors.New("BatchFlushTimeout must be at least 1 ms")
	}
	if s.ExchangeOperationTimeout < time.Millisecond {
		return errors.New("ExchangeOperationTimeout must be at least 1 ms")
	}
	return nil
}
