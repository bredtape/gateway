package nats_sync

import (
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

func init() {
	// enable rand pool for better performance (batching)
	uuid.EnableRandPool()
}

var (
	ErrNoSubscription = errors.New("no subscription")
	// slow consumer error, when the target has too many messages queued
	ErrSlowConsumer         = errors.New("slow consumer")
	ErrSourceSequenceBroken = errors.New("source sequence broken. Subscription must be restarted")
	ErrBackoff              = errors.New("cannot accept new messages, pending messages must be dispatched first")
)

// to manage internal state of sync.
type state struct {
	deployment gateway.Deployment
	cs         map[gateway.Deployment]CommunicationSettings
	// original subscription, remember if subscription is reset
	subscription map[SubscriptionKey]SourceSubscription

	// outstanding acks
	sourceAckWindows map[SubscriptionKey]*SourcePendingWindow

	// messages to be sent to remote deployment.
	// Only present if there are messages to be sent for that subscription
	sourceOutgoing map[gateway.Deployment]map[SubscriptionKey][]*v1.Msg

	//InfoRequests map[string]StreamInfoRequest
	//PubRequests  map[string]PublishRequest

	// requested local subscription
	SourceLocalSubscriptions map[SubscriptionKey]SourceSubscription
}

func newState(d gateway.Deployment, cs map[gateway.Deployment]CommunicationSettings) *state {
	return &state{
		deployment: d,
		cs:         cs,
		// subscription at either source or target
		subscription: make(map[SubscriptionKey]SourceSubscription),

		// -- fields for source --

		sourceAckWindows: make(map[SubscriptionKey]*SourcePendingWindow),
		sourceOutgoing:   make(map[gateway.Deployment]map[SubscriptionKey][]*v1.Msg),

		// local subscription at source
		SourceLocalSubscriptions: make(map[SubscriptionKey]SourceSubscription),

		// -- fields for target --

	}
}

// Notes:
// Do not remember remote sequence, but simply restart subscription on startup,
// and then maybe fast-forward based on lowest_source_sequence_received

// register subscription. If this deployment matches the source a LocalSubscriptions entry
// will be present. If this deployment matches the target, messages are expected
// to arrive and will then be published to the (local) target stream.
func (s *state) RegisterSubscription(req *v1.SubscribeRequest) error {
	isSource := req.GetSourceDeployment() == s.deployment.String()
	isTarget := req.GetTargetDeployment() == s.deployment.String()

	if !isSource && !isTarget {
		return errors.New("neither source nor target")
	}

	sub := toSourceSubscription(
		gateway.Deployment(req.GetTargetDeployment()),
		req.GetSourceStreamName(),
		req.GetConsumerConfig(), req.GetFilterSubjects())
	// keep the original subscription
	s.subscription[sub.SubscriptionKey] = sub

	if isSource {
		subLocal := sub
		// be optimistic that the target has the most recent message
		subLocal.DeliverPolicy = jetstream.DeliverLastPolicy
		s.SourceLocalSubscriptions[sub.SubscriptionKey] = subLocal
	}

	return nil
}

func (s *state) UnregisterSubscription(req *v1.UnsubscribeRequest) error {
	return nil
}

// stream info request for local nats stream
type StreamInfoRequest struct {
	StreamName     string
	SubjectFilters []string
}

type StreamInfoResponse struct {
	StreamName string
	IsEmpty    bool

	// optional subject filters
	SubjectFilters []string

	// head sequence given the subject filters
	HeadSequence uint64
}

// message that should be published to local nats stream
type PublishRequest struct {
	SourceStreamName string
	TargetStreamName string

	// sequence of the last message published to the target stream. 0 if no messages have been published
	// Used for optimistic concurrency check
	LastSequence uint64

	// batch of messages where the source and previous source sequence are aligned
	// The SourceSequence (if non-zero) of the first message must match the LastSequence
	Messages []*v1.Msg
}

type PublishResponse struct {
	Error error

	SourceStreamName             string
	SourceSequence               uint64
	Subject                      string
	LowestSourceSequenceReceived uint64
}

func ToDeliverPolicy(p v1.DeliverPolicy) jetstream.DeliverPolicy {
	switch p {
	case v1.DeliverPolicy_DELIVER_POLICY_ALL:
		return jetstream.DeliverAllPolicy
	case v1.DeliverPolicy_DELIVER_POLICY_LAST:
		return jetstream.DeliverLastPolicy
	case v1.DeliverPolicy_DELIVER_POLICY_NEW:
		return jetstream.DeliverNewPolicy
	case v1.DeliverPolicy_DELIVER_POLICY_BY_START_SEQUENCE:
		return jetstream.DeliverByStartSequencePolicy
	case v1.DeliverPolicy_DELIVER_POLICY_BY_START_TIME:
		return jetstream.DeliverByStartTimePolicy
	default:
		panic("unknown deliver policy")
	}
}

func FromDeliverPolicy(p jetstream.DeliverPolicy) v1.DeliverPolicy {
	switch p {
	case jetstream.DeliverAllPolicy:
		return v1.DeliverPolicy_DELIVER_POLICY_ALL
	case jetstream.DeliverLastPolicy:
		return v1.DeliverPolicy_DELIVER_POLICY_LAST
	case jetstream.DeliverNewPolicy:
		return v1.DeliverPolicy_DELIVER_POLICY_NEW
	case jetstream.DeliverByStartSequencePolicy:
		return v1.DeliverPolicy_DELIVER_POLICY_BY_START_SEQUENCE
	case jetstream.DeliverByStartTimePolicy:
		return v1.DeliverPolicy_DELIVER_POLICY_BY_START_TIME
	default:
		panic("unknown deliver policy")
	}
}

func fromUnixTime(seconds float64) time.Time {
	if seconds == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(seconds*1e9))
}

func toUnixTime(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

// compare messages by source sequence (only!)
func cmpMsgSequence(a, b *v1.Msg) int {
	if a.SourceSequence < b.SourceSequence {
		return -1
	}
	if a.SourceSequence > b.SourceSequence {
		return 1
	}
	return 0
}

type SubscriptionKey struct {
	TargetDeployment gateway.Deployment
	SourceStreamName string
}

func GetSubscriptionKey(b *v1.Msgs) SubscriptionKey {
	return SubscriptionKey{
		TargetDeployment: gateway.Deployment(b.GetTargetDeployment()),
		SourceStreamName: b.GetSourceStreamName()}
}

// set ID used when sending a set of messages to be acknowledged
type SetID uuid.UUID

func NewSetID() SetID {
	id := uuid.New()
	return SetID(id)
}

func (id SetID) String() string {
	return uuid.UUID(id).String()
}

func (id SetID) GetBytes() []byte {
	return id[:]
}

func NewSetIDFromBytes(x []byte) (SetID, error) {
	u, err := uuid.FromBytes(x)
	if err != nil {
		return SetID{}, err
	}
	return SetID(u), nil
}
