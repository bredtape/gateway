package nats_sync

import (
	"fmt"
	"slices"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/bredtape/set"
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
	ErrSlowConsumer            = errors.New("slow consumer")
	ErrAckWindowOutsidePending = errors.New("ack window outside pending")
)

// to manage internal state of sync.
type state struct {
	deployment gateway.Deployment
	cs         map[gateway.Deployment]CommunicationSettings
	// original subscription, remember if subscription is reset
	originalSubscription map[SubscriptionKey]sourceSubscription

	// outstanding acks
	ackWindows map[SubscriptionKey]AckWindow

	InfoRequests map[string]StreamInfoRequest
	PubRequests  map[string]PublishRequest

	// requested local subscription
	LocalSubscriptions map[SubscriptionKey]sourceSubscription

	Outgoing map[SubscriptionKey]*v1.MsgBatch
}

func newState(d gateway.Deployment, cs map[gateway.Deployment]CommunicationSettings) *state {
	return &state{
		deployment:           d,
		cs:                   cs,
		originalSubscription: make(map[SubscriptionKey]sourceSubscription),
		ackWindows:           make(map[SubscriptionKey]AckWindow),
		// public fields
		InfoRequests:       make(map[string]StreamInfoRequest),
		PubRequests:        make(map[string]PublishRequest),
		LocalSubscriptions: make(map[SubscriptionKey]sourceSubscription),
		Outgoing:           make(map[SubscriptionKey]*v1.MsgBatch)}
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
		req.GetTargetDeployment(),
		req.GetSourceStreamName(),
		req.GetConsumerConfig(), req.GetFilterSubjects())
	// keep the original subscription
	s.originalSubscription[sub.SubscriptionKey] = sub

	if isSource {
		subLocal := sub
		// be optimistic that the target has the most recent message
		subLocal.DeliverPolicy = jetstream.DeliverLastPolicy
		s.LocalSubscriptions[sub.SubscriptionKey] = subLocal
	}

	return nil
}

func (s *state) UnregisterSubscription(req *v1.UnsubscribeRequest) error {
	return nil
}

// handle both incoming messages and ack's
func (s *state) HandleIncoming(t time.Time, me *v1.MessageExchange) error {
	if me.ToDeployment != s.deployment.String() {
		return errors.New("not for this deployment")
	}

	// for _, ack := range me.Acknowledges {

	// }

	return nil
}

// handle incoming ack from target deployment
// ack may be out or order (so the sequence from/to are out of order)
func (s *state) HandleTargetAck(t time.Time, targetDeployment gateway.Deployment, ack *v1.Acknowledge) error {
	key := SubscriptionKey{
		TargetDeployment: targetDeployment,
		SourceStreamName: ack.GetSourceStreamName()}

	if _, exists := s.originalSubscription[key]; !exists {
		return errors.Wrapf(ErrNoSubscription, "no subscription for key %v", key)
	}

	if ack.GetIsNak() {
		return errors.New("not implemented")
	}

	w, exists := s.ackWindows[key]
	if !exists {
		// no outstanding acks, ignore
		return nil
	}

	r0 := w.Pending

	r1 := RangeInclusive[uint64]{
		From: ack.GetSourceSequenceFrom(),
		To:   ack.GetSourceSequenceTo()}

	// ack window is not pending, ignore
	if !r0.Overlaps(r1) {
		return nil
	}

	if r1.To > r0.To {
		return errors.Wrap(ErrAckWindowOutsidePending, "ack window is AFTER pending")
	}

	if !r0.Contains(r1) {
		return errors.New("ack window not contained in pending")
	}

	// ignore the fact that ack may be before pending From
	if r0.From != r1.From {
		return errors.New("expected ack window to align with pending")
	}

	if r0.To == r1.To {
		delete(s.ackWindows, key)
	} else {
		w.Pending.From = r1.To
		s.ackWindows[key] = w
	}

	return nil
}

// deliver local messages from requested subscriptions (to be sent to remote)
func (s *state) SourceDeliverFromLocal(b *v1.MsgBatch) error {
	key := GetSubscriptionKey(b)

	_, exists := s.LocalSubscriptions[key]
	if !exists {
		return errors.Wrapf(ErrNoSubscription, "no subscription for key %v", key)
	}

	if out, exists := s.Outgoing[key]; exists {
		if len(out.Messages) == 0 {
			return fmt.Errorf("outgoing messages should not be empty (key %v)", key)
		}

		err := mergeMsgBatch(out, b)
		if err != nil {
			return err
		}
	} else {
		s.Outgoing[key] = b
	}

	return nil
}

// mark batch as dispatched to target deployment at the specified time
func (s *state) SourceMarkDispatched(t time.Time, b *v1.MsgBatch) error {
	key := GetSubscriptionKey(b)
	delete(s.Outgoing, key)
	return nil
}

// message sequence MUST be in order.
// Message seqeunces MUST have some overlap, e.g. the last sequence of the first batch must
// be the same as the first sequence of the second batch.
// If an error is returned, the first batch has not be modified
func mergeMsgBatch(x, y *v1.MsgBatch) error {
	if x.GetSourceStreamName() != y.GetSourceStreamName() {
		return errors.New("source stream name mismatch")
	}

	if x.GetTargetDeployment() != y.GetTargetDeployment() {
		return errors.New("target deployment mismatch")
	}

	seen := set.New[uint64](len(x.Messages) + len(y.Messages))
	for _, x := range x.Messages {
		seen.Add(x.SourceSequence)
	}

	for _, y := range y.Messages {
		if seen.Contains(y.SourceSequence) {
			continue
		}
		seen.Add(y.SourceSequence)
		x.Messages = append(x.Messages, y)
	}

	slices.SortFunc(x.Messages, cmpMsgSequence)

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

// description at source nats stream
type sourceSubscription struct {
	SubscriptionKey

	DeliverPolicy  jetstream.DeliverPolicy
	OptStartSeq    uint64
	OptStartTime   time.Time
	FilterSubjects []string
}

func toSourceSubscription(targetDeployment, sourceStream string, s *v1.ConsumerConfig, filterSubjects []string) sourceSubscription {
	return sourceSubscription{
		SubscriptionKey: SubscriptionKey{
			TargetDeployment: targetDeployment,
			SourceStreamName: sourceStream},
		DeliverPolicy:  ToDeliverPolicy(s.GetDeliverPolicy()),
		OptStartSeq:    s.GetOptStartSeq(),
		OptStartTime:   fromUnixTime(s.GetOptStartTime()),
		FilterSubjects: filterSubjects}
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
	case v1.DeliverPolicy_DELIVER_POLICY_LAST_PER_SUBJECT:
		return jetstream.DeliverLastPerSubjectPolicy
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

func GetSubscriptionKey(b *v1.MsgBatch) SubscriptionKey {
	return SubscriptionKey{
		TargetDeployment: gateway.Deployment(b.GetTargetDeployment()),
		SourceStreamName: b.GetSourceStreamName()}
}

type AckWindow struct {
	// from and to sequence number, both inclusive
	Pending RangeInclusive[uint64]

	// number of messages in the window
	// This is because the sequence numbers are not necessarily contiguous
	//Count uint64
}
