package nats_sync

import (
	"fmt"
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

	// -- fields for source --
	// outstanding acks
	sourceAckWindows map[SourceSubscriptionKey]*SourcePendingWindow

	// outgoing messages per target deployment
	// Only present if there are messages to be sent for that subscription
	sourceOutgoing map[gateway.Deployment]map[SourceSubscriptionKey][]*v1.Msg

	//InfoRequests map[string]StreamInfoRequest
	//PubRequests  map[string]PublishRequest

	// original source subscription, needed when subscription is restarted
	sourceOriginalSubscription map[SourceSubscriptionKey]SourceSubscription

	// requested local subscription
	SourceLocalSubscriptions map[SourceSubscriptionKey]SourceSubscription

	// -- fields for target --

	targetCommit map[TargetSubscriptionKey]*TargetCommitWindow

	// target incoming per source deployment
	// Ordered by sequence From
	TargetIncoming map[TargetSubscriptionKey][]*v1.Msgs

	// target subscription
	targetSubscription map[TargetSubscriptionKey]TargetSubscription
}

func newState(d gateway.Deployment, cs map[gateway.Deployment]CommunicationSettings) *state {
	return &state{
		deployment: d,
		cs:         cs,
		// subscription at either source or target
		sourceOriginalSubscription: make(map[SourceSubscriptionKey]SourceSubscription),
		sourceAckWindows:           make(map[SourceSubscriptionKey]*SourcePendingWindow),
		sourceOutgoing:             make(map[gateway.Deployment]map[SourceSubscriptionKey][]*v1.Msg),
		SourceLocalSubscriptions:   make(map[SourceSubscriptionKey]SourceSubscription),
		targetCommit:               make(map[TargetSubscriptionKey]*TargetCommitWindow),
		TargetIncoming:             make(map[TargetSubscriptionKey][]*v1.Msgs),
		targetSubscription:         make(map[TargetSubscriptionKey]TargetSubscription)}
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

	if isSource {
		sub := toSourceSubscription(
			gateway.Deployment(req.GetTargetDeployment()),
			req.GetSourceStreamName(),
			req.GetConsumerConfig(), req.GetFilterSubjects())
		// keep the original subscription
		s.sourceOriginalSubscription[sub.SourceSubscriptionKey] = sub

		subLocal := sub
		// be optimistic that the target has the most recent message
		subLocal.DeliverPolicy = jetstream.DeliverLastPolicy
		s.SourceLocalSubscriptions[sub.SourceSubscriptionKey] = subLocal
		return nil
	}

	if isTarget {
		key := TargetSubscriptionKey{
			SourceDeployment: gateway.Deployment(req.GetSourceDeployment()),
			SourceStreamName: req.GetSourceStreamName()}

		sub := TargetSubscription{
			TargetSubscriptionKey: key,
			TargetDeployment:      gateway.Deployment(req.GetTargetDeployment()),
			DeliverPolicy:         ToDeliverPolicy(req.GetConsumerConfig().GetDeliverPolicy()),
			OptStartSeq:           req.GetConsumerConfig().GetOptStartSeq(),
			OptStartTime:          fromUnixTime(req.GetConsumerConfig().GetOptStartTime()),
			FilterSubjects:        req.GetFilterSubjects()}

		s.targetSubscription[key] = sub
		return nil
	}

	return errors.New("neither source nor target")

}

func (s *state) UnregisterSubscription(req *v1.UnsubscribeRequest) error {
	return errors.New("not implemented")
}

// create batch of messages and acks queued for 'to' deployment. Returns nil if no messages are queued
// This will not delete the messages from the outgoing queue (you must call SourceMarkDispatched)
func (s *state) CreateMessageBatch(t time.Time, to gateway.Deployment) (*v1.MessageBatch, error) {
	m := &v1.MessageBatch{
		ToDeployment:   to.String(),
		FromDeployment: s.deployment.String(),
		SentTimestamp:  toUnixTime(t)}

	for key := range s.sourceOutgoing[to] {
		b := s.packMessages(key)
		if b != nil {
			m.ListOfMessages = append(m.ListOfMessages, b)
		}
	}

	for _, w := range s.targetCommit {
		for _, ack := range w.PendingAcks[to] {
			m.Acknowledges = append(m.Acknowledges, ack)
		}
	}

	if len(m.ListOfMessages) == 0 && len(m.Acknowledges) == 0 {
		return nil, nil
	}
	return m, nil
}

type DispatchReport struct {
	MessagesErrors map[SourceSubscriptionKey]error
	AcksErrors     map[TargetSubscriptionKey]error
}

func (r DispatchReport) IsEmpty() bool {
	return len(r.MessagesErrors) == 0 && len(r.AcksErrors) == 0
}

// mark batch as dispatched to target deployment.
// A (partial) error may be returned per Subscription, meaning messages for other
// subscription may have succeeded.
func (s *state) MarkDispatched(b *v1.MessageBatch) DispatchReport {
	if b == nil {
		return DispatchReport{}
	}

	d := gateway.Deployment(b.GetToDeployment())

	msgsErrs := make(map[SourceSubscriptionKey]error)
	for _, msgs := range b.ListOfMessages {
		key := SourceSubscriptionKey{
			TargetDeployment: d,
			SourceStreamName: msgs.GetSourceStreamName()}

		setID, err := NewSetIDFromBytes(msgs.GetSetId())
		if err != nil {
			msgsErrs[key] = errors.Wrap(err, "failed to parse batch id")
			continue
		}

		if _, exists := s.sourceAckWindows[key]; !exists {
			s.sourceAckWindows[key] = &SourcePendingWindow{}
		}
		w := s.sourceAckWindows[key]

		ack := SourcePendingAck{
			SetID:         setID,
			SentTimestamp: fromUnixTime(b.GetSentTimestamp()),
			SequenceRange: RangeInclusive[uint64]{
				From: msgs.GetLastSequence(),
				To:   msgs.GetLastSequence()}}

		if len(msgs.GetMessages()) > 0 {
			ack.SequenceRange.To = msgs.GetMessages()[len(msgs.GetMessages())-1].GetSequence()
		}

		err = w.MarkDispatched(ack)
		if err != nil {
			msgsErrs[key] = errors.Wrap(err, "failed to mark dispatched")
		}

		delete(s.sourceOutgoing[d], key)
	}

	// acknowledges
	ackErrs := make(map[TargetSubscriptionKey]error)
	for _, ack := range b.Acknowledges {
		key := TargetSubscriptionKey{
			SourceDeployment: gateway.Deployment(b.GetToDeployment()),
			SourceStreamName: ack.GetSourceStreamName()}

		w, exists := s.targetCommit[key]
		if !exists {
			ackErrs[key] = fmt.Errorf("no target commits with key %s, have %v", key, s.targetCommit)
			continue
		}

		id, err := NewSetIDFromBytes(ack.GetSetId())
		if err != nil {
			ackErrs[key] = errors.Wrap(err, "failed to parse set id")
			continue
		}
		delete(w.PendingAcks[key.SourceDeployment], id)
	}

	return DispatchReport{
		MessagesErrors: msgsErrs,
		AcksErrors:     ackErrs}
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

	SourceStreamName       string
	Sequence               uint64
	Subject                string
	LowestSequenceReceived uint64
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
	if a.Sequence < b.Sequence {
		return -1
	}
	if a.Sequence > b.Sequence {
		return 1
	}
	return 0
}

type SourceSubscriptionKey struct {
	TargetDeployment gateway.Deployment
	SourceStreamName string
}

func GetSubscriptionKey(b *v1.Msgs) SourceSubscriptionKey {
	return SourceSubscriptionKey{
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
