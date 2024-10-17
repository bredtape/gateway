package nats_sync

import (
	"fmt"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	ErrPayloadTooLarge      = errors.New("payload too large")
)

/* TODO:
guarantee that messages are delivered in order
[x] at target, send nak if Msgs is rejected (because it is out of sequence with target)
[x] at source, handle nak by restarting subscription
[x] at source, do not restart subscription again if a nak is received and ack is pending for a Msgs which
	  contains the requested sequence range.
[-] at target, do not accept batches with overlapping sequences (because it is impossible to keep 'Count')
[-] at source, do not accept ack of batches with overlapping sequences (because it is impossible to keep 'Count')
[x] initial source window start sequence (which could be persisted to improve startup)

backpressure:
[x] at source, do not accept new messages if there are too many pending messages (MaxPendingAcksPrSubscription)
[x] at source, accept a partial batch of messages if limit has not been reached, and then apply backpressure
[x] at target, do not accept incoming messages if there are too many pending messages (MaxPendingIncomingMessagesPrSubscription)

ack timeout:
[x] at source, if ack is not received within AckTimeout, resend Msgs again (with the same SetID)

heartbeat:
[x] at source, add empty Msgs to the batch if no activity for a subscription within HeartbeatIntervalPrSubscription
	  No activity means:
		* no messages have been sent
		* no messages have been resent
		* no pending acks
		* but some messages must have been sent before (to initialize the window)
[x] at target, accept empty Msgs and send Acknowledge

max size:
[x] at source, do not send batches that exceed MaxAccumulatedPayloadSizeBytes

flush:
[ ] at source, with pending messages/acks pr target, indicate the earliest time messages/acks should be sent to the target

short circuit:
[ ] at source, if no acknowledgements are received for all streams per deployment, stop resending messages,
	  only 'heartbeats'. How to "back-on"?

fault scenarios:
[?] at source, subscription is continuously restarted, all pendings acks are deleted and a new batch is sent
	  again and again. Can this happen? How to detect?
[?] source have subscription registered, but not target. Target keep replying with NAK, source keeps
    restarting. Instrument with metrics and define alert to detect this scenario.

*/

// to manage internal state of sync.
type state struct {
	deployment gateway.Deployment
	cs         map[gateway.Deployment]CommunicationSettings

	// -- fields for source --
	// outstanding acks
	sourceAckWindows map[SourceSubscriptionKey]*SourcePendingWindow

	// outgoing messages buffered per target deployment (not send yet)
	// Only present if there are messages to be sent for that subscription
	sourceOutgoing map[gateway.Deployment]map[SourceSubscriptionKey][]*v1.Msg

	//InfoRequests map[string]StreamInfoRequest
	//PubRequests  map[string]PublishRequest

	// original source subscription, needed when subscription is restarted
	sourceOriginalSubscription map[SourceSubscriptionKey]SourceSubscription

	// -- fields for target --

	targetCommit map[TargetSubscriptionKey]*TargetCommitWindow

	// target incoming per source deployment
	// Ordered by sequence From. Do not modify, use TargetCommit/TargetCommitReject
	TargetIncoming map[TargetSubscriptionKey][]*v1.Msgs

	// target subscription
	targetSubscription map[TargetSubscriptionKey]TargetSubscription
}

// new state for deployment, settings and initial source sequences
// The deployment may be a source and/or target for streams
// The initialSourceSequences is optional and specify the last sequence number acknowledged for each source stream
func newState(d gateway.Deployment, cs map[gateway.Deployment]CommunicationSettings,
	initialSourceSequences map[SourceSubscriptionKey]uint64) (*state, error) {
	s := &state{
		deployment: d,
		cs:         cs,
		// subscription at either source or target
		sourceOriginalSubscription: make(map[SourceSubscriptionKey]SourceSubscription),
		sourceAckWindows:           make(map[SourceSubscriptionKey]*SourcePendingWindow),
		sourceOutgoing:             make(map[gateway.Deployment]map[SourceSubscriptionKey][]*v1.Msg),
		targetCommit:               make(map[TargetSubscriptionKey]*TargetCommitWindow),
		TargetIncoming:             make(map[TargetSubscriptionKey][]*v1.Msgs),
		targetSubscription:         make(map[TargetSubscriptionKey]TargetSubscription)}

	for sub, seq := range initialSourceSequences {
		s.sourceAckWindows[sub] = &SourcePendingWindow{
			Extrema: RangeInclusive[uint64]{From: seq, To: seq}}
	}

	return s, s.Validate()
}

// register subscription. If this deployment matches the source a LocalSubscriptions entry
// will be present. If this deployment matches the target, messages are expected
// to arrive and will then be published to the (local) target stream.
func (s *state) RegisterSubscription(req *v1.StartSyncRequest) error {
	if req.GetSourceDeployment() == req.GetTargetDeployment() {
		return errors.New("source and target deployment must be different")
	}

	isSource := req.GetSourceDeployment() == s.deployment.String()
	isTarget := req.GetTargetDeployment() == s.deployment.String()

	if isSource {
		sub := toSourceSubscription(
			gateway.Deployment(req.GetTargetDeployment()),
			req.GetSourceStreamName(),
			req.GetConsumerConfig(), req.GetFilterSubjects())

		if _, exists := s.sourceOriginalSubscription[sub.SourceSubscriptionKey]; exists {
			return errors.New("subscription already exists")
		}

		// keep the original subscription
		s.sourceOriginalSubscription[sub.SourceSubscriptionKey] = sub.Clone()

		// check if a initial pending ack window exists
		if w, exists := s.sourceAckWindows[sub.SourceSubscriptionKey]; exists {
			if w.Extrema.From > 0 {
				sub.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
				sub.OptStartSeq = w.Extrema.From
			}
		}
		return nil
	}

	if isTarget {
		key := TargetSubscriptionKey{
			SourceDeployment: gateway.Deployment(req.GetSourceDeployment()),
			SourceStreamName: req.GetSourceStreamName()}
		if _, exists := s.targetSubscription[key]; exists {
			return errors.New("subscription already exists")
		}

		sub := TargetSubscription{
			TargetSubscriptionKey: key,
			TargetDeployment:      gateway.Deployment(req.GetTargetDeployment()),
			DeliverPolicy:         ToDeliverPolicy(req.GetConsumerConfig().GetDeliverPolicy()),
			OptStartSeq:           req.GetConsumerConfig().GetOptStartSeq(),
			FilterSubjects:        req.GetFilterSubjects()}
		if sub.DeliverPolicy == jetstream.DeliverByStartTimePolicy {
			sub.OptStartTime = req.GetConsumerConfig().GetOptStartTime().AsTime()
		}

		s.targetSubscription[key] = sub
		return nil
	}

	return errors.New("neither source nor target")
}

func (s *state) UnregisterSubscription(req *v1.StopSyncRequest) error {
	if req.GetSourceDeployment() == req.GetTargetDeployment() {
		return errors.New("source and target deployment must be different")
	}

	isSource := req.GetSourceDeployment() == s.deployment.String()
	isTarget := req.GetTargetDeployment() == s.deployment.String()

	if isSource {
		key := SourceSubscriptionKey{
			TargetDeployment: gateway.Deployment(req.GetTargetDeployment()),
			SourceStreamName: req.GetSourceStreamName()}

		delete(s.sourceOriginalSubscription, key)
		delete(s.sourceAckWindows, key)
		delete(s.sourceOutgoing[key.TargetDeployment], key)
		if len(s.sourceOutgoing[key.TargetDeployment]) == 0 {
			delete(s.sourceOutgoing, key.TargetDeployment)
		}
		return nil
	}

	if isTarget {
		key := TargetSubscriptionKey{
			SourceDeployment: gateway.Deployment(req.GetSourceDeployment()),
			SourceStreamName: req.GetSourceStreamName()}

		delete(s.targetSubscription, key)
		delete(s.targetCommit, key)
		delete(s.TargetIncoming, key)

		return nil
	}

	return errors.New("neither source nor target")
}

// create batch of messages and acks queued for 'to' deployment. Returns nil if no messages are queued
// This will not delete the messages from the outgoing queue (you must call MarkDispatched)
// You should call MarkDispatched after sending the batch to the target deployment.
// If you call CreateMessageBatch again before a succesful MarkDispatched, the same messages will be included (and more, if some messages have been delivered via SourceDeliverFromLocal)
func (s *state) CreateMessageBatch(now time.Time, to gateway.Deployment) (*v1.MessageBatch, error) {
	m := &v1.MessageBatch{
		ToDeployment:   to.String(),
		FromDeployment: s.deployment.String(),
		SentTimestamp:  timestamppb.New(now.UTC())}

	settings := s.cs[to]

	remainingPayloadCapacity := settings.MaxAccumulatedPayloadSizeBytes
	for key, w := range s.sourceAckWindows {
		if key.TargetDeployment != to {
			continue
		}

		ids := w.GetRetransmit(now, settings.AckTimeoutPrSubscription, settings.AckRetryPrSubscription)
		xs := s.repackMessages(key, ids)

		// add messages to batch. We can't breakup a set of messages, so we must include all messages
		for _, x := range xs {
			m.ListOfMessages = append(m.ListOfMessages, x)
			remainingPayloadCapacity -= getPayloadSize(x)
			if remainingPayloadCapacity <= 0 {
				break
			}
		}

		if !w.ShouldSentHeartbeat(now, settings.HeartbeatIntervalPrSubscription) {
			continue
		}

		sub, exists := s.sourceOriginalSubscription[key]
		if !exists {
			panic("no subscription")
		}

		m.ListOfMessages = append(m.ListOfMessages, &v1.Msgs{
			SetId:            NewSetID().String(),
			SourceDeployment: s.deployment.String(),
			TargetDeployment: to.String(),
			SourceStreamName: key.SourceStreamName,
			LastSequence:     w.Extrema.To,
			ConsumerConfig:   fromSourceSubscription(sub)})
	}

	for key := range s.sourceOutgoing[to] {
		xs := s.packMessages(remainingPayloadCapacity, key)
		for _, x := range xs {
			m.ListOfMessages = append(m.ListOfMessages, x)
			remainingPayloadCapacity -= getPayloadSize(x)
			if remainingPayloadCapacity <= 0 {
				break
			}
		}
	}

	for _, w := range s.targetCommit {
		for _, ack := range w.PendingAcks {
			m.Acknowledges = append(m.Acknowledges, ack)
		}
	}

	if len(m.ListOfMessages) == 0 && len(m.Acknowledges) == 0 {
		return nil, nil
	}
	return m, nil
}

// pending stats for target deployment evaluated at 'now'
// returns [number of messages (including re-transmit), number of pending acks]
func (s *state) PendingStats(now time.Time, to gateway.Deployment) []int {
	var nm, na int

	for key := range s.sourceOutgoing[to] {
		nm += len(s.sourceOutgoing[to][key])
	}

	for _, w := range s.targetCommit {
		na += len(w.PendingAcks)
	}

	for key, w := range s.sourceAckWindows {
		if key.TargetDeployment != to {
			continue
		}

		settings := s.cs[key.TargetDeployment]
		nm += len(w.GetRetransmit(now, settings.AckTimeoutPrSubscription, settings.AckRetryPrSubscription))

		if w.ShouldSentHeartbeat(now, settings.HeartbeatIntervalPrSubscription) {
			nm++
		}
	}

	return []int{nm, na}
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

	// messages
	msgsErrs := make(map[SourceSubscriptionKey]error)
	for _, msgs := range b.ListOfMessages {
		key := SourceSubscriptionKey{
			TargetDeployment: d,
			SourceStreamName: msgs.GetSourceStreamName()}

		if _, exists := s.sourceAckWindows[key]; !exists {
			s.sourceAckWindows[key] = &SourcePendingWindow{}
		}
		w := s.sourceAckWindows[key]

		ack := SourcePendingAck{
			SetID:         SetID(msgs.GetSetId()),
			SentTimestamp: b.GetSentTimestamp().AsTime(),
			SequenceRange: RangeInclusive[uint64]{
				From: msgs.GetLastSequence(),
				To:   msgs.GetLastSequence()},
			Messages: msgs.GetMessages()}

		if len(msgs.GetMessages()) > 0 {
			ack.SequenceRange.To = msgs.GetMessages()[len(msgs.GetMessages())-1].GetSequence()
		}

		err := w.MarkDispatched(ack)
		if err != nil {
			msgsErrs[key] = errors.Wrap(err, "failed to mark dispatched")
		}

		// only remove messages included in the batch
		xs := s.sourceOutgoing[d][key]
		idx, found := findSequenceIndex(xs, ack.SequenceRange.To)
		if found && (idx+1) < len(xs) {
			s.sourceOutgoing[d][key] = xs[idx+1:]
		} else {
			s.sourceOutgoing[d][key] = nil
		}
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

		delete(w.PendingAcks, SetID(ack.GetSetId()))
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
type SetID string

func NewSetID() SetID {
	return SetID(uuid.New().String())
}

func (id SetID) String() string {
	return string(id)
}

func (s *state) Validate() error {
	if _, exists := s.cs[s.deployment]; exists {
		return errors.New("must only have communication settings for targets, not self")
	}

	if len(s.cs) == 0 {
		return errors.New("communication settings empty")
	}

	for d, cs := range s.cs {
		if ve := cs.Validate(); ve != nil {
			return errors.Wrapf(ve, "invalid communication settings for deployment %s", d)
		}
	}
	return nil
}

func getPayloadSize(m *v1.Msgs) int {
	size := 0
	for _, x := range m.Messages {
		size += len(x.Data)
	}
	return size
}
