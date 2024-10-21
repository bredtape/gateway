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
	// slow consumer error, when the sink has too many messages queued
	ErrSlowConsumer         = errors.New("slow consumer")
	ErrSourceSequenceBroken = errors.New("source sequence broken. Subscription must be restarted")
	ErrBackoff              = errors.New("cannot accept new messages, pending messages must be dispatched first")
	ErrPayloadTooLarge      = errors.New("payload too large")
)

/* TODO:
guarantee that messages are delivered in order
[x] at sink, send nak if Msgs is rejected (because it is out of sequence with sink)
[x] at source, handle nak by restarting subscription
[x] at source, do not restart subscription again if a nak is received and ack is pending for a Msgs which
	  contains the requested sequence range.
[-] at sink, do not accept batches with overlapping sequences (because it is impossible to keep 'Count')
[-] at source, do not accept ack of batches with overlapping sequences (because it is impossible to keep 'Count')
[x] initial source window start sequence (which could be persisted to improve startup)

backpressure:
[x] at source, do not accept new messages if there are too many pending messages (MaxPendingAcksPrSubscription)
[x] at source, accept a partial batch of messages if limit has not been reached, and then apply backpressure
[x] at sink, do not accept incoming messages if there are too many pending messages (MaxPendingIncomingMessagesPrSubscription)

ack timeout:
[x] at source, if ack is not received within AckTimeout, resend Msgs again (with the same SetID)

heartbeat:
[x] at source, add empty Msgs to the batch if no activity for a subscription within HeartbeatIntervalPrSubscription
	  No activity means:
		* no messages have been sent
		* no messages have been resent
		* no pending acks
		* but some messages must have been sent before (to initialize the window)
[x] at sink, accept empty Msgs and send Acknowledge

max size:
[x] at source, do not send batches that exceed MaxAccumulatedPayloadSizeBytes

flush:
[ ] at source, with pending messages/acks pr sink, indicate the earliest time messages/acks should be sent to the sink

short circuit:
[ ] at source, if no acknowledgements are received for all streams per deployment, stop resending messages,
	  only 'heartbeats'. How to "back-on"?

fault scenarios:
[?] at source, subscription is continuously restarted, all pendings acks are deleted and a new batch is sent
	  again and again. Can this happen? How to detect?
[?] source have subscription registered, but not sink. Sink keep replying with NAK, source keeps
    restarting. Instrument with metrics and define alert to detect this scenario.

*/

// to manage internal state of sync.
type state struct {
	from gateway.Deployment // self, the source or sink deployment
	to   gateway.Deployment // other communicating part, the sink or source deployment

	// communication settings for 'b' deployment
	cs CommunicationSettings

	// -- fields for source --
	// outstanding acks
	sourceAckWindows map[SourceSubscriptionKey]*SourcePendingWindow

	// outgoing messages buffered per sink deployment (not send yet)
	// Only present if there are messages to be sent for that subscription
	sourceOutgoing map[SourceSubscriptionKey][]*v1.Msg

	//InfoRequests map[string]StreamInfoRequest
	//PubRequests  map[string]PublishRequest

	// original source subscription, needed when subscription is restarted
	sourceOriginalSubscription map[SourceSubscriptionKey]SourceSubscription

	// -- fields for sink --

	sinkCommit map[SinkSubscriptionKey]*SinkCommitWindow

	// sink incoming per source deployment
	// Ordered by sequence From. Do not modify, use SinkCommit/SinkCommitReject
	SinkIncoming map[SinkSubscriptionKey][]*v1.Msgs

	// sink subscription
	sinkSubscription map[SinkSubscriptionKey]SinkSubscription
}

// new state for deployment 'from' communicating with 'to', where 'from' sources messages to sink 'to',
// and 'to' sources messages to sink 'from'.
// The initialSourceSequences is optional and specify the last sequence number acknowledged for each source stream
func newState(from, to gateway.Deployment, cs CommunicationSettings,
	initialSourceSequences map[SourceSubscriptionKey]SourceSequence) (*state, error) {
	if from == to {
		return nil, errors.New("from and to must be different")
	}
	s := &state{
		from: from,
		to:   to,
		cs:   cs,
		// subscription at either source or sink
		sourceOriginalSubscription: make(map[SourceSubscriptionKey]SourceSubscription),
		sourceAckWindows:           make(map[SourceSubscriptionKey]*SourcePendingWindow),
		sourceOutgoing:             make(map[SourceSubscriptionKey][]*v1.Msg),
		sinkCommit:                 make(map[SinkSubscriptionKey]*SinkCommitWindow),
		SinkIncoming:               make(map[SinkSubscriptionKey][]*v1.Msgs),
		sinkSubscription:           make(map[SinkSubscriptionKey]SinkSubscription)}

	for sub, seq := range initialSourceSequences {
		s.sourceAckWindows[sub] = &SourcePendingWindow{
			Extrema: RangeInclusive[SourceSequence]{From: seq, To: seq}}
	}

	return s, s.Validate()
}

func (s *state) HasSubscriptions() bool {
	return len(s.sourceOriginalSubscription) > 0 || len(s.sinkSubscription) > 0
}

// start sync. If this deployment matches the source a LocalSubscriptions entry
// will be present. If this deployment matches the sink, messages are expected
// to arrive and will then be published to the (local) sink stream.
func (s *state) RegisterStartSync(req *v1.StartSyncRequest) error {
	from := gateway.Deployment(req.GetSourceDeployment())
	to := gateway.Deployment(req.GetSinkDeployment())

	if err := s.requestMatch(from, to); err != nil {
		return err
	}

	isSource := from == s.from
	isSink := to == s.from

	if isSource {
		sub := toSourceSubscription(
			to,
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
				sub.OptStartSeq = uint64(w.Extrema.From)
			}
		}
		return nil
	}

	if isSink {
		key := SinkSubscriptionKey{SourceStreamName: req.GetSourceStreamName()}
		if _, exists := s.sinkSubscription[key]; exists {
			return errors.New("subscription already exists")
		}

		sub := SinkSubscription{
			SinkSubscriptionKey: key,
			SinkDeployment:      to,
			DeliverPolicy:       ToDeliverPolicy(req.GetConsumerConfig().GetDeliverPolicy()),
			OptStartSeq:         req.GetConsumerConfig().GetOptStartSeq(),
			FilterSubjects:      req.GetFilterSubjects()}
		if sub.DeliverPolicy == jetstream.DeliverByStartTimePolicy {
			sub.OptStartTime = req.GetConsumerConfig().GetOptStartTime().AsTime()
		}

		s.sinkSubscription[key] = sub
		return nil
	}

	return errors.New("neither source nor sink")
}

// stop sync
func (s *state) RegisterStopSync(req *v1.StopSyncRequest) error {
	source := gateway.Deployment(req.GetSourceDeployment())
	sink := gateway.Deployment(req.GetSinkDeployment())

	if err := s.requestMatch(source, sink); err != nil {
		return err
	}

	isSource := source == s.from
	isSink := sink == s.from

	if isSource {
		key := SourceSubscriptionKey{SourceStreamName: req.GetSourceStreamName()}
		delete(s.sourceOriginalSubscription, key)
		delete(s.sourceAckWindows, key)
		delete(s.sourceOutgoing, key)
		return nil
	}

	if isSink {
		key := SinkSubscriptionKey{SourceStreamName: req.GetSourceStreamName()}
		delete(s.sinkSubscription, key)
		delete(s.sinkCommit, key)
		delete(s.SinkIncoming, key)
		return nil
	}

	return errors.New("neither source nor sink")
}

// create batch of messages and acks queued for 'to' deployment. Returns nil if no messages are queued
// This will not delete the messages from the outgoing queue (you must call MarkDispatched)
// You should call MarkDispatched after sending the batch to the sink deployment.
// If you call CreateMessageBatch again before a succesful MarkDispatched, the same messages will be included (and more, if some messages have been delivered via SourceDeliverFromLocal)
func (s *state) CreateMessageBatch(now time.Time) (*v1.MessageBatch, error) {
	m := &v1.MessageBatch{
		FromDeployment: s.from.String(),
		ToDeployment:   s.to.String(),
		SentTimestamp:  timestamppb.New(now.UTC())}

	remainingPayloadCapacity := s.cs.MaxAccumulatedPayloadSizeBytes
	for key, w := range s.sourceAckWindows {
		ids := w.GetRetransmit(now, s.cs.AckTimeoutPrSubscription, s.cs.AckRetryPrSubscription)
		xs := s.repackMessages(key, ids)

		// add messages to batch. We can't breakup a set of messages, so we must include all messages
		for _, x := range xs {
			m.ListOfMessages = append(m.ListOfMessages, x)
			remainingPayloadCapacity -= getPayloadSize(x)
			if remainingPayloadCapacity <= 0 {
				break
			}
		}

		if !w.ShouldSentHeartbeat(now, s.cs.HeartbeatIntervalPrSubscription) {
			continue
		}

		sub, exists := s.sourceOriginalSubscription[key]
		if !exists {
			panic("no subscription")
		}

		m.ListOfMessages = append(m.ListOfMessages, &v1.Msgs{
			SetId:            NewSetID().String(),
			SourceDeployment: s.from.String(),
			SinkDeployment:   s.to.String(),
			SourceStreamName: key.SourceStreamName,
			LastSequence:     uint64(w.Extrema.To),
			ConsumerConfig:   fromSourceSubscription(sub)})
	}

	for key := range s.sourceOutgoing {
		xs := s.packMessages(remainingPayloadCapacity, key)
		for _, x := range xs {
			m.ListOfMessages = append(m.ListOfMessages, x)
			remainingPayloadCapacity -= getPayloadSize(x)
			if remainingPayloadCapacity <= 0 {
				break
			}
		}
	}

	for _, w := range s.sinkCommit {
		for _, ack := range w.PendingAcks {
			m.Acknowledges = append(m.Acknowledges, ack)
		}
	}

	if len(m.ListOfMessages) == 0 && len(m.Acknowledges) == 0 {
		return nil, nil
	}
	return m, nil
}

// pending stats for 'to' deployment evaluated at 'now'
// returns [number of messages (including re-transmit), number of pending acks]
func (s *state) PendingStats(now time.Time) []int {
	var nm, na int

	for key := range s.sourceOutgoing {
		nm += len(s.sourceOutgoing[key])
	}

	for _, w := range s.sinkCommit {
		na += len(w.PendingAcks)
	}

	for _, w := range s.sourceAckWindows {
		nm += len(w.GetRetransmit(now, s.cs.AckTimeoutPrSubscription, s.cs.AckRetryPrSubscription))

		if w.ShouldSentHeartbeat(now, s.cs.HeartbeatIntervalPrSubscription) {
			nm++
		}
	}

	return []int{nm, na}
}

type DispatchReport struct {
	MessagesErrors map[SourceSubscriptionKey]error
	AcksErrors     map[SinkSubscriptionKey]error
}

func (r DispatchReport) IsEmpty() bool {
	return len(r.MessagesErrors) == 0 && len(r.AcksErrors) == 0
}

// mark batch as dispatched to sink deployment.
// A (partial) error may be returned per Subscription, meaning messages for other
// subscription may have succeeded.
func (s *state) MarkDispatched(b *v1.MessageBatch) DispatchReport {
	if b == nil {
		return DispatchReport{}
	}

	// messages
	msgsErrs := make(map[SourceSubscriptionKey]error)
	for _, msgs := range b.ListOfMessages {
		key := SourceSubscriptionKey{SourceStreamName: msgs.GetSourceStreamName()}

		if _, exists := s.sourceAckWindows[key]; !exists {
			s.sourceAckWindows[key] = &SourcePendingWindow{}
		}
		w := s.sourceAckWindows[key]

		ack := SourcePendingAck{
			SetID:         SetID(msgs.GetSetId()),
			SentTimestamp: b.GetSentTimestamp().AsTime(),
			SequenceRange: RangeInclusive[SourceSequence]{
				From: SourceSequence(msgs.GetLastSequence()),
				To:   SourceSequence(msgs.GetLastSequence())},
			Messages: msgs.GetMessages()}

		if len(msgs.GetMessages()) > 0 {
			ack.SequenceRange.To = SourceSequence(msgs.GetMessages()[len(msgs.GetMessages())-1].GetSequence())
		}

		err := w.MarkDispatched(ack)
		if err != nil {
			msgsErrs[key] = errors.Wrap(err, "failed to mark dispatched")
		}

		// only remove messages included in the batch
		xs := s.sourceOutgoing[key]
		idx, found := findSequenceIndex(xs, uint64(ack.SequenceRange.To))
		if found && (idx+1) < len(xs) {
			s.sourceOutgoing[key] = xs[idx+1:]
		} else {
			s.sourceOutgoing[key] = nil
		}
	}

	// acknowledges
	ackErrs := make(map[SinkSubscriptionKey]error)
	for _, ack := range b.Acknowledges {
		key := SinkSubscriptionKey{SourceStreamName: ack.GetSourceStreamName()}

		w, exists := s.sinkCommit[key]
		if !exists {
			ackErrs[key] = fmt.Errorf("no sink commits with key %s, have %v", key, s.sinkCommit)
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
	SinkStreamName   string

	// sequence of the last message published to the sink stream. 0 if no messages have been published
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
	SourceStreamName string
}

func GetSubscriptionKey(b *v1.Msgs) SourceSubscriptionKey {
	return SourceSubscriptionKey{
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
	if ve := s.cs.Validate(); ve != nil {
		return errors.Wrap(ve, "invalid communication settings")
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

func (s *state) requestMatch(source, sink gateway.Deployment) error {
	if source == sink {
		return errors.New("source and sink deployment must be different")
	}
	if source != s.from && source != s.to {
		return errors.New("source not matching this state")
	}
	if sink != s.from && sink != s.to {
		return errors.New("sink not matching this state")
	}
	return nil
}
