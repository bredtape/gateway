package nats_sync

import (
	"cmp"
	"slices"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// requested local subscription (may be different from original subscription)
func (s *state) GetSourceLocalSubscriptions(key SourceSubscriptionKey) (SourceSubscription, bool) {
	sub, exists := s.sourceOriginalSubscription[key]
	if !exists {
		return SourceSubscription{}, false
	}

	w := s.sourceAckWindows[key]

	if w != nil && w.Extrema.To > 0 {
		return SourceSubscription{
			SourceSubscriptionKey: key,
			DeliverPolicy:         jetstream.DeliverByStartSequencePolicy,
			OptStartSeq:           w.Extrema.To,
			FilterSubjects:        sub.FilterSubjects}, true
	}

	return sub.Clone(), true
}

// deliver local messages from requested subscriptions (to be sent to remote)
// Messages must be in order.
// The lastSequence must match the last message in the previous batch,
// or 0 if the subscription has been restarted. If messages are out of order,
// a ErrSourceSequenceBroken is returned (and the subscription must be restarted)
// If more messages are attempted to be delivered than outgoing and pending,
// an error of ErrBackoff is returned. Messages may be partial delivered, indicated
// by the returned count!
func (s *state) SourceDeliverFromLocal(key SourceSubscriptionKey, lastSequence uint64, messages ...*v1.Msg) (int, error) {
	if _, exists := s.sourceOriginalSubscription[key]; !exists {
		return 0, errors.Wrapf(ErrNoSubscription, "no local subscription for key %v", key)
	}

	// backoff
	settings := s.cs[key.TargetDeployment]
	acceptCount := s.sourceCalculateAcceptCount(key, lastSequence == 0)
	if acceptCount == 0 {
		return 0, errors.Wrapf(ErrBackoff, "max pending acks (%d) reached, only accepting %d", settings.MaxPendingAcksPrSubscription, acceptCount)
	}
	if acceptCount > len(messages) {
		acceptCount = len(messages)
	}

	w, exists := s.sourceAckWindows[key]
	if !exists {
		w = &SourcePendingWindow{}
	}

	if lastSequence > 0 {
		pendingMessages := s.sourceOutgoing[key.TargetDeployment][key]

		// lastSequence must match the last pending message
		if len(pendingMessages) > 0 {
			seq := pendingMessages[len(pendingMessages)-1].GetSequence()
			if seq != lastSequence {
				return 0, errors.Wrapf(ErrSourceSequenceBroken, "last sequence %d does not match last message %d", lastSequence, seq)
			}
		}

		if w.Extrema.To != lastSequence {
			return 0, errors.Wrapf(ErrSourceSequenceBroken, "pending window %s does not match last sequence %d", w.Extrema.String(), lastSequence)
		}
	}

	// when lastSequence > 0:
	// * if some outgoing, last sequence must match last message
	// * otherwise it must match pending window Extrema.To

	if lastSequence > 0 {
		xs := s.sourceOutgoing[key.TargetDeployment][key]
		if len(xs) > 0 {
			lastMessageSeq := xs[len(xs)-1].GetSequence()
			if lastMessageSeq != lastSequence {
				return 0, errors.Wrapf(ErrSourceSequenceBroken, "last sequence %d does not match last message %d", lastSequence, lastMessageSeq)
			}
		} else {
			if w, exists := s.sourceAckWindows[key]; exists {
				if w.Extrema.To != lastSequence {
					return 0, errors.Wrapf(ErrSourceSequenceBroken, "pending window extrema %s does not match last sequence %d", w.Extrema.String(), lastSequence)
				}
			}
		}
	}

	if !slices.IsSortedFunc(messages, cmpMsgSequence) {
		return 0, errors.New("messages must be in order")
	}

	for _, m := range messages {
		if len(m.Data) > settings.MaxAccumulatedPayloadSizeBytes {
			return 0, errors.Wrapf(ErrPayloadTooLarge, "message sequence %d", m.GetSequence())
		}
	}

	if len(messages) > 0 && messages[0].Sequence <= lastSequence {
		return 0, errors.Wrapf(ErrSourceSequenceBroken, "message sequence %d must be after lastSequence %d", messages[0].Sequence, lastSequence)
	}

	// only checks before this point

	if !exists {
		s.sourceAckWindows[key] = w
	}

	// ack window reset, ignore all pending and acknowledged
	if lastSequence == 0 {
		w.Pending = nil
		w.Acknowledged = nil
		w.Extrema.From = 0
	}

	w.Extrema.To = messages[acceptCount-1].GetSequence()

	// prune existing messages if subscription was restarted
	if _, exists := s.sourceOutgoing[key.TargetDeployment]; !exists || lastSequence == 0 {
		s.sourceOutgoing[key.TargetDeployment] = make(map[SourceSubscriptionKey][]*v1.Msg)
	}

	s.sourceOutgoing[key.TargetDeployment][key] =
		append(s.sourceOutgoing[key.TargetDeployment][key], messages[:acceptCount]...)

	if acceptCount < len(messages) {
		return acceptCount, errors.Wrapf(ErrBackoff, "max pending acks (%d) reached, only accepting %d", settings.MaxPendingAcksPrSubscription, acceptCount)
	}
	return acceptCount, nil
}

func (s *state) packMessages(payloadCapacity int, key SourceSubscriptionKey) []*v1.Msgs {
	messages, exists := s.sourceOutgoing[key.TargetDeployment][key]
	if !exists {
		return nil
	}

	sub, exists := s.sourceOriginalSubscription[key]
	if !exists {
		panic("no subscription")
	}

	b := &v1.Msgs{
		SetId:            NewSetID().String(),
		SourceDeployment: s.deployment.String(),
		TargetDeployment: key.TargetDeployment.String(),
		SourceStreamName: key.SourceStreamName,
		FilterSubjects:   sub.FilterSubjects,
		ConsumerConfig:   fromSourceSubscription(sub),
		Messages:         make([]*v1.Msg, 0, min(len(messages), payloadCapacity))}

	for _, m := range messages {
		payloadCapacity -= len(m.Data)
		if payloadCapacity < 0 {
			break
		}
		b.Messages = append(b.Messages, m)
	}

	if w, exists := s.sourceAckWindows[key]; exists {
		b.LastSequence = w.Extrema.To
	}

	if len(b.Messages) == 0 {
		return nil
	}

	return []*v1.Msgs{b}
}

// repack messages, if pending acknowledgement has timed out
func (s *state) repackMessages(key SourceSubscriptionKey, ids []SetID) []*v1.Msgs {
	w, exists := s.sourceAckWindows[key]
	if !exists {
		return nil
	}

	var result []*v1.Msgs
	for _, id := range ids {
		pending, exists := w.Pending[id]
		if !exists {
			panic("no pending")
		}

		result = append(result, &v1.Msgs{
			SetId:            pending.SetID.String(),
			TargetDeployment: key.TargetDeployment.String(),
			SourceStreamName: key.SourceStreamName,
			ConsumerConfig:   fromSourceSubscription(s.sourceOriginalSubscription[key]),
			LastSequence:     pending.SequenceRange.From,
			Messages:         pending.Messages})
	}
	return result
}

// handle incoming ack from target deployment
// ack may be out or order (so the sequence from/to are out of order)
func (s *state) SourceHandleTargetAck(currentTime, sentTime time.Time, targetDeployment gateway.Deployment, ack *v1.Acknowledge) error {
	key := SourceSubscriptionKey{
		TargetDeployment: targetDeployment,
		SourceStreamName: ack.GetSourceStreamName()}

	_, exists := s.sourceOriginalSubscription[key]
	if !exists {
		return errors.Wrapf(ErrNoSubscription, "no subscription for key %v", key)
	}

	w, exists := s.sourceAckWindows[key]
	if !exists {
		// no outstanding acks, ignore
		return nil
	}

	p := SourcePendingAck{
		SetID:         SetID(ack.GetSetId()),
		IsNAK:         ack.GetIsNak(),
		SentTimestamp: sentTime,
		SequenceRange: RangeInclusive[uint64]{
			From: ack.GetSequenceFrom(),
			To:   ack.GetSequenceTo()},
		// Messages not specified
	}

	// only checks before this point

	err := w.ReceiveAck(currentTime, p)
	if err != nil {
		return errors.Wrap(err, "failed to process ack")
	}

	// if nak, delete pending messages
	if p.IsNAK {
		// keep messages from Extrema.To, if found
		if w.Extrema.To > 0 {
			keepers := keepMessagesFrom(s.sourceOutgoing[key.TargetDeployment][key], w.Extrema.To)
			if len(keepers) > 0 {
				w.Extrema.To = keepers[len(keepers)-1].GetSequence()
			}
			s.sourceOutgoing[key.TargetDeployment][key] = keepers
		} else {
			delete(s.sourceOutgoing[key.TargetDeployment], key)
		}
	}

	return nil
}

// description at source nats stream
type SourceSubscription struct {
	SourceSubscriptionKey

	DeliverPolicy  jetstream.DeliverPolicy
	OptStartSeq    uint64
	OptStartTime   time.Time
	FilterSubjects []string
}

func (s SourceSubscription) Clone() SourceSubscription {
	return SourceSubscription{
		SourceSubscriptionKey: s.SourceSubscriptionKey,
		DeliverPolicy:         s.DeliverPolicy,
		OptStartSeq:           s.OptStartSeq,
		OptStartTime:          s.OptStartTime,
		FilterSubjects:        slices.Clone(s.FilterSubjects)}
}

func toSourceSubscription(targetDeployment gateway.Deployment, sourceStream string, s *v1.ConsumerConfig, filterSubjects []string) SourceSubscription {
	sub := SourceSubscription{
		SourceSubscriptionKey: SourceSubscriptionKey{
			TargetDeployment: targetDeployment,
			SourceStreamName: sourceStream},
		DeliverPolicy:  ToDeliverPolicy(s.GetDeliverPolicy()),
		OptStartSeq:    s.GetOptStartSeq(),
		FilterSubjects: filterSubjects}
	slices.Sort(sub.FilterSubjects)

	if sub.DeliverPolicy == jetstream.DeliverByStartTimePolicy {
		sub.OptStartTime = s.GetOptStartTime().AsTime()
	}
	return sub
}

func fromSourceSubscription(sub SourceSubscription) *v1.ConsumerConfig {
	c := &v1.ConsumerConfig{
		DeliverPolicy: FromDeliverPolicy(sub.DeliverPolicy),
		OptStartSeq:   sub.OptStartSeq}
	if sub.DeliverPolicy == jetstream.DeliverByStartTimePolicy {
		c.OptStartTime = timestamppb.New(sub.OptStartTime.UTC())
	}
	return c
}

func (s *state) sourceCalculateAcceptCount(key SourceSubscriptionKey, isReset bool) int {
	settings := s.cs[key.TargetDeployment]

	var pendingAckCount int
	if w, exists := s.sourceAckWindows[key]; exists {
		for _, v := range w.Pending {
			pendingAckCount += len(v.Messages)
		}
	}

	count := settings.MaxPendingAcksPrSubscription
	if !isReset {
		count -= len(s.sourceOutgoing[key.TargetDeployment][key])
		count -= pendingAckCount
	}

	return count
}

// keep messages from the sequence number
// The sequence number must be in the list
func keepMessagesFrom(messages []*v1.Msg, from uint64) []*v1.Msg {
	idx, found := findSequenceIndex(messages, from)
	if !found {
		return nil
	}

	return messages[idx:]
}

func findSequenceIndex(messages []*v1.Msg, seq uint64) (int, bool) {
	idx, found := slices.BinarySearchFunc(messages, seq, func(msg *v1.Msg, x uint64) int {
		return cmp.Compare(msg.GetSequence(), x)
	})
	return idx, found
}
