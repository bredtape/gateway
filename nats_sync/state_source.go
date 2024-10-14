package nats_sync

import (
	"slices"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// deliver local messages from requested subscriptions (to be sent to remote)
// Messages must be in order.
// The lastSequence must match the last message in the previous batch,
// or 0 if the subscription has been restarted
// If more messages are attempted to be delivered than outgoing and pending,
// an error of ErrBackoff is returned. Messages may be partial delivered, indicated
// by the returned count!
func (s *state) SourceDeliverFromLocal(key SourceSubscriptionKey, lastSequence uint64, messages ...*v1.Msg) (int, error) {
	_, exists := s.SourceLocalSubscriptions[key]
	if !exists {
		return 0, errors.Wrapf(ErrNoSubscription, "no local subscription for key %v", key)
	}

	settings := s.cs[key.TargetDeployment]
	w, exists := s.sourceAckWindows[key]

	// backoff
	acceptCount := s.sourceCalculateAcceptCount(key, lastSequence == 0)
	if acceptCount == 0 {
		return 0, errors.Wrapf(ErrBackoff, "max pending acks (%d) reached, only accepting %d", settings.MaxPendingAcksPrSubscription, acceptCount)
	}
	if acceptCount > len(messages) {
		acceptCount = len(messages)
	}

	// no ack window
	if !exists && lastSequence != 0 {
		pendingMessages := s.sourceOutgoing[key.TargetDeployment][key]

		// lastSequence must match the last pending message
		if len(pendingMessages) > 0 {
			seq := pendingMessages[len(pendingMessages)-1].GetSequence()
			if seq != lastSequence {
				return 0, errors.Wrapf(ErrSourceSequenceBroken, "last sequence %d does not match last message %d", lastSequence, seq)
			}
		} else {
			return 0, errors.Wrap(ErrSourceSequenceBroken, "no ack window exists, first ack must have sequence From 0")
		}
	}

	if exists && w.Extrema.To != lastSequence {
		return 0, errors.Wrapf(ErrSourceSequenceBroken, "pending window %s does not match last sequence %d", w.Extrema.String(), lastSequence)
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

	if len(messages) > 0 && messages[0].Sequence <= lastSequence {
		return 0, errors.Wrapf(ErrSourceSequenceBroken, "message sequence %d must be after lastSequence %d", messages[0].Sequence, lastSequence)
	}

	// only checks before this point

	// ack window reset
	if exists && lastSequence == 0 {
		delete(s.sourceAckWindows, key)
	}

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

func (s *state) packMessages(key SourceSubscriptionKey) *v1.Msgs {
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
		TargetDeployment: key.TargetDeployment.String(),
		SourceStreamName: key.SourceStreamName,
		FilterSubjects:   sub.FilterSubjects,
		ConsumerConfig:   fromSourceSubscription(sub),
		Messages:         messages}

	if w, exists := s.sourceAckWindows[key]; exists {
		b.LastSequence = w.Extrema.To
	}

	return b
}

// handle incoming ack from target deployment
// ack may be out or order (so the sequence from/to are out of order)
func (s *state) SourceHandleTargetAck(currentTime, sentTime time.Time, targetDeployment gateway.Deployment, ack *v1.Acknowledge) error {
	key := SourceSubscriptionKey{
		TargetDeployment: targetDeployment,
		SourceStreamName: ack.GetSourceStreamName()}

	sub, exists := s.sourceOriginalSubscription[key]
	if !exists {
		return errors.Wrapf(ErrNoSubscription, "no subscription for key %v", key)
	}

	if ack.GetIsNak() {
		return errors.New("not implemented")
	}

	w, exists := s.sourceAckWindows[key]
	if !exists {
		// no outstanding acks, ignore
		return nil
	}

	p := SourcePendingAck{
		SetID:         SetID(ack.GetSetId()),
		SentTimestamp: sentTime,
		SequenceRange: RangeInclusive[uint64]{
			From: ack.GetSequenceFrom(),
			To:   ack.GetSequenceTo()},
		// Count not specified,
	}

	// only checks before this point

	changes, err := w.ReceiveAck(p)
	if err != nil {
		return errors.Wrap(err, "failed to process ack")
	}

	if changes {
		s.SourceLocalSubscriptions[key] = SourceSubscription{
			SourceSubscriptionKey: key,
			DeliverPolicy:         jetstream.DeliverByStartSequencePolicy,
			OptStartSeq:           w.Extrema.From,
			FilterSubjects:        sub.FilterSubjects}
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

func toSourceSubscription(targetDeployment gateway.Deployment, sourceStream string, s *v1.ConsumerConfig, filterSubjects []string) SourceSubscription {
	sub := SourceSubscription{
		SourceSubscriptionKey: SourceSubscriptionKey{
			TargetDeployment: targetDeployment,
			SourceStreamName: sourceStream},
		DeliverPolicy:  ToDeliverPolicy(s.GetDeliverPolicy()),
		OptStartSeq:    s.GetOptStartSeq(),
		FilterSubjects: filterSubjects}

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

	pendingMessages := s.sourceOutgoing[key.TargetDeployment][key]

	count := settings.MaxPendingAcksPrSubscription
	if !isReset {
		count -= len(pendingMessages)
		count -= pendingAckCount
	}

	if count < 0 {
		count = 0
	}

	return count
}
