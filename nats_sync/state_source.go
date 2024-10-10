package nats_sync

import (
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

// deliver local messages from requested subscriptions (to be sent to remote)
// Messages must be in order.
// The lastSequence must match the last message in the previous batch,
// or 0 if the subscription has been restarted
func (s *state) SourceDeliverFromLocal(key SourceSubscriptionKey, lastSequence uint64, messages ...*v1.Msg) error {
	_, exists := s.SourceLocalSubscriptions[key]
	if !exists {
		return errors.Wrapf(ErrNoSubscription, "no local subscription for key %v", key)
	}

	w, exists := s.sourceAckWindows[key]
	// no ack window, first ack must have sequence from 0
	if !exists && lastSequence != 0 {
		return errors.Wrap(ErrSourceSequenceBroken, "no ack window exists, first ack must have sequence From 0")
	}

	// ack window reset
	if exists && lastSequence == 0 {
		delete(s.sourceAckWindows, key)
	}

	if exists && w.Extrema.To != lastSequence {
		return errors.Wrapf(ErrSourceSequenceBroken, "pending window %s does not match last sequence %d", w.Extrema.String(), lastSequence)
	}

	// when lastSequence > 0:
	// * if some outgoing, last sequence must match last message
	// * otherwise it must match pending window Extrema.To

	if lastSequence > 0 {
		xs := s.sourceOutgoing[key.TargetDeployment][key]
		if len(xs) > 0 {
			lastMessageSeq := xs[len(xs)-1].GetSequence()
			if lastMessageSeq != lastSequence {
				return errors.Wrapf(ErrSourceSequenceBroken, "last sequence %d does not match last message %d", lastSequence, lastMessageSeq)
			}
		} else {
			if w, exists := s.sourceAckWindows[key]; exists {
				if w.Extrema.To != lastSequence {
					return errors.Wrapf(ErrSourceSequenceBroken, "pending window extrema %s does not match last sequence %d", w.Extrema.String(), lastSequence)
				}
			}
		}
	}

	seq := lastSequence
	for _, m := range messages {
		if seq >= m.GetSequence() {
			return errors.Wrapf(ErrSourceSequenceBroken,
				"message sequence %d must be in order", m.GetSequence())
		}

		if _, exists := s.sourceOutgoing[key.TargetDeployment]; !exists {
			s.sourceOutgoing[key.TargetDeployment] = make(map[SourceSubscriptionKey][]*v1.Msg)
		}
		s.sourceOutgoing[key.TargetDeployment][key] = append(s.sourceOutgoing[key.TargetDeployment][key], m)

		seq = m.GetSequence()
	}

	return nil
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
		SetId:            NewSetID().GetBytes(),
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

	setID, err := NewSetIDFromBytes(ack.GetSetId())
	if err != nil {
		return errors.Wrap(err, "failed to parse batch id")
	}

	p := SourcePendingAck{
		SetID:         setID,
		SentTimestamp: sentTime,
		SequenceRange: RangeInclusive[uint64]{
			From: ack.GetSequenceFrom(),
			To:   ack.GetSequenceTo()}}
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
	return SourceSubscription{
		SourceSubscriptionKey: SourceSubscriptionKey{
			TargetDeployment: targetDeployment,
			SourceStreamName: sourceStream},
		DeliverPolicy:  ToDeliverPolicy(s.GetDeliverPolicy()),
		OptStartSeq:    s.GetOptStartSeq(),
		OptStartTime:   fromUnixTime(s.GetOptStartTime()),
		FilterSubjects: filterSubjects}
}

func fromSourceSubscription(sub SourceSubscription) *v1.ConsumerConfig {
	return &v1.ConsumerConfig{
		DeliverPolicy: FromDeliverPolicy(sub.DeliverPolicy),
		OptStartSeq:   sub.OptStartSeq,
		OptStartTime:  toUnixTime(sub.OptStartTime)}
}
