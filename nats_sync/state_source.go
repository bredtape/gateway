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
func (s *state) SourceDeliverFromLocal(key SubscriptionKey, lastSequence uint64, messages ...*v1.Msg) error {
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
			lastMessageSeq := xs[len(xs)-1].GetSourceSequence()
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
		if seq >= m.GetSourceSequence() {
			return errors.Wrapf(ErrSourceSequenceBroken, "message sequence %d must be in order", m.GetSourceSequence())
		}

		if _, exists := s.sourceOutgoing[key.TargetDeployment]; !exists {
			s.sourceOutgoing[key.TargetDeployment] = make(map[SubscriptionKey][]*v1.Msg)
		}
		s.sourceOutgoing[key.TargetDeployment][key] = append(s.sourceOutgoing[key.TargetDeployment][key], m)

		seq = m.GetSourceSequence()
	}

	return nil
}

// create batch of messages queued for 'to' deployment. Returns nil if no messages are queued
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

	if len(m.ListOfMessages) == 0 {
		return nil, nil
	}
	return m, nil
}

func (s *state) packMessages(key SubscriptionKey) *v1.Msgs {
	messages, exists := s.sourceOutgoing[key.TargetDeployment][key]
	if !exists {
		return nil
	}

	sub, exists := s.subscription[key]
	if !exists {
		panic("no subscription")
	}

	b := &v1.Msgs{
		SetId:            NewSetID().GetBytes(),
		TargetDeployment: key.TargetDeployment.String(),
		SourceStreamName: key.SourceStreamName,
		FilterSubjects:   sub.FilterSubjects,
		ConsumerConfig:   fromSourceSubscription(sub),
		SequenceTo:       messages[len(messages)-1].GetSourceSequence(),
		Messages:         messages}

	if w, exists := s.sourceAckWindows[key]; !exists {
		b.SequenceFrom = 0
	} else {
		b.SequenceFrom = w.Extrema.To
	}

	return b
}

// mark batch as dispatched to target deployment.
// A (partial) error may be returned per Subscription, meaning messages for other
// subscription may have succeeded.
func (s *state) SourceMarkDispatched(b *v1.MessageBatch) map[SubscriptionKey]error {
	if b == nil {
		return nil
	}

	errs := make(map[SubscriptionKey]error)

	d := gateway.Deployment(b.GetToDeployment())
	for _, msgs := range b.ListOfMessages {
		key := SubscriptionKey{
			TargetDeployment: d,
			SourceStreamName: msgs.GetSourceStreamName()}

		setID, err := NewSetIDFromBytes(msgs.GetSetId())
		if err != nil {
			errs[key] = errors.Wrap(err, "failed to parse batch id")
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
				From: msgs.GetSequenceFrom(),
				To:   msgs.GetSequenceTo()}}

		err = w.MarkDispatched(ack)
		if err != nil {
			errs[key] = errors.Wrap(err, "failed to mark dispatched")
		}

		delete(s.sourceOutgoing[d], key)
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// handle incoming ack from target deployment
// ack may be out or order (so the sequence from/to are out of order)
func (s *state) SourceHandleTargetAck(currentTime, sentTime time.Time, targetDeployment gateway.Deployment, ack *v1.Acknowledge) error {
	key := SubscriptionKey{
		TargetDeployment: targetDeployment,
		SourceStreamName: ack.GetSourceStreamName()}

	sub, exists := s.subscription[key]
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
			From: ack.GetSourceSequenceFrom(),
			To:   ack.GetSourceSequenceTo()}}
	changes, err := w.ReceiveAck(p)
	if err != nil {
		return errors.Wrap(err, "failed to process ack")
	}

	if changes {
		s.SourceLocalSubscriptions[key] = SourceSubscription{
			SubscriptionKey: key,
			DeliverPolicy:   jetstream.DeliverByStartSequencePolicy,
			OptStartSeq:     w.Extrema.From,
			FilterSubjects:  sub.FilterSubjects}
	}

	return nil
}

// description at source nats stream
type SourceSubscription struct {
	SubscriptionKey

	DeliverPolicy  jetstream.DeliverPolicy
	OptStartSeq    uint64
	OptStartTime   time.Time
	FilterSubjects []string
}

func toSourceSubscription(targetDeployment gateway.Deployment, sourceStream string, s *v1.ConsumerConfig, filterSubjects []string) SourceSubscription {
	return SourceSubscription{
		SubscriptionKey: SubscriptionKey{
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
