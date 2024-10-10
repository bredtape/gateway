package nats_sync

import (
	"cmp"
	"slices"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

func (s *state) TargetDeliverFromRemote(t time.Time, msgs *v1.Msgs) error {
	sub := getTargetSubscription(msgs)
	key := sub.TargetSubscriptionKey

	if sub.TargetDeployment != s.deployment {
		return errors.New("target deployment does not match")
	}

	subRegistered, exists := s.targetSubscription[key]
	if !exists {
		return errors.Wrapf(ErrNoSubscription, "no subscription for key %v", key)
	}

	if !subRegistered.Equals(sub) {
		return errors.Wrap(ErrNoSubscription, "subscription does not match")
	}

	if !slices.IsSortedFunc(msgs.GetMessages(), cmpMsgSequence) {
		return errors.New("messages must be in order")
	}

	s.TargetIncoming[key] = append(s.TargetIncoming[key], msgs)
	slices.SortFunc(s.TargetIncoming[key], cmpMsgsSequence)

	return nil
}

// commit set of incoming messages (which mean they have been persisted at the target).
// Incoming messages will not be deleted, if an error is returned
func (s *state) TargetCommit(msgs *v1.Msgs) error {
	sub := getTargetSubscription(msgs)
	key := sub.TargetSubscriptionKey

	for idx, pendingMsgs := range s.TargetIncoming[key] {
		if !slices.Equal(pendingMsgs.GetSetId(), msgs.GetSetId()) {
			continue
		}

		seq := RangeInclusive[uint64]{
			From: pendingMsgs.GetLastSequence(),
			To:   pendingMsgs.GetLastSequence()}

		if len(pendingMsgs.GetMessages()) > 0 {
			seq.To = pendingMsgs.GetMessages()[len(pendingMsgs.GetMessages())-1].GetSequence()
		}
		if seq.From > seq.To {
			return errors.New("invalid message range")
		}

		ack := &v1.Acknowledge{
			SetId:            msgs.GetSetId(),
			SourceStreamName: msgs.GetSourceStreamName(),
			SequenceFrom:     seq.From,
			SequenceTo:       seq.To}
		_, err := NewSetIDFromBytes(msgs.GetSetId())
		if err != nil {
			return errors.Wrap(err, "failed to parse set ID")
		}

		if w, exists := s.targetCommit[key]; !exists {
			// sequence must start from 0
			if seq.From != 0 {
				return errors.Wrapf(ErrSourceSequenceBroken, "nothing committed yet, sequence must start from 0, but was %s", seq)
			}

			s.targetCommit[key] = &TargetCommitWindow{CommittedExtrema: seq}

		} else {
			// sequence must continue from last committed
			if seq.From != w.CommittedExtrema.To {
				return errors.Wrap(ErrSourceSequenceBroken, "sequence must continue from last committed")
			}
		}

		s.targetCommit[key].Commit(sub.SourceDeployment, ack)
		s.TargetIncoming[key] = slices.Delete(s.TargetIncoming[key], idx, idx+1)
		if len(s.TargetIncoming[key]) == 0 {
			delete(s.TargetIncoming, key)
		}

		return nil
	}

	return errors.New("set not found")
}

type TargetSubscriptionKey struct {
	SourceDeployment gateway.Deployment
	SourceStreamName string
}

type TargetSubscription struct {
	TargetSubscriptionKey

	TargetDeployment gateway.Deployment
	DeliverPolicy    jetstream.DeliverPolicy
	OptStartSeq      uint64
	OptStartTime     time.Time
	FilterSubjects   []string
}

func (x TargetSubscription) Equals(y TargetSubscription) bool {
	return x.TargetSubscriptionKey == y.TargetSubscriptionKey &&
		x.TargetDeployment == y.TargetDeployment &&
		x.DeliverPolicy == y.DeliverPolicy &&
		x.OptStartSeq == y.OptStartSeq &&
		x.OptStartTime.Equal(y.OptStartTime) &&
		slices.Equal(x.FilterSubjects, y.FilterSubjects)
}

func getTargetSubscription(msgs *v1.Msgs) TargetSubscription {
	cc := msgs.GetConsumerConfig()
	return TargetSubscription{
		TargetSubscriptionKey: TargetSubscriptionKey{
			SourceDeployment: gateway.Deployment(msgs.GetSourceDeployment()),
			SourceStreamName: msgs.GetSourceStreamName()},
		TargetDeployment: gateway.Deployment(msgs.GetTargetDeployment()),
		DeliverPolicy:    ToDeliverPolicy(cc.GetDeliverPolicy()),
		OptStartSeq:      cc.GetOptStartSeq(),
		OptStartTime:     fromUnixTime(cc.GetOptStartTime()),
		FilterSubjects:   msgs.GetFilterSubjects()}
}

func fromTargetSubscription(sub TargetSubscription) *v1.ConsumerConfig {
	return &v1.ConsumerConfig{
		DeliverPolicy: FromDeliverPolicy(sub.DeliverPolicy),
		OptStartSeq:   sub.OptStartSeq,
		OptStartTime:  toUnixTime(sub.OptStartTime)}
}

type TargetCommitWindow struct {
	// extrama, where From is the lowest sequence number received and To is the highest
	//ReceivedExtrema RangeInclusive[uint64]

	CommittedExtrema RangeInclusive[uint64]

	// pending acks per source deployment (which is the intended destination).
	// Acks that have been committed and are waiting to be included in the next batch
	PendingAcks map[gateway.Deployment]map[SetID]*v1.Acknowledge

	// lowest committed sequence number. This is used to indicate to the source,
	// if the subscription should be restarted
	// If LowestCommitted is 0, the subscription should be restarted.
	// Otherwise lowest committed MUST match CommittedExtrema.From
	//LowestCommitted uint64
}

func (w *TargetCommitWindow) Commit(deployment gateway.Deployment, ack *v1.Acknowledge) {
	if w.PendingAcks == nil {
		w.PendingAcks = make(map[gateway.Deployment]map[SetID]*v1.Acknowledge)
	}
	if w.PendingAcks[deployment] == nil {
		w.PendingAcks[deployment] = make(map[SetID]*v1.Acknowledge)
	}
	id, _ := NewSetIDFromBytes(ack.GetSetId())
	w.PendingAcks[deployment][id] = ack
}

func cmpMsgsSequence(a, b *v1.Msgs) int {
	return cmp.Compare(a.GetLastSequence(), b.GetLastSequence())
}
