package sync

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/smartystreets/goconvey/convey"
)

// sink deliver messages from remote to internal buffer (in-memory), waiting to be committed
func (s *state) SinkDeliverFromRemote(t time.Time, msgs *v1.Msgs) error {
	sub := getSinkSubscription(msgs)
	key := sub.SinkSubscriptionKey

	if sub.SinkDeployment != s.from {
		return errors.Errorf("sink deployment 'to' does not match, expected %s, got %s", s.from, sub.SinkDeployment)
	}

	subRegistered, exists := s.sinkSubscription[key]
	if !exists {
		return errors.Wrapf(ErrNoSubscription, "no subscription for key %v", key)
	}

	if !subRegistered.Equals(sub) {
		convey.Printf("subRegistered: %v, received %v", subRegistered, sub)
		return errors.Wrap(ErrNoSubscription, "subscription does not match")
	}

	if !slices.IsSortedFunc(msgs.GetMessages(), cmpMsgSequence) {
		return errors.New("messages must be in order")
	}

	c := len(msgs.GetMessages())
	for _, m := range s.SinkIncoming[key] {
		c += len(m.GetMessages())
	}

	if c > s.cs.PendingIncomingMessagesPrSubscriptionMaxBuffered {
		return errors.Wrapf(ErrBackoff, "max pending incoming messages (%d) reached", s.cs.PendingIncomingMessagesPrSubscriptionMaxBuffered)
	}

	// all checks passed, add to incoming
	// (not checking that message sequence aligns here, since messages can be received out of order)
	s.SinkIncoming[key] = append(s.SinkIncoming[key], msgs)

	// order by last sequence
	slices.SortFunc(s.SinkIncoming[key], cmpMsgsSequence)

	return nil
}

// commit set of incoming messages (which mean they have been persisted at the sink).
// Incoming messages will not be deleted, if an error is returned.
// Messages must be persisted with source-sequence
func (s *state) SinkCommit(msgs *v1.Msgs) error {
	sub := getSinkSubscription(msgs)
	key := sub.SinkSubscriptionKey

	for idx, pendingMsgs := range s.SinkIncoming[key] {
		if pendingMsgs.GetSetId() != msgs.GetSetId() {
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

		if w, exists := s.sinkCommit[key]; !exists {
			// sequence must start from 0
			if seq.From != 0 {
				return errors.Wrapf(ErrSourceSequenceBroken, "nothing committed yet, sequence must start from 0, but was %s", seq)
			}

			s.sinkCommit[key] = &SinkCommitWindow{CommittedExtrema: seq}

		} else {
			// sequence must continue from last committed
			if !seq.ContainsValue(w.CommittedExtrema.To) {
				return errors.Wrapf(ErrSourceSequenceBroken, "sequence range %s must continue from last committed %d",
					seq, w.CommittedExtrema.To)
			}
		}

		s.sinkCommit[key].Commit(ack)
		s.SinkIncoming[key] = slices.Delete(s.SinkIncoming[key], idx, idx+1)
		if len(s.SinkIncoming[key]) == 0 {
			delete(s.SinkIncoming, key)
		}

		return nil
	}

	return errors.New("set not found")
}

// reject a set of incoming messages
// A NAK (not-acknowledge) will be sent back to the source, indicating how
// the sync should be restarted.
// If lastSequence>0, the source should restart from lastSequence
// If lastSequence=0, the source should restart the subscription
func (s *state) SinkCommitReject(msgs *v1.Msgs, lastSequence SourceSequence) error {
	sub := getSinkSubscription(msgs)
	key := sub.SinkSubscriptionKey

	for idx, pendingMsgs := range s.SinkIncoming[key] {
		if pendingMsgs.GetSetId() != msgs.GetSetId() {
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
			SequenceFrom:     uint64(lastSequence),
			IsNegative:       true,
			Reason:           fmt.Sprintf("sink rejected range %s, request restart (lastSequence %d)", seq.String(), lastSequence)}

		if _, exists := s.sinkCommit[key]; !exists {
			s.sinkCommit[key] = &SinkCommitWindow{}
		}

		s.sinkCommit[key].Commit(ack)

		s.SinkIncoming[key] = slices.Delete(s.SinkIncoming[key], idx, idx+1)
		if len(s.SinkIncoming[key]) == 0 {
			delete(s.SinkIncoming, key)
		}

		return nil
	}

	return errors.New("matching set ID not found")
}

type SinkSubscriptionKey struct {
	SourceStreamName string
}

type SinkSubscription struct {
	SinkSubscriptionKey

	SinkDeployment gateway.Deployment
	DeliverPolicy  jetstream.DeliverPolicy
	OptStartSeq    uint64
	OptStartTime   time.Time
	FilterSubjects []string
}

func (x SinkSubscription) Equals(y SinkSubscription) bool {
	return x.SinkSubscriptionKey == y.SinkSubscriptionKey &&
		x.SinkDeployment == y.SinkDeployment &&
		x.DeliverPolicy == y.DeliverPolicy &&
		x.OptStartSeq == y.OptStartSeq &&
		x.OptStartTime.Equal(y.OptStartTime) &&
		slices.Equal(x.FilterSubjects, y.FilterSubjects)
}

func getSinkSubscription(msgs *v1.Msgs) SinkSubscription {
	cc := msgs.GetConsumerConfig()
	sub := SinkSubscription{
		SinkSubscriptionKey: SinkSubscriptionKey{SourceStreamName: msgs.GetSourceStreamName()},
		SinkDeployment:      gateway.Deployment(msgs.GetSinkDeployment()),
		DeliverPolicy:       ToDeliverPolicy(cc.GetDeliverPolicy()),
		OptStartSeq:         cc.GetOptStartSeq(),
		FilterSubjects:      msgs.GetFilterSubjects()}
	slices.Sort(sub.FilterSubjects)

	if sub.DeliverPolicy == jetstream.DeliverByStartTimePolicy {
		sub.OptStartTime = cc.GetOptStartTime().AsTime()
	}
	return sub
}

type SinkCommitWindow struct {
	// extrama, where From is the lowest sequence number committed and To is the highest
	// received, but not yet committed
	CommittedExtrema RangeInclusive[uint64]

	// pending acks per source deployment (which is the intended destination).
	// Acks that have been committed and are waiting to be included in the next batch
	PendingAcks map[SetID]*v1.Acknowledge

	// lowest committed sequence number. This is used to indicate to the source,
	// if the subscription should be restarted
	// If LowestCommitted is 0, the subscription should be restarted.
	// Otherwise lowest committed MUST match CommittedExtrema.From
	//LowestCommitted uint64
}

// commit ack/nak (assuming the matching window has been picked)
func (w *SinkCommitWindow) Commit(ack *v1.Acknowledge) {
	if w.PendingAcks == nil {
		w.PendingAcks = make(map[SetID]*v1.Acknowledge)
	}
	id := SetID(ack.GetSetId())
	w.PendingAcks[id] = ack

	if ack.IsNegative {
		w.CommittedExtrema = RangeInclusive[uint64]{
			From: ack.GetSequenceFrom(),
			To:   ack.GetSequenceFrom()}
	}
}

// comparer for *v1.Msgs, comparing the last sequence number
func cmpMsgsSequence(a, b *v1.Msgs) int {
	return cmp.Compare(a.GetLastSequence(), b.GetLastSequence())
}
