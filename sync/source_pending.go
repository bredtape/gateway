package sync

import (
	"slices"
	"time"

	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/bredtape/retry"
	"github.com/pkg/errors"
)

// pending per subscription
type SourcePendingWindow struct {
	// minimum acknowledged From (inclusive) and maximum pending To (inclusive)
	Extrema RangeInclusive[SourceSequence]

	// minimum acknowledge sequence. Must always be lower or equal to PendingExtrema.From
	AcknowledgedSequence SourceSequence

	// pending sequence From (buffered) and To (dispatched)
	PendingExtrema RangeInclusive[SourceSequence]

	// pending acks
	Pending map[SetID]SourcePendingAck

	// number of times pending acks have timed out and retried
	// Reset, when successful ack is received
	PendingRetries int

	// acknowledged acks, but not contiguous with the Extrema,
	// i.e. some acks are missing in-between.
	// Ordered by SequenceRange.From
	Acknowledged []SourcePendingAck

	// last activity time. Updated when messages are dispatched/retried and ack's received
	LastActivity time.Time
}

// mark batch dispatched.
// If the source sequence From is 0, it is assumed the subscription has been restarted, and
// all pending acks will be removed.
// Otherwise, the pending sequence must start at Extrema.To
func (w *SourcePendingWindow) MarkDispatched(pending SourcePendingAck) error {
	if w.Extrema.From > pending.SequenceRange.From {
		return errors.New("sequence range mismatch")
	}

	if pending.SequenceRange.From == 0 {
		w.Pending = make(map[SetID]SourcePendingAck)
		w.Acknowledged = nil
	}

	if w.Extrema.To != pending.SequenceRange.From {
		return errors.Wrapf(ErrSourceSequenceBroken, "dispatch pending ack From %d must be a continuation of the pending window %s",
			pending.SequenceRange.From, w.Extrema.String())
	}

	if w.Extrema.From > pending.SequenceRange.From {
		return errors.Wrapf(ErrSourceSequenceBroken,
			"pending ack sequence %s comes before pending window %s",
			pending.SequenceRange.String(), w.Extrema.String())
	}

	if !w.Extrema.Overlaps(pending.SequenceRange) {
		return errors.Wrapf(ErrSourceSequenceBroken,
			"pending sequence %s have gaps with pending window %s",
			pending.SequenceRange.String(), w.Extrema.String())
	}

	// only checks before this point

	if w.Pending == nil {
		w.Pending = make(map[SetID]SourcePendingAck)
	}

	// this is a retransmit, increment retries
	if _, exists := w.Pending[pending.SetID]; exists {
		w.PendingRetries++
	}

	w.Pending[pending.SetID] = pending
	w.Extrema = w.Extrema.MaxTo(pending.SequenceRange)
	w.LastActivity = maxTime(w.LastActivity, pending.SentTimestamp)

	return nil
}

// receive ack at the source
func (w *SourcePendingWindow) ReceiveAck(received time.Time, ack SourcePendingAck) (bool, error) {
	p, exists := w.Pending[ack.SetID]
	if !exists {
		// ack is not pending, ignore
		return false, nil
	}

	delete(w.Pending, ack.SetID)

	w.LastActivity = maxTime(w.LastActivity, received)

	if ack.IsNegative {
		from := ack.SequenceRange.From
		w.Extrema = RangeInclusive[SourceSequence]{From: from, To: from}
		pendings, to := w.getConsecutivePendingStartingFrom(from)

		w.Pending = pendings
		w.Extrema.To = to
		w.Acknowledged = nil

		return true, nil

	} else {
		w.PendingRetries = 0
		w.Acknowledged = append(w.Acknowledged, p)
		slices.SortFunc(w.Acknowledged, cmpPendingAckSequence)

		for i, ack := range w.Acknowledged {
			if ack.SequenceRange.From == w.Extrema.From {
				w.Acknowledged = slices.Delete(w.Acknowledged, i, i+1)
				w.Extrema.From = ack.SequenceRange.To
			}
		}

		return true, nil
	}
}

// get pending acks that should be retransmitted
func (w *SourcePendingWindow) GetRetransmit(now time.Time, ackTimeoutDuration time.Duration, backoff retry.Retryer) []SetID {
	timeout := now.Add(-ackTimeoutDuration)

	var result []SetID
	for _, pending := range w.Pending {
		if pending.SentTimestamp.After(timeout) {
			continue
		}

		retryWhen := now.Add(-backoff.Next(w.PendingRetries))
		if pending.SentTimestamp.After(retryWhen) {
			continue
		}
		result = append(result, pending.SetID)
	}
	return result
}

func (w *SourcePendingWindow) ShouldSentHeartbeat(now time.Time, heartbeatInterval time.Duration) bool {
	if len(w.Pending) > 0 {
		return false
	}
	if w.LastActivity.IsZero() {
		return false
	}
	if w.LastActivity.After(now.Add(-heartbeatInterval)) {
		return false
	}
	return true
}

type SourcePendingAck struct {
	SetID         SetID
	IsNegative    bool
	SentTimestamp time.Time
	SequenceRange RangeInclusive[SourceSequence]
	Messages      []*v1.Msg
}

// compare PendingAck by sequence range
func cmpPendingAckSequence(a, b SourcePendingAck) int {
	if a.SequenceRange.From < b.SequenceRange.From {
		return -1
	}
	if a.SequenceRange.From > b.SequenceRange.From {
		return 1
	}
	return 0
}

func (w SourcePendingWindow) getConsecutivePendingStartingFrom(from SourceSequence) (map[SetID]SourcePendingAck, SourceSequence) {
	pendings := make(map[SetID]SourcePendingAck, 0)
	max := from

	r := RangeInclusive[SourceSequence]{From: from, To: from}
	for {
		changes := false

		for _, p := range w.Pending {
			if r.To == p.SequenceRange.From {
				r = r.Expand(p.SequenceRange)
				max = r.To
				pendings[p.SetID] = p
				changes = true
			}
		}
		if !changes {
			break
		}
	}
	return pendings, max
}

func maxTime(time1, time2 time.Time) time.Time {
	if time1.After(time2) {
		return time1
	}
	return time2
}
