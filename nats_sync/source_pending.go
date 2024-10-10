package nats_sync

import (
	"slices"
	"time"

	"github.com/pkg/errors"
)

// pending per subscription
type SourcePendingWindow struct {
	// min From (inclusive) and max To (inclusive) pending
	// if Acknowledged is empty, From and To are equal
	Extrema RangeInclusive[uint64]

	// pending acks
	Pending map[SetID]SourcePendingAck

	// acknowledged acks, but not contiguous with the Extrema,
	// i.e. some acks are missing in-between.
	// Ordered by SequenceRange.From
	Acknowledged []SourcePendingAck
}

// mark batch dispatched.
// If the source sequence From is 0, it is assumed the subscription has been restarted, and
// all pending acks will be removed.
// Otherwise, the pending sequence must start at Extrema.To
func (w *SourcePendingWindow) MarkDispatched(pending SourcePendingAck) error {

	if w.Extrema.From > pending.SequenceRange.From {
		return errors.New("sequence range mismatch")
	}

	if len(w.Acknowledged) > 0 {
		return errors.New("not implemented, acks")
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

	if w.Pending == nil {
		w.Pending = make(map[SetID]SourcePendingAck)
	}

	w.Pending[pending.SetID] = pending
	w.Extrema = w.Extrema.MaxTo(pending.SequenceRange)

	return nil
}

func (w *SourcePendingWindow) ReceiveAck(ack SourcePendingAck) (bool, error) {
	p, exists := w.Pending[ack.SetID]
	if !exists {
		// ack is not pending, ignore
		return false, nil
	}

	delete(w.Pending, ack.SetID)
	w.Acknowledged = append(w.Acknowledged, p)
	slices.SortFunc(w.Acknowledged, cmpPendingAckSequence)

	count := len(w.Acknowledged)
	for i, ack := range w.Acknowledged {
		if ack.SequenceRange.From == w.Extrema.From {
			w.Acknowledged = slices.Delete(w.Acknowledged, i, i+1)
			w.Extrema.From = ack.SequenceRange.To
		}
	}

	return len(w.Acknowledged) < count, nil
}

type SourcePendingAck struct {
	SetID         SetID
	SentTimestamp time.Time
	SequenceRange RangeInclusive[uint64]
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
