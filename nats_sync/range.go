package nats_sync

import "cmp"

// range with inclusive From and To
type RangeInclusive[T cmp.Ordered] struct {
	From T
	To   T
}

// whether the 'lhs' contains the 'rhs' range
func (lhs RangeInclusive[T]) Contains(rhs RangeInclusive[T]) bool {
	if lhs.From > rhs.From {
		return false
	}

	if lhs.To < rhs.To {
		return false
	}

	return true
}

func (lhs RangeInclusive[T]) Overlaps(rhs RangeInclusive[T]) bool {
	if lhs.From > rhs.To {
		return false
	}

	if lhs.To < rhs.From {
		return false
	}

	return true
}
