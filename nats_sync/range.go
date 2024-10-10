package nats_sync

import (
	"cmp"
	"fmt"
)

// range with inclusive From and To
type RangeInclusive[T cmp.Ordered] struct {
	From T
	To   T
}

func (r RangeInclusive[T]) String() string {
	return fmt.Sprintf("[%v, %v]", r.From, r.To)
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

// returns a new range using the lhs 'From', but the maximum 'To' of the two ranges
func (lhs RangeInclusive[T]) MaxTo(rhs RangeInclusive[T]) RangeInclusive[T] {
	return RangeInclusive[T]{
		From: lhs.From,
		To:   max(lhs.To, rhs.To)}
}

func max[T cmp.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
