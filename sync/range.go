package sync

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

func (r RangeInclusive[T]) Validate() error {
	if r.From > r.To {
		return fmt.Errorf("invalid range %v", r)
	}
	return nil
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

// whether the 'lhs' contains the value
func (lhs RangeInclusive[T]) ContainsValue(value T) bool {
	if value < lhs.From {
		return false
	}

	if value > lhs.To {
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

// expand the range to include the 'rhs' range. New From is the minimum of the two ranges,
// and the new To is the maximum of the two ranges
func (lhs RangeInclusive[T]) Expand(rhs RangeInclusive[T]) RangeInclusive[T] {
	return RangeInclusive[T]{
		From: min(lhs.From, rhs.From),
		To:   max(lhs.To, rhs.To)}
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
