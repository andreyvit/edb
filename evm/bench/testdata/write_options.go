package guest

import "m"

func Direct(r *m.CustomerEdit) int64 {
	var sum int64
	for i := int64(0); i < 256; i++ {
		r.earned = i & 1
		sum += r.earned
	}
	return sum
}

func Nested(r *m.CustomerEdit) uint64 {
	var sum uint64
	child := r.profile
	for i := uint64(0); i < 256; i++ {
		child.balance = i & 1
		sum += child.balance
	}
	return sum
}
