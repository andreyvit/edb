package kvo

func denseMapGet[M ~[]T, K ~uint64, T any](m M, k K) T {
	if int(k) >= len(m) {
		var zero T
		return zero
	}
	return m[k]
}

func denseMapSet[M ~[]T, K ~uint64, T any](mptr *M, k K, v T) {
	m := *mptr
	if int(k) >= len(m) {
		newlen := roundUpToPowerOf2(int(k) + 1)
		m = make([]T, newlen)
		copy(m, *mptr)
		*mptr = m
	}
	m[k] = v
}

func roundUpToPowerOf2(n int) int {
	if n < 4 {
		return 4
	}
	p := n - 1
	p |= p >> 1
	p |= p >> 2
	p |= p >> 4
	p |= p >> 8
	p |= p >> 16
	p |= p >> 32
	p++
	if p < 0 {
		return n
	} else {
		return p
	}
}
