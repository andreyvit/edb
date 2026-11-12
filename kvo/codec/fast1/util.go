package fast1

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
