package kvo

import "testing"

func TestHexAndKeysThenValuesSwap(t *testing.T) {
	eq(t, Hex(0x42), "42h")

	s := keysThenValues{2, 1, 20, 10}
	s.Swap(0, 1)
	eq(t, s[0], uint64(1))
	eq(t, s[1], uint64(2))
	eq(t, s[2], uint64(10))
	eq(t, s[3], uint64(20))
}

