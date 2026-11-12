package kvo

import "testing"

func TestHex(t *testing.T) {
	eq(t, Hex(0x42), "42h")
}
