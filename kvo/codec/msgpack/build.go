package msgpack

import (
	"encoding/binary"
	"iter"
)

// BuildMap copies raw entries into one new map, preserving their encodings.
// capacity is a sizing hint; nil values represent MessagePack null.
func BuildMap(capacity int, entries iter.Seq2[View, View]) View {
	dst := make([]byte, 5, max(5, capacity+5))
	n := 0
	for key, value := range entries {
		if key.Missing() {
			panic("msgpack: missing map key")
		}
		dst = append(dst, key.raw...)
		if value.Missing() {
			dst = append(dst, 0xc0)
		} else {
			dst = append(dst, value.raw...)
		}
		n++
	}
	var header [5]byte
	h := appendMapHeader(header[:0], n)
	start := 5 - len(h)
	copy(dst[start:5], h)
	return Trusted(dst[start:])
}

// BuildArray copies raw elements into one new array.
func BuildArray(capacity int, entries iter.Seq[View]) View {
	dst := make([]byte, 5, max(5, capacity+5))
	n := 0
	for value := range entries {
		if value.Missing() {
			dst = append(dst, 0xc0)
		} else {
			dst = append(dst, value.raw...)
		}
		n++
	}
	var header [5]byte
	h := header[:0]
	switch {
	case n < 16:
		h = append(h, byte(0x90+n))
	case n <= 0xffff:
		h = binary.BigEndian.AppendUint16(append(h, 0xdc), uint16(n))
	default:
		h = binary.BigEndian.AppendUint32(append(h, 0xdd), uint32(n))
	}
	start := 5 - len(h)
	copy(dst[start:5], h)
	return Trusted(dst[start:])
}
