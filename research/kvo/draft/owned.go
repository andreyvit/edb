package kvodraft

import "encoding/binary"

// TryPatchOwned replaces an existing scalar cell in an indexed numeric object.
// data must be validated and exclusively owned. Never pass bbolt's borrowed
// bytes here. Clearing a value, changing a shared class, or replacing a byte
// payload returns false so the caller can rebuild through an editor.
func TryPatchOwned(data []byte, key uint64, value Value) bool {
	if !value.valid() {
		panic(ErrValue)
	}
	if len(data) == 0 || value.wire == Bytes || value.IsZero() {
		return false
	}
	v := Trusted(data)
	if v.Kind() != Numeric {
		panic(ErrKind)
	}
	if v.Layout() != Indexed {
		return false
	}
	n, shared, w, b := v.header()
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if getWidth(b[mid*w:], w) < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == n || getWidth(b[lo*w:], w) != key {
		return false
	}
	codes := 0
	old := shared
	if shared == mixed {
		codes = (n*3 + 7) / 8
		old = getWire(b[n*w:], lo)
	}
	if old == Bytes || shared != mixed && shared != value.wire {
		return false
	}
	if shared == mixed {
		bits := b[n*w:]
		bit := lo * 3
		shift := uint(bit % 8)
		word := uint16(bits[bit/8])
		if shift > 5 {
			word |= uint16(bits[bit/8+1]) << 8
		}
		word = word&^(7<<shift) | uint16(value.wire)<<shift
		bits[bit/8] = byte(word)
		if shift > 5 {
			bits[bit/8+1] = byte(word >> 8)
		}
	}
	binary.LittleEndian.PutUint64(b[n*w+codes+lo*8:], value.word)
	return true
}
