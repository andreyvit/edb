package kvolab

// The selected scalar contract. Signedness comes from the model, never from a
// wire tag. These two static pointers are in-memory construction/copy hints,
// not persisted types or fabricated addresses.
import (
	"math"
	"math/bits"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

var currentWordHints [2]byte

type currentValue struct {
	ptr *byte
	num uint64
}

func currentWord(n uint64) currentValue {
	if n <= 127 {
		return currentValue{num: n}
	}
	return currentValue{&currentWordHints[1], n}
}
func currentInt(n int64) currentValue {
	if n >= 0 && n <= 127 {
		return currentWord(uint64(n))
	}
	return currentValue{&currentWordHints[0], uint64(n)}
}

// A schema-free Fast read must retain the original eight-byte boundary. In
// particular, shortening unknown signed +128 to unsigned byte 80 would change
// its meaning to -128 when read as a signed Compact payload.
func currentRawWord(n uint64) currentValue { return currentValue{num: n} }
func currentBytes(b []byte) currentValue {
	if len(b) == 0 {
		return currentValue{}
	}
	return currentValue{unsafe.SliceData(b), uint64(len(b))}
}
func (v currentValue) isSpan() bool {
	return v.ptr != nil && v.ptr != &currentWordHints[0] && v.ptr != &currentWordHints[1]
}

// Empty spans are normalized on construction, so zero needs no kind check.
func (v currentValue) IsZero() bool { return v.num == 0 }
func (v currentValue) Bytes() []byte {
	if !v.isSpan() {
		if v.num != 0 {
			panic(draft.ErrType)
		}
		return nil
	}
	return unsafe.Slice(v.ptr, int(v.num))
}

// Keep the common word read small enough to inline. The borrowed-byte case
// retains the same checked decoder in a separate helper.
func (v currentValue) Uint() uint64 {
	if v.ptr == nil {
		return v.num
	}
	return currentUintPayload(v)
}
func currentUintPayload(v currentValue) uint64 {
	if !v.isSpan() {
		return v.num
	}
	if v.num > 8 {
		panic(draft.ErrType)
	}
	return tagWord(v.Bytes())
}

// Retained before outlining, for a paired implementation-only comparison.
func (v currentValue) UintBeforeInlining() uint64 {
	if !v.isSpan() {
		return v.num
	}
	if v.num > 8 {
		panic(draft.ErrType)
	}
	return tagWord(v.Bytes())
}
func (v currentValue) Int() int64 {
	if v.ptr == nil {
		return int64(v.num)
	}
	return currentIntPayload(v)
}
func currentIntPayload(v currentValue) int64 {
	if !v.isSpan() {
		return int64(v.num)
	}
	n := v.num
	if n > 8 {
		panic(draft.ErrType)
	}
	if n == 0 {
		return 0
	}
	shift := uint(64 - 8*n)
	return int64(tagWord(v.Bytes())<<shift) >> shift
}
func (v currentValue) Float() float64 { return math.Float64frombits(v.Uint()) }
func (v currentValue) Bool() bool {
	n := v.Uint()
	if n > 1 {
		panic(draft.ErrType)
	}
	return n != 0
}
func (v currentValue) scalarWidth() int {
	switch v.ptr {
	case &currentWordHints[0]:
		n := v.num ^ uint64(int64(v.num)>>63)
		return (bits.Len64(n) + 1 + 7) / 8
	case &currentWordHints[1]:
		return (bits.Len64(v.num) + 7) / 8
	default:
		if v.num <= 127 {
			return 0
		}
		return 8
	}
}

func currentFromCompact(v lengthValue) currentValue {
	return currentValue{v.ptr, v.num}
}
func currentFromFast(v lengthValue) currentValue {
	if v.ptr == nil {
		return currentRawWord(v.num)
	}
	return currentBytes(v.Bytes())
}

// Only edit scratch uses these codes. They preserve constructor intent across
// deferred writes; they do not add signed or width classes to the wire format.
func currentEditCode(v currentValue) draft.Wire {
	switch v.ptr {
	case &currentWordHints[0]:
		return 11
	case &currentWordHints[1]:
		return 12
	default:
		return 8
	}
}
func currentEditWord(n uint64, code draft.Wire) currentValue {
	switch code {
	case 11:
		return currentInt(int64(n))
	case 12:
		return currentWord(n)
	default:
		return currentRawWord(n)
	}
}
