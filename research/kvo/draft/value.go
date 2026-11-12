// Package kvodraft is an executable draft of KVO's schema-independent framing.
// It is not the production kvo package and its wire format is not stable.
package kvodraft

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
)

// Wire describes physical storage, not a semantic type.
type Wire byte

const (
	Zero Wire = iota
	Varint
	Fixed1
	Fixed2
	Fixed4
	Fixed8
	Bytes
	mixed Wire = 7
)

type Layout byte

const (
	Compact Layout = iota
	Indexed
)

type Kind byte

const (
	Numeric Kind = iota
	Dictionary
	Sequence
)

var (
	ErrFormat     = errors.New("invalid draft KVO encoding")
	ErrOrder      = errors.New("keys must be unique and sorted")
	ErrKind       = errors.New("wrong container kind")
	ErrValue      = errors.New("invalid value")
	ErrType       = errors.New("incompatible value access")
	ErrUnknownKey = errors.New("unknown record field")
	ErrIndex      = errors.New("list index out of bounds")
	ErrLimit      = errors.New("draft KVO encoding limit exceeded")
)

// Value borrows its byte payload. Scalar words never contain Go pointers.
// It deliberately carries no semantic schema or object catalog.
type Value struct {
	wire Wire
	word uint64
	data []byte
}

func ZeroValue() Value { return Value{} }

// Word uses a fixed-width scalar encoding, useful for a hot mutable counter.
func Word(v uint64) Value {
	if v == 0 {
		return Value{}
	}
	return Value{wire: Fixed8, word: v}
}
func Uint(v uint64) Value {
	if v == 0 {
		return Value{}
	}
	w := Fixed8
	size := 8
	switch {
	case v <= 255:
		w, size = Fixed1, 1
	case v <= 65535:
		w, size = Fixed2, 2
	case v <= math.MaxUint32:
		w, size = Fixed4, 4
	}
	if max(1, (bits.Len64(v)+6)/7) < size {
		w = Varint
	}
	return Value{wire: w, word: v}
}
func Int(v int64) Value { return Uint(uint64(v<<1) ^ uint64(v>>63)) }
func Float(v float64) Value {
	// Both signs of zero have Go zero semantics.
	if v == 0 {
		return Value{}
	}
	return Value{wire: Fixed8, word: math.Float64bits(v)}
}
func Bool(v bool) Value {
	if v {
		return Uint(1)
	}
	return Value{}
}
func ByteString(v []byte) Value {
	if len(v) == 0 {
		return Value{}
	}
	return Value{wire: Bytes, data: v}
}

// ObjectValue knows that an empty container is the model's zero value. Raw byte
// spans remain opaque: ByteString never guesses whether bytes encode an object.
func ObjectValue(v View) Value {
	if v.Len() == 0 {
		return Value{}
	}
	return ByteString(v.data)
}
func (v Value) Wire() Wire { return v.wire }
func (v Value) IsZero() bool {
	if v.wire == Bytes {
		return len(v.data) == 0
	}
	return v.word == 0
}
func (v Value) Uint() uint64 {
	if v.IsZero() {
		return 0
	}
	if v.wire < Varint || v.wire > Fixed8 {
		panic(ErrType)
	}
	return v.word
}
func (v Value) Int() int64 { n := v.Uint(); return int64(n>>1) ^ -int64(n&1) }
func (v Value) Float() float64 {
	if v.IsZero() {
		return 0
	}
	if v.wire != Fixed8 {
		panic(ErrType)
	}
	return math.Float64frombits(v.word)
}
func (v Value) Bool() bool {
	n := v.Uint()
	if n > 1 {
		panic(ErrValue)
	}
	return n != 0
}
func (v Value) Bytes() []byte {
	if v.IsZero() {
		return nil
	}
	if v.wire != Bytes {
		panic(ErrType)
	}
	return v.data
}
func (v Value) Object() View {
	if v.IsZero() {
		return View{}
	}
	if v.wire != Bytes {
		panic(ErrType)
	}
	view, err := Open(v.data)
	if err != nil {
		panic(err)
	}
	return view
}
func (v Value) valid() bool {
	switch v.wire {
	case Zero:
		return v.word == 0
	case Fixed1:
		return v.word <= 255
	case Fixed2:
		return v.word <= 65535
	case Fixed4:
		return v.word <= math.MaxUint32
	case Bytes:
		return true
	case Varint, Fixed8:
		return true
	}
	return false
}
func ulen(v uint64) int { return max(1, (bits.Len64(v)+6)/7) }
func width(v uint64) int {
	switch {
	case v <= 255:
		return 1
	case v <= 65535:
		return 2
	case v <= math.MaxUint32:
		return 4
	default:
		return 8
	}
}
func widthCode(n int) byte {
	switch n {
	case 1:
		return 0
	case 2:
		return 1
	case 4:
		return 2
	case 8:
		return 3
	}
	panic(ErrValue)
}
func putWidth(b []byte, v uint64, n int) {
	switch n {
	case 1:
		b[0] = byte(v)
	case 2:
		binary.LittleEndian.PutUint16(b, uint16(v))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(v))
	case 8:
		binary.LittleEndian.PutUint64(b, v)
	}
}
func getWidth(b []byte, n int) uint64 {
	switch n {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	}
	panic(ErrValue)
}
func appendBody(dst []byte, v Value) []byte {
	switch v.wire {
	case Zero:
		return dst
	case Varint:
		return binary.AppendUvarint(dst, v.word)
	case Fixed1:
		return append(dst, byte(v.word))
	case Fixed2:
		return binary.LittleEndian.AppendUint16(dst, uint16(v.word))
	case Fixed4:
		return binary.LittleEndian.AppendUint32(dst, uint32(v.word))
	case Fixed8:
		return binary.LittleEndian.AppendUint64(dst, v.word)
	case Bytes:
		dst = binary.AppendUvarint(dst, uint64(len(v.data)))
		return append(dst, v.data...)
	}
	panic(ErrValue)
}
func bodyLen(v Value) int {
	switch v.wire {
	case Zero:
		return 0
	case Varint:
		return ulen(v.word)
	case Fixed1:
		return 1
	case Fixed2:
		return 2
	case Fixed4:
		return 4
	case Fixed8:
		return 8
	case Bytes:
		return ulen(uint64(len(v.data))) + len(v.data)
	}
	panic(ErrValue)
}

// AppendValue is the standalone form used for change payloads: wire + body.
func AppendValue(dst []byte, v Value) []byte {
	if !v.valid() {
		panic(ErrValue)
	}
	if len(v.data) > MaxBytes || bodyLen(v)+1 > MaxBytes {
		panic(ErrLimit)
	}
	if v.IsZero() {
		v = Value{}
	}
	return appendBody(append(dst, byte(v.wire)), v)
}
