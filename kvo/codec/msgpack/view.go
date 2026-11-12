package msgpack

import (
	"encoding/binary"
	"errors"
	"math"
)

var ErrFormat = errors.New("invalid MessagePack value")
var ErrDepth = errors.New("MessagePack nesting exceeds 256")

// View borrows one encoded MessagePack value. It never constructs a Go object.
// Trusted is for stored bytes; accessors still check boundaries before reading.
type View struct{ raw []byte }

func Trusted(raw []byte) View { return View{raw} }

// Open panics on error. Use TryOpen to handle errors explicitly.
func Open(raw []byte) View {
	v, err := TryOpen(raw)
	if err != nil {
		panic(err)
	}
	return v
}

func TryOpen(raw []byte) (v View, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()
	if skip(raw, 0) != len(raw) {
		return View{}, ErrFormat
	}
	return Trusted(raw), nil
}
func (v View) Raw() []byte   { return v.raw }
func (v View) Missing() bool { return len(v.raw) == 0 }
func (v View) Nil() bool     { return len(v.raw) == 0 || v.raw[0] == 0xc0 }
func need(b []byte, n int) {
	if n < 0 || len(b) < n {
		panic(ErrFormat)
	}
}

// header returns header bytes, scalar payload bytes and child value count.
func header(b []byte) (h, n, children int) {
	need(b, 1)
	c := b[0]
	switch {
	case c <= 0x7f || c >= 0xe0 || c == 0xc0 || c == 0xc2 || c == 0xc3:
		return 1, 0, 0
	case c >= 0xa0 && c <= 0xbf:
		return 1, int(c & 31), 0
	case c >= 0x90 && c <= 0x9f:
		return 1, 0, int(c & 15)
	case c >= 0x80 && c <= 0x8f:
		return 1, 0, int(c&15) * 2
	}
	switch c {
	case 0xc4, 0xd9:
		need(b, 2)
		return 2, int(b[1]), 0
	case 0xc5, 0xda:
		need(b, 3)
		return 3, int(binary.BigEndian.Uint16(b[1:])), 0
	case 0xc6, 0xdb:
		need(b, 5)
		return 5, int(binary.BigEndian.Uint32(b[1:])), 0
	case 0xc7:
		need(b, 3)
		return 3, int(b[1]), 0
	case 0xc8:
		need(b, 4)
		return 4, int(binary.BigEndian.Uint16(b[1:])), 0
	case 0xc9:
		need(b, 6)
		return 6, int(binary.BigEndian.Uint32(b[1:])), 0
	case 0xca, 0xce, 0xd2:
		return 1, 4, 0
	case 0xcb, 0xcf, 0xd3:
		return 1, 8, 0
	case 0xcc, 0xd0:
		return 1, 1, 0
	case 0xcd, 0xd1:
		return 1, 2, 0
	case 0xd4:
		return 2, 1, 0
	case 0xd5:
		return 2, 2, 0
	case 0xd6:
		return 2, 4, 0
	case 0xd7:
		return 2, 8, 0
	case 0xd8:
		return 2, 16, 0
	case 0xdc, 0xde:
		need(b, 3)
		n = int(binary.BigEndian.Uint16(b[1:]))
		if c == 0xde {
			n *= 2
		}
		return 3, 0, n
	case 0xdd, 0xdf:
		need(b, 5)
		n = int(binary.BigEndian.Uint32(b[1:]))
		if n < 0 || n > len(b)-5 {
			panic(ErrFormat)
		}
		if c == 0xdf {
			n *= 2
		}
		return 5, 0, n
	}
	panic(ErrFormat)
}
func skip(b []byte, depth int) int {
	if depth > 256 {
		panic(ErrDepth)
	}
	h, n, count := header(b)
	need(b, h)
	if n > len(b)-h {
		panic(ErrFormat)
	}
	if n < 0 || count < 0 {
		panic(ErrFormat)
	}
	off := h + n
	if count > len(b)-off {
		panic(ErrFormat)
	}
	for range count {
		off += skip(b[off:], depth+1)
	}
	return off
}
func (v View) Bytes() []byte {
	if v.Nil() {
		return nil
	}
	c := v.raw[0]
	if !(c >= 0xa0 && c <= 0xbf || c >= 0xc4 && c <= 0xc6 || c >= 0xd9 && c <= 0xdb) {
		panic(ErrFormat)
	}
	h, n, _ := header(v.raw)
	need(v.raw, h+n)
	return v.raw[h : h+n]
}
func (v View) Uint() uint64 {
	if v.Nil() {
		return 0
	}
	b := v.raw
	c := b[0]
	if c <= 0x7f {
		return uint64(c)
	}
	if c >= 0xe0 {
		return uint64(int64(int8(c)))
	}
	h, n, _ := header(b)
	need(b, h+n)
	switch c {
	case 0xcc:
		return uint64(b[1])
	case 0xcd:
		return uint64(binary.BigEndian.Uint16(b[1:]))
	case 0xce:
		return uint64(binary.BigEndian.Uint32(b[1:]))
	case 0xcf:
		return binary.BigEndian.Uint64(b[1:])
	case 0xd0:
		return uint64(int64(int8(b[1])))
	case 0xd1:
		return uint64(int64(int16(binary.BigEndian.Uint16(b[1:]))))
	case 0xd2:
		return uint64(int64(int32(binary.BigEndian.Uint32(b[1:]))))
	case 0xd3:
		return binary.BigEndian.Uint64(b[1:])
	}
	panic(ErrFormat)
}
func (v View) Int() int64 { return int64(v.Uint()) }
func (v View) Bool() bool {
	if v.Nil() {
		return false
	}
	if v.raw[0] == 0xc3 {
		return true
	}
	if v.raw[0] == 0xc2 {
		return false
	}
	panic(ErrFormat)
}
func (v View) Float() float64 {
	if v.Nil() {
		return 0
	}
	switch v.raw[0] {
	case 0xca:
		need(v.raw, 5)
		return float64(math.Float32frombits(binary.BigEndian.Uint32(v.raw[1:])))
	case 0xcb:
		need(v.raw, 9)
		return math.Float64frombits(binary.BigEndian.Uint64(v.raw[1:]))
	}
	if v.raw[0] <= 0x7f || v.raw[0] >= 0xcc && v.raw[0] <= 0xcf {
		return float64(v.Uint())
	}
	return float64(v.Int())
}
func (v View) mapHeader() (int, int) {
	if v.Nil() {
		return 0, 0
	}
	c := v.raw[0]
	if !(c >= 0x80 && c <= 0x8f || c == 0xde || c == 0xdf) {
		panic(ErrFormat)
	}
	h, _, n := header(v.raw)
	return h, n / 2
}

// Lookup scans encoded field boundaries and borrows only the selected value.
// It checks all keys so duplicate keys retain MessagePack's last-value semantics.
func (v View) Lookup(name string) View {
	off, n := v.mapHeader()
	var found View
	for range n {
		end := off + skip(v.raw[off:], 0)
		key := Trusted(v.raw[off:end]).Bytes()
		off = end
		end = off + skip(v.raw[off:], 0)
		if string(key) == name {
			found = Trusted(v.raw[off:end])
		}
		off = end
	}
	return found
}
func appendMapHeader(dst []byte, n int) []byte {
	if n < 16 {
		return append(dst, byte(0x80|n))
	}
	if n <= math.MaxUint16 {
		return binary.BigEndian.AppendUint16(append(dst, 0xde), uint16(n))
	}
	return binary.BigEndian.AppendUint32(append(dst, 0xdf), uint32(n))
}
func appendString(dst []byte, s string) []byte {
	n := len(s)
	switch {
	case n < 32:
		dst = append(dst, byte(0xa0|n))
	case n <= 255:
		dst = append(dst, 0xd9, byte(n))
	case n <= 65535:
		dst = binary.BigEndian.AppendUint16(append(dst, 0xda), uint16(n))
	default:
		dst = binary.BigEndian.AppendUint32(append(dst, 0xdb), uint32(n))
	}
	return append(dst, s...)
}

// Set replaces a map field while copying every untouched key and value verbatim.
// The result is owned, and does not mutate or retain spare capacity in the input.
func (v View) Set(name string, value []byte) View {
	if value == nil {
		panic(ErrFormat)
	}
	return v.Apply([]FieldEdit{{name, value}})
}

// Iterator borrows successive encoded children. Maps return keys and values;
// arrays return a missing key. No Go map, slice or element is decoded.
type Iterator struct {
	raw               []byte
	offset, remaining int
	pairs             bool
}

func (v View) Iter() Iterator {
	if v.Nil() {
		return Iterator{}
	}
	c := v.raw[0]
	pairs := c >= 0x80 && c <= 0x8f || c == 0xde || c == 0xdf
	if !pairs && !(c >= 0x90 && c <= 0x9f || c == 0xdc || c == 0xdd) {
		panic(ErrFormat)
	}
	h, _, n := header(v.raw)
	if pairs {
		n /= 2
	}
	return Iterator{v.raw, h, n, pairs}
}
func (it *Iterator) Len() int { return it.remaining }
func (it *Iterator) Next() (key, value View, ok bool) {
	if it.remaining == 0 {
		return View{}, View{}, false
	}
	if it.pairs {
		end := it.offset + skip(it.raw[it.offset:], 0)
		key = Trusted(it.raw[it.offset:end])
		it.offset = end
	}
	end := it.offset + skip(it.raw[it.offset:], 0)
	value = Trusted(it.raw[it.offset:end])
	it.offset = end
	it.remaining--
	return key, value, true
}

// Delete removes all occurrences of a map key, preserving every other encoded
// entry verbatim. An absent key leaves the original view unchanged.
func (v View) Delete(name string) View {
	if v.Lookup(name).Missing() {
		return v
	}
	return v.Apply([]FieldEdit{{Name: name}})
}
