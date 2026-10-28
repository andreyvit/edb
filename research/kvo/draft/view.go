package kvodraft

import (
	"bytes"
	"encoding/binary"
	"math"
)

// Limits bound a single container. Byte payloads are opaque until opened.
const MaxBytes = 64 << 20
const MaxEntries = 1 << 20

// View is a borrowed, immutable object. Its lifetime is the data's lifetime.
type View struct{ data []byte }

func (v View) Raw() []byte { return v.data }
func (v View) Kind() Kind {
	if len(v.data) == 0 {
		return Numeric
	}
	return Kind((v.data[0] >> 1) & 3)
}
func (v View) Layout() Layout {
	if len(v.data) == 0 {
		return Compact
	}
	return Layout(v.data[0] & 1)
}
func (v View) Len() int { n, _, _, _ := v.header(); return n }

// Trusted skips validation. Use only for bytes already validated or produced by
// this codec. Invalid bytes may panic; no unsafe memory operations are used.
func Trusted(data []byte) View { return View{data} }
func readU(b []byte) (uint64, []byte) {
	n, k := binary.Uvarint(b)
	if k <= 0 || k != ulen(n) {
		panic(ErrFormat)
	}
	return n, b[k:]
}
func readBody(b []byte, w Wire) (Value, []byte) {
	v := Value{wire: w}
	switch w {
	case Zero:
	case Varint:
		v.word, b = readU(b)
	case Fixed1:
		if len(b) < 1 {
			panic(ErrFormat)
		}
		v.word = uint64(b[0])
		b = b[1:]
	case Fixed2:
		if len(b) < 2 {
			panic(ErrFormat)
		}
		v.word = uint64(binary.LittleEndian.Uint16(b))
		b = b[2:]
	case Fixed4:
		if len(b) < 4 {
			panic(ErrFormat)
		}
		v.word = uint64(binary.LittleEndian.Uint32(b))
		b = b[4:]
	case Fixed8:
		if len(b) < 8 {
			panic(ErrFormat)
		}
		v.word = binary.LittleEndian.Uint64(b)
		b = b[8:]
	case Bytes:
		n, r := readU(b)
		if n > uint64(len(r)) {
			panic(ErrFormat)
		}
		v.data = r[:int(n)]
		b = r[int(n):]
	default:
		panic(ErrFormat)
	}
	return v, b
}
func ReadValue(b []byte) (v Value, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != ErrFormat {
				panic(p)
			}
			v = Value{}
			err = ErrFormat
		}
	}()
	if len(b) == 0 || len(b) > MaxBytes {
		return Value{}, ErrFormat
	}
	v, tail := readBody(b[1:], Wire(b[0]))
	if len(tail) != 0 {
		return Value{}, ErrFormat
	}
	return v, nil
}
func (v View) header() (n int, w Wire, width int, body []byte) {
	if len(v.data) == 0 {
		return 0, mixed, 0, nil
	}
	if v.Layout() == Compact {
		tag, rest := readU(v.data)
		if tag>>6 > MaxEntries {
			panic(ErrFormat)
		}
		return int(tag >> 6), Wire((tag >> 3) & 7), 0, rest
	}
	if len(v.data) < 5 {
		panic(ErrFormat)
	}
	n = int(binary.LittleEndian.Uint32(v.data[1:]))
	d := v.data[0]
	return n, Wire((d >> 3) & 7), 1 << (d >> 6), v.data[5:]
}
func getWire(b []byte, i int) Wire {
	bit := i * 3
	word := uint16(b[bit/8])
	if bit%8 > 5 {
		word |= uint16(b[bit/8+1]) << 8
	}
	return Wire((word >> uint(bit%8)) & 7)
}
func putWire(b []byte, i int, w Wire) {
	bit := i * 3
	word := uint16(w) << uint(bit%8)
	b[bit/8] |= byte(word)
	if bit%8 > 5 {
		b[bit/8+1] |= byte(word >> 8)
	}
}
func (v View) numericAt(i int) Value {
	n, w, kw, b := v.header()
	b = b[n*kw:]
	if w == mixed {
		w = getWire(b, i)
		b = b[(n*3+7)/8:]
	}
	cell := b[i*8 : i*8+8]
	if w == Bytes {
		off := int(binary.LittleEndian.Uint32(cell))
		size := int(binary.LittleEndian.Uint32(cell[4:]))
		return ByteString(v.data[off : off+size])
	}
	return Value{wire: w, word: binary.LittleEndian.Uint64(cell)}
}
func (v View) entryAt(i int) []byte {
	n, _, w, b := v.header()
	a := getWidth(b[i*w:], w)
	z := getWidth(b[(i+1)*w:], w)
	return b[(n+1)*w:][int(a):int(z)]
}
func parseEntry(b []byte, kind Kind, w Wire) (key []byte, value Value, tail []byte) {
	if kind == Dictionary {
		n, r := readU(b)
		if n > uint64(len(r)) {
			panic(ErrFormat)
		}
		key = r[:int(n)]
		b = r[int(n):]
	}
	if w == mixed {
		if len(b) == 0 {
			panic(ErrFormat)
		}
		w = Wire(b[0])
		b = b[1:]
	}
	value, tail = readBody(b, w)
	return
}

// Open checks structural boundaries and key ordering without a semantic schema.
// It never parses inside Bytes, even if a caller intends them as a child object.
func Open(data []byte) (view View, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != ErrFormat {
				panic(p)
			}
			view = View{}
			err = ErrFormat
		}
	}()
	if len(data) == 0 {
		return View{}, nil
	}
	if len(data) > MaxBytes || (data[0]>>1)&3 > 2 {
		return View{}, ErrFormat
	}
	v := View{data}
	n, wire, w, body := v.header()
	if n < 0 || n > MaxEntries {
		return View{}, ErrFormat
	}
	if v.Layout() == Compact {
		it := v.Iter()
		var previous uint64
		var previousString []byte
		for i := 0; i < n; i++ {
			if !it.Next() {
				return View{}, ErrFormat
			}
			e := it.Entry()
			if i > 0 && v.Kind() == Numeric && e.Key <= previous {
				return View{}, ErrFormat
			}
			if i > 0 && v.Kind() == Dictionary && bytes.Compare(e.KeyBytes, previousString) <= 0 {
				return View{}, ErrFormat
			}
			previous, previousString = e.Key, e.KeyBytes
		}
		if it.pos != len(data) {
			return View{}, ErrFormat
		}
	} else {
		if v.Kind() == Numeric {
			codes := 0
			if wire == mixed {
				codes = (n*3 + 7) / 8
			}
			start := 5 + n*w + codes + n*8
			if start > len(data) {
				return View{}, ErrFormat
			}
			if codes > 0 && (n*3)%8 != 0 && body[n*w+codes-1]>>uint((n*3)%8) != 0 {
				return View{}, ErrFormat
			}
			end := start
			var previous uint64
			for i := 0; i < n; i++ {
				key := getWidth(body[i*w:], w)
				if i > 0 && key <= previous {
					return View{}, ErrFormat
				}
				previous = key
				code := wire
				if code == mixed {
					code = getWire(body[n*w:], i)
				}
				if code > Bytes {
					return View{}, ErrFormat
				}
				cell := body[n*w+codes+i*8:][:8]
				if code == Bytes {
					off := uint64(binary.LittleEndian.Uint32(cell))
					size := uint64(binary.LittleEndian.Uint32(cell[4:]))
					if off != uint64(end) || size > uint64(len(data)-end) {
						return View{}, ErrFormat
					}
					end += int(size)
				} else if !(Value{wire: code, word: binary.LittleEndian.Uint64(cell)}).valid() {
					return View{}, ErrFormat
				}
			}
			if end != len(data) {
				return View{}, ErrFormat
			}
		} else {
			if w == 8 || (n+1)*w > len(body) {
				return View{}, ErrFormat
			}
			payload := body[(n+1)*w:]
			if getWidth(body, w) != 0 || getWidth(body[n*w:], w) != uint64(len(payload)) {
				return View{}, ErrFormat
			}
			var previous []byte
			for i := 0; i < n; i++ {
				a, z := getWidth(body[i*w:], w), getWidth(body[(i+1)*w:], w)
				if a > z || z > uint64(len(payload)) {
					return View{}, ErrFormat
				}
				key, _, rest := parseEntry(payload[int(a):int(z)], v.Kind(), wire)
				if len(rest) != 0 {
					return View{}, ErrFormat
				}
				if v.Kind() == Dictionary && i > 0 && bytes.Compare(key, previous) <= 0 {
					return View{}, ErrFormat
				}
				previous = key
			}
		}
	}
	return v, nil
}

type Entry struct {
	Key      uint64
	KeyBytes []byte
	Value    Value
}

// Iterator holds only a cursor and borrowed slices; it allocates no entry objects.
type Iterator struct {
	view          View
	n, index, pos int
	key           uint64
	entry         Entry
}

func (v View) Iter() Iterator {
	n, _, _, body := v.header()
	return Iterator{view: v, n: n, pos: len(v.data) - len(body)}
}
func (it *Iterator) Next() bool {
	if it.index == it.n {
		return false
	}
	it.entry, it.pos, it.key = it.view.readEntry(it.index, it.pos, it.key)
	it.index++
	return true
}
func (it *Iterator) Entry() Entry { return it.entry }
func (v View) Get(key uint64) Value {
	if len(v.data) == 0 {
		return Value{}
	}
	if v.Kind() != Numeric {
		panic(ErrKind)
	}
	if v.Layout() == Indexed {
		n, _, w, b := v.header()
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if getWidth(b[mid*w:], w) < key {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < n && getWidth(b[lo*w:], w) == key {
			return v.numericAt(lo)
		}
		return Value{}
	}
	it := v.Iter()
	for it.Next() {
		e := it.Entry()
		if e.Key == key {
			return e.Value
		}
		if e.Key > key {
			break
		}
	}
	return Value{}
}
func (v View) GetString(key string) Value {
	if len(v.data) == 0 {
		return Value{}
	}
	if v.Kind() != Dictionary {
		panic(ErrKind)
	}
	if v.Layout() == Indexed {
		n, w, _, _ := v.header()
		lo, hi := 0, n
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			k, _, _ := parseEntry(v.entryAt(mid), Dictionary, w)
			if string(k) < key {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < n {
			k, value, _ := parseEntry(v.entryAt(lo), Dictionary, w)
			if string(k) == key {
				return value
			}
		}
		return Value{}
	}
	it := v.Iter()
	for it.Next() {
		e := it.Entry()
		if string(e.KeyBytes) == key {
			return e.Value
		}
		if string(e.KeyBytes) > key {
			break
		}
	}
	return Value{}
}
func (v View) At(index int) Value {
	if len(v.data) != 0 && v.Kind() != Sequence {
		panic(ErrKind)
	}
	if index < 0 || index >= v.Len() {
		panic(ErrIndex)
	}
	if v.Layout() == Indexed {
		_, w, _, _ := v.header()
		_, value, _ := parseEntry(v.entryAt(index), Sequence, w)
		return value
	}
	it := v.Iter()
	for i := 0; i <= index; i++ {
		it.Next()
	}
	return it.Entry().Value
}

// readEntry is a pure cursor step used by the editor's multi-pass writer.
func (v View) readEntry(index, pos int, key uint64) (e Entry, nextPos int, nextKey uint64) {
	_, shared, _, _ := v.header()
	kind := v.Kind()
	if v.Layout() == Indexed {
		if kind == Numeric {
			_, _, w, b := v.header()
			e.Key = getWidth(b[index*w:], w)
			e.Value = v.numericAt(index)
		} else {
			e.KeyBytes, e.Value, _ = parseEntry(v.entryAt(index), kind, shared)
			e.Key = uint64(index)
		}
		return e, pos, e.Key
	}
	b := v.data[pos:]
	if kind == Numeric {
		delta, rest := readU(b)
		wire := shared
		if wire == mixed {
			if delta&7 == 7 {
				if delta != 7 {
					panic(ErrFormat)
				}
				delta, rest = readU(rest)
				if delta <= math.MaxUint64>>3 {
					panic(ErrFormat)
				}
				if len(rest) == 0 {
					panic(ErrFormat)
				}
				wire = Wire(rest[0])
				rest = rest[1:]
			} else {
				wire = Wire(delta & 7)
				delta >>= 3
			}
		}
		if delta > math.MaxUint64-key || index > 0 && delta == 0 {
			panic(ErrFormat)
		}
		e.Key = key + delta
		e.Value, b = readBody(rest, wire)
	} else {
		e.KeyBytes, e.Value, b = parseEntry(b, kind, shared)
		e.Key = uint64(index)
	}
	return e, len(v.data) - len(b), e.Key
}
