package kvolab

import (
	"encoding/binary"
	"math/bits"
)

// SCShape is the writer's physical descriptor, external to each value. These
// prototypes assume trusted data and a validated, immutable descriptor.
// SCBool shares True's code but encodes both bool values, never a constant.
const (
	SCNumericMap Kind = 250
	SCNullable   Kind = 251
	SCAny        Kind = 252
)

type SCField struct {
	Key   uint64
	Shape *SCShape
}
type SCShape struct {
	Kind   Kind
	Fields []SCField
	Elem   *SCShape
	// RequiredRecord is a schema contract, never inferred from one value's
	// occupancy. All fields must exist; the record then has no presence bytes.
	RequiredRecord bool
}
type SCView struct {
	Data  []byte
	Shape *SCShape
}

func ulen(v uint64) int { return max(1, (bits.Len64(v)+6)/7) }

// Record membership has one of three tiny encodings, selected by byte size:
// 0 = all descriptor fields; 1 = bitmap; n+2 = n sorted ordinal deltas.
// This changes neither the public model nor the representation of field values.
func appendSCPresence(dst []byte, s *SCShape, v Value) []byte {
	if len(v.Fields) == len(s.Fields) {
		for i, f := range v.Fields {
			if f.Key != s.Fields[i].Key {
				panic("schema mismatch")
			}
		}
		if s.RequiredRecord {
			return dst
		}
		return append(dst, 0)
	}
	if s.RequiredRecord {
		panic("missing required field")
	}
	count := len(v.Fields)
	sparseSize := ulen(uint64(count) + 2)
	j, prev := 0, -1
	for _, f := range v.Fields {
		for j < len(s.Fields) && s.Fields[j].Key < f.Key {
			j++
		}
		if j == len(s.Fields) || s.Fields[j].Key != f.Key {
			panic("unknown writer field")
		}
		sparseSize += ulen(uint64(j - prev))
		prev = j
	}
	bitmapSize := 1 + (len(s.Fields)+7)/8
	if bitmapSize < sparseSize {
		dst = append(dst, 1)
		start := len(dst)
		dst = append(dst, make([]byte, bitmapSize-1)...)
		j = 0
		for _, f := range v.Fields {
			for s.Fields[j].Key < f.Key {
				j++
			}
			dst[start+j/8] |= 1 << uint(j%8)
		}
		return dst
	}
	dst = binary.AppendUvarint(dst, uint64(count)+2)
	j, prev = 0, -1
	for _, f := range v.Fields {
		for s.Fields[j].Key < f.Key {
			j++
		}
		dst = binary.AppendUvarint(dst, uint64(j-prev))
		prev = j
	}
	return dst
}

// AppendSC emits only data whose interpretation is not already in SCShape.
// Runtime dictionary keys are still encoded. Numbers use base-128 varints;
// schema-known floats have exactly eight bytes; booleans have one byte.
func AppendSC(dst []byte, s *SCShape, v Value) []byte {
	switch s.Kind {
	case SCAny:
		return appendValue(dst, v, Compact, Compact)
	case SCNullable:
		if v.Kind == Null {
			return append(dst, 0)
		}
		return AppendSC(append(dst, 1), s.Elem, v)
	case Null:
		return dst
	case True:
		if v.Kind == True {
			return append(dst, 1)
		}
		if v.Kind != False {
			panic("bool")
		}
		return append(dst, 0)
	case Uint:
		return binary.AppendUvarint(dst, v.U)
	case Int:
		return binary.AppendUvarint(dst, uint64(int64(v.U)<<1)^uint64(int64(v.U)>>63))
	case Float:
		return binary.LittleEndian.AppendUint64(dst, v.U)
	case Text:
		dst = binary.AppendUvarint(dst, uint64(len(v.S)))
		return append(dst, v.S...)
	case Blob:
		dst = binary.AppendUvarint(dst, uint64(len(v.B)))
		return append(dst, v.B...)
	case Record:
		dst = appendSCPresence(dst, s, v)
		j := 0
		for _, f := range v.Fields {
			for s.Fields[j].Key < f.Key {
				j++
			}
			dst = AppendSC(dst, s.Fields[j].Shape, f.Value)
		}
		return dst
	case SCNumericMap:
		dst = binary.AppendUvarint(dst, uint64(len(v.Fields)))
		var prev uint64
		for _, f := range v.Fields {
			if f.Key < prev {
				panic("unsorted")
			}
			dst = binary.AppendUvarint(dst, f.Key-prev)
			dst = AppendSC(dst, s.Elem, f.Value)
			prev = f.Key
		}
		return dst
	case Dict:
		dst = binary.AppendUvarint(dst, uint64(len(v.Entries)))
		for _, e := range v.Entries {
			dst = binary.AppendUvarint(dst, uint64(len(e.Key)))
			dst = append(dst, e.Key...)
			dst = AppendSC(dst, s.Elem, e.Value)
		}
		return dst
	case List:
		dst = binary.AppendUvarint(dst, uint64(len(v.Items)))
		for _, e := range v.Items {
			dst = AppendSC(dst, s.Elem, e)
		}
		return dst
	default:
		panic("schema kind")
	}
}

// SkipSC is recursive for containers. It deliberately does not pretend that
// schema knowledge alone gives constant-time skipping or random field access.
func SkipSC(data []byte, s *SCShape) []byte {
	switch s.Kind {
	case SCAny:
		_, rest := readValue(data)
		return rest
	case SCNullable:
		if data[0] == 0 {
			return data[1:]
		}
		return SkipSC(data[1:], s.Elem)
	case Null:
		return data
	case True:
		return data[1:]
	case Uint, Int:
		_, data = takeU(data)
		return data
	case Float:
		return data[8:]
	case Text, Blob:
		n, rest := takeU(data)
		return rest[n:]
	case Record:
		it := SCView{data, s}.Fields()
		for {
			_, _, ok := it.Next()
			if !ok {
				return it.body
			}
		}
	case SCNumericMap, Dict, List:
		n, rest := takeU(data)
		for i := uint64(0); i < n; i++ {
			if s.Kind == SCNumericMap {
				_, rest = takeU(rest)
			}
			if s.Kind == Dict {
				k, b := takeU(rest)
				rest = b[k:]
			}
			rest = SkipSC(rest, s.Elem)
		}
		return rest
	default:
		panic("schema kind")
	}
}

type SCFields struct {
	shape                  *SCShape
	body, bitmap, ordinals []byte
	mode                   uint64
	left, slot             int
}

func (v SCView) Fields() SCFields {
	if v.Shape.RequiredRecord {
		return SCFields{shape: v.Shape, body: v.Data, mode: 0, left: len(v.Shape.Fields), slot: -1}
	}
	mode, body := takeU(v.Data)
	it := SCFields{shape: v.Shape, body: body, mode: mode, slot: -1}
	switch mode {
	case 0:
		it.left = len(v.Shape.Fields)
	case 1:
		n := (len(v.Shape.Fields) + 7) / 8
		it.bitmap, it.body = body[:n], body[n:]
		for _, b := range it.bitmap {
			it.left += bits.OnesCount8(b)
		}
	default:
		it.left = int(mode - 2)
		it.ordinals = body
		for i := 0; i < it.left; i++ {
			_, it.body = takeU(it.body)
		}
	}
	return it
}
func (it *SCFields) Next() (int, SCView, bool) {
	if it.left == 0 {
		return 0, SCView{}, false
	}
	switch it.mode {
	case 0:
		it.slot++
	case 1:
		it.slot++
		for it.bitmap[it.slot/8]&(1<<uint(it.slot%8)) == 0 {
			it.slot++
		}
	default:
		delta, rest := takeU(it.ordinals)
		it.ordinals = rest
		if delta == 0 {
			panic("duplicate ordinal")
		}
		it.slot += int(delta)
	}
	s := it.shape.Fields[it.slot].Shape
	tail := SkipSC(it.body, s)
	v := SCView{it.body[:len(it.body)-len(tail)], s}
	it.body = tail
	it.left--
	return it.slot, v, true
}
func (v SCView) Get(key uint64) (SCView, bool) {
	it := v.Fields()
	for {
		slot, x, ok := it.Next()
		if !ok {
			return SCView{}, false
		}
		k := v.Shape.Fields[slot].Key
		if k == key {
			return x, true
		}
		if k > key {
			return SCView{}, false
		}
	}
}
func (v SCView) Uint() uint64  { n, _ := takeU(v.Data); return n }
func (v SCView) Int() int64    { n, _ := takeU(v.Data); return int64(n>>1) ^ -int64(n&1) }
func (v SCView) Bytes() []byte { n, b := takeU(v.Data); return b[:n] }
func (v SCView) GetString(key string) (SCView, bool) {
	n, b := takeU(v.Data)
	for i := uint64(0); i < n; i++ {
		l, r := takeU(b)
		k := r[:l]
		r = r[l:]
		tail := SkipSC(r, v.Shape.Elem)
		if string(k) == key {
			return SCView{r[:len(r)-len(tail)], v.Shape.Elem}, true
		}
		b = tail
	}
	return SCView{}, false
}
