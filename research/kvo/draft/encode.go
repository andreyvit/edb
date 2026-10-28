package kvodraft

import (
	"encoding/binary"
	"math"
)

type Field struct {
	Key   uint64
	Value Value
}
type Pair struct {
	Key   string
	Value Value
}

func selector(current, next Wire, first bool) Wire {
	if first {
		return next
	}
	if current != next {
		return mixed
	}
	return current
}

// Compact metadata shares the count varint. Indexed metadata fills one byte,
// followed by a fixed count. There is no object version or separate header.
func compactTag(n int, kind Kind, shared Wire) uint64 {
	return uint64(n)<<6 | uint64(shared)<<3 | uint64(kind)<<1
}
func indexedTag(kind Kind, shared Wire, width int) byte {
	return 1 | byte(kind)<<1 | byte(shared)<<3 | widthCode(width)<<6
}
func appendKey(dst []byte, delta uint64, w, shared Wire) []byte {
	if shared != mixed {
		return binary.AppendUvarint(dst, delta)
	}
	if delta <= math.MaxUint64>>3 {
		return binary.AppendUvarint(dst, delta<<3|uint64(w))
	}
	dst = append(dst, 7)
	dst = binary.AppendUvarint(dst, delta)
	return append(dst, byte(w))
}
func keyLen(delta uint64, shared Wire) int {
	if shared != mixed {
		return ulen(delta)
	}
	if delta <= math.MaxUint64>>3 {
		return ulen(delta << 3)
	}
	return 2 + ulen(delta)
}

// AppendNumeric encodes sorted unique fields, omitting zero values. Ordering is
// checked even for omitted entries. Invalid usage panics with a defined error.
func AppendNumeric(dst []byte, fields []Field, layout Layout) []byte {
	return appendNumeric(dst, numberCursor{fields: fields, direct: true}, layout)
}
func (c *numberCursor) nextStored() (Field, bool) {
	for {
		f, ok := c.next()
		if !ok || !f.Value.IsZero() {
			return f, ok
		}
	}
}
func appendNumeric(dst []byte, source numberCursor, layout Layout) []byte {
	if layout > Indexed {
		panic(ErrValue)
	}
	c := source
	n := 0
	shared := mixed
	var last, previous uint64
	first := true
	blobSize := 0
	for {
		f, ok := c.next()
		if !ok {
			break
		}
		if !f.Value.valid() {
			panic(ErrValue)
		}
		if !first && f.Key <= previous {
			panic(ErrOrder)
		}
		previous, first = f.Key, false
		if f.Value.IsZero() {
			continue
		}
		last = f.Key
		shared = selector(shared, f.Value.wire, n == 0)
		n++
		if n > MaxEntries {
			panic(ErrLimit)
		}
		if f.Value.wire == Bytes {
			if len(f.Value.data) > MaxBytes-blobSize {
				panic(ErrLimit)
			}
			blobSize += len(f.Value.data)
		}
	}
	start := len(dst)
	if layout == Compact {
		tag := compactTag(n, Numeric, shared)
		size := ulen(tag)
		var prev uint64
		c = source
		for {
			f, ok := c.nextStored()
			if !ok {
				break
			}
			size += keyLen(f.Key-prev, shared) + bodyLen(f.Value)
			if size > MaxBytes {
				panic(ErrLimit)
			}
			prev = f.Key
		}
		dst = binary.AppendUvarint(dst, tag)
		prev = 0
		c = source
		for {
			f, ok := c.nextStored()
			if !ok {
				break
			}
			dst = appendKey(dst, f.Key-prev, f.Value.wire, shared)
			dst = appendBody(dst, f.Value)
			prev = f.Key
		}
		return dst
	}
	kw := width(last)
	codes := 0
	if shared == mixed {
		codes = (n*3 + 7) / 8
	}
	fixed := 5 + n*kw + codes + n*8
	if fixed > MaxBytes-blobSize {
		panic(ErrLimit)
	}
	dst = append(dst, make([]byte, fixed)...)
	dst[start] = indexedTag(Numeric, shared, kw)
	binary.LittleEndian.PutUint32(dst[start+1:], uint32(n))
	i := 0
	c = source
	for {
		f, ok := c.nextStored()
		if !ok {
			break
		}
		putWidth(dst[start+5+i*kw:], f.Key, kw)
		if shared == mixed {
			putWire(dst[start+5+n*kw:], i, f.Value.wire)
		}
		pos := start + 5 + n*kw + codes + i*8
		if f.Value.wire == Bytes {
			binary.LittleEndian.PutUint32(dst[pos:], uint32(len(dst)-start))
			binary.LittleEndian.PutUint32(dst[pos+4:], uint32(len(f.Value.data)))
			dst = append(dst, f.Value.data...)
		} else {
			binary.LittleEndian.PutUint64(dst[pos:], f.Value.word)
		}
		i++
	}
	return dst
}
func AppendDictionary(dst []byte, pairs []Pair, layout Layout) []byte {
	return appendEntries(dst, pairs, nil, Dictionary, layout)
}
func AppendList(dst []byte, values []Value, layout Layout) []byte {
	return appendEntries(dst, nil, values, Sequence, layout)
}
func entryValue(pairs []Pair, values []Value, kind Kind, i int) Value {
	if kind == Dictionary {
		return pairs[i].Value
	}
	v := values[i]
	if !v.valid() {
		panic(ErrValue)
	}
	if v.IsZero() {
		return Value{}
	}
	return v
}
func appendEntries(dst []byte, pairs []Pair, values []Value, kind Kind, layout Layout) []byte {
	if layout > Indexed {
		panic(ErrValue)
	}
	count := len(values)
	if kind == Dictionary {
		count = len(pairs)
	}
	shared := mixed
	size, n := 0, 0
	for i := 0; i < count; i++ {
		v := entryValue(pairs, values, kind, i)
		if !v.valid() {
			panic(ErrValue)
		}
		if kind == Dictionary {
			if len(pairs[i].Key) > MaxBytes {
				panic(ErrLimit)
			}
			if i > 0 && pairs[i-1].Key >= pairs[i].Key {
				panic(ErrOrder)
			}
			if v.IsZero() {
				continue
			}
			size += ulen(uint64(len(pairs[i].Key))) + len(pairs[i].Key)
		}
		if len(v.data) > MaxBytes {
			panic(ErrLimit)
		}
		shared = selector(shared, v.wire, n == 0)
		size += bodyLen(v)
		n++
		if size > MaxBytes || n > MaxEntries {
			panic(ErrLimit)
		}
	}
	if shared == mixed {
		size += n
	}
	start := len(dst)
	ow, payloadStart := 0, 0
	if layout == Compact {
		tag := compactTag(n, kind, shared)
		if size+ulen(tag) > MaxBytes {
			panic(ErrLimit)
		}
		dst = binary.AppendUvarint(dst, tag)
	} else {
		ow = width(uint64(size))
		fixed := 5 + (n+1)*ow
		if fixed > MaxBytes-size {
			panic(ErrLimit)
		}
		dst = append(dst, make([]byte, fixed)...)
		dst[start] = indexedTag(kind, shared, ow)
		binary.LittleEndian.PutUint32(dst[start+1:], uint32(n))
		payloadStart = len(dst)
	}
	j := 0
	for i := 0; i < count; i++ {
		v := entryValue(pairs, values, kind, i)
		if kind == Dictionary && v.IsZero() {
			continue
		}
		if layout == Indexed {
			putWidth(dst[start+5+j*ow:], uint64(len(dst)-payloadStart), ow)
		}
		if kind == Dictionary {
			p := pairs[i]
			dst = binary.AppendUvarint(dst, uint64(len(p.Key)))
			dst = append(dst, p.Key...)
		}
		if shared == mixed {
			dst = append(dst, byte(v.wire))
		}
		dst = appendBody(dst, v)
		j++
	}
	if layout == Indexed {
		putWidth(dst[start+5+n*ow:], uint64(len(dst)-payloadStart), ow)
	}
	return dst
}
