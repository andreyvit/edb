// Package kvolab contains disposable representation experiments, not a storage API.
package kvolab

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

type Kind byte

const (
	Null Kind = iota
	False
	True
	Uint
	Int
	Float
	Text
	Blob
	Record
	Dict
	List
	IndexedRecord
	IndexedDict
	OffsetRecord
	OffsetDict
)

// Value is fixture/build data. Benchmarked views do not construct Value trees.
type Value struct {
	Kind    Kind
	U       uint64
	S       string
	B       []byte
	Fields  []Field
	Entries []Entry
	Items   []Value
}
type Field struct {
	Key   uint64
	Value Value
}
type Entry struct {
	Key   string
	Value Value
}
type Policy byte

const (
	Compact Policy = iota
	Indexed
	Offsets
)

func U(v uint64) Value        { return Value{Kind: Uint, U: v} }
func I(v int64) Value         { return Value{Kind: Int, U: uint64(v)} }
func S(v string) Value        { return Value{Kind: Text, S: v} }
func Obj(fs ...Field) Value   { return Value{Kind: Record, Fields: fs} }
func Map(es ...Entry) Value   { return Value{Kind: Dict, Entries: es} }
func Array(vs ...Value) Value { return Value{Kind: List, Items: vs} }
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
func putWidth(b []byte, v uint64, w int) {
	switch w {
	case 1:
		b[0] = byte(v)
	case 2:
		binary.LittleEndian.PutUint16(b, uint16(v))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(v))
	case 8:
		binary.LittleEndian.PutUint64(b, v)
	default:
		panic("width")
	}
}
func getWidth(b []byte, w int) uint64 {
	switch w {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	default:
		panic("width")
	}
}
func takeU(b []byte) (uint64, []byte) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		panic("varint")
	}
	return v, b[n:]
}
func frame(dst []byte, k Kind, body []byte) []byte {
	dst = append(dst, byte(k))
	dst = binary.AppendUvarint(dst, uint64(len(body)))
	return append(dst, body...)
}

// Encode uses one policy for this node. Children independently use childPolicy.
// Indexed numeric and string maps use 9-byte tagged cells and local references.
func Encode(v Value, p Policy) []byte                   { return appendValue(nil, v, p, p) }
func EncodeMixed(v Value, root, children Policy) []byte { return appendValue(nil, v, root, children) }
func appendValue(dst []byte, v Value, p, childPolicy Policy) []byte {
	switch v.Kind {
	case Null, False, True:
		return append(dst, byte(v.Kind))
	case Uint:
		dst = append(dst, byte(Uint))
		return binary.AppendUvarint(dst, v.U)
	case Int:
		dst = append(dst, byte(Int))
		return binary.AppendVarint(dst, int64(v.U))
	case Float:
		dst = append(dst, byte(v.Kind))
		return binary.LittleEndian.AppendUint64(dst, v.U)
	case Text:
		return frame(dst, Text, []byte(v.S))
	case Blob:
		return frame(dst, Blob, v.B)
	case Record:
		if p == Indexed {
			return appendIndexed(dst, v, childPolicy)
		}
		if p == Offsets {
			return appendOffsets(dst, v, childPolicy)
		}
		body := binary.AppendUvarint(nil, uint64(len(v.Fields)))
		var previous uint64
		for i, f := range v.Fields {
			if i > 0 && f.Key <= previous {
				panic("unsorted/duplicate key")
			}
			encoded := appendValue(nil, f.Value, childPolicy, childPolicy)
			kind := Kind(encoded[0])
			delta := f.Key - previous
			if delta > math.MaxUint64>>4 || kind >= 15 {
				body = append(body, 15)
				body = binary.AppendUvarint(body, delta)
				body = append(body, byte(kind))
			} else {
				body = binary.AppendUvarint(body, delta<<4|uint64(kind))
			}
			body = append(body, encoded[1:]...)
			previous = f.Key
		}
		return frame(dst, Record, body)
	case Dict:
		if p == Offsets {
			return appendOffsetDict(dst, v, childPolicy)
		}
		if p == Indexed {
			return appendIndexed(dst, v, childPolicy)
		}
		body := binary.AppendUvarint(nil, uint64(len(v.Entries)))
		for i, e := range v.Entries {
			if i > 0 && e.Key <= v.Entries[i-1].Key {
				panic("unsorted/duplicate key")
			}
			body = binary.AppendUvarint(body, uint64(len(e.Key)))
			body = append(body, e.Key...)
			body = appendValue(body, e.Value, childPolicy, childPolicy)
		}
		return frame(dst, Dict, body)
	case List:
		body := binary.AppendUvarint(nil, uint64(len(v.Items)))
		for _, v := range v.Items {
			body = appendValue(body, v, childPolicy, childPolicy)
		}
		return frame(dst, List, body)
	default:
		panic("fixture kind")
	}
}
func scalar(k Kind) bool { return k <= Float }
func appendIndexed(dst []byte, v Value, children Policy) []byte {
	n := len(v.Fields)
	kw := 1
	k := IndexedRecord
	if v.Kind == Dict {
		n = len(v.Entries)
		kw = 8
		k = IndexedDict
	} else if n > 0 {
		kw = width(v.Fields[n-1].Key)
	}
	// Header: count:uvarint keywidth:byte, keys[n], tagged cells[n], payload.
	body := binary.AppendUvarint(nil, uint64(n))
	body = append(body, byte(kw))
	keys := len(body)
	cells := keys + n*kw
	body = append(body, make([]byte, n*(kw+9))...)
	for i := 0; i < n; i++ {
		var val Value
		if v.Kind == Dict {
			e := v.Entries[i]
			val = e.Value
			off := len(body)
			body = append(body, e.Key...)
			putWidth(body[keys+i*kw:], uint64(off)<<32|uint64(len(e.Key)), kw)
		} else {
			f := v.Fields[i]
			val = f.Value
			putWidth(body[keys+i*kw:], f.Key, kw)
		}
		cell := cells + i*9
		body[cell] = byte(val.Kind)
		if scalar(val.Kind) {
			binary.LittleEndian.PutUint64(body[cell+1:], val.U)
		} else {
			off := len(body)
			body = appendValue(body, val, children, children)
			binary.LittleEndian.PutUint64(body[cell+1:], uint64(off)<<32|uint64(len(body)-off))
		}
	}
	return frame(dst, k, body)
}
func appendOffsets(dst []byte, v Value, children Policy) []byte {
	n := len(v.Fields)
	kw := 1
	if n > 0 {
		kw = width(v.Fields[n-1].Key)
	}
	values := make([]byte, 0)
	offsets := make([]int, 0, n+1)
	for _, f := range v.Fields {
		offsets = append(offsets, len(values))
		values = appendValue(values, f.Value, children, children)
	}
	offsets = append(offsets, len(values))
	ow := width(uint64(len(values)))
	if ow == 8 {
		panic("too large")
	}
	body := binary.AppendUvarint(nil, uint64(n))
	body = append(body, byte(kw), byte(ow))
	for _, f := range v.Fields {
		off := len(body)
		body = append(body, make([]byte, kw)...)
		putWidth(body[off:], f.Key, kw)
	}
	for _, o := range offsets {
		off := len(body)
		body = append(body, make([]byte, ow)...)
		putWidth(body[off:], uint64(o), ow)
	}
	body = append(body, values...)
	return frame(dst, OffsetRecord, body)
}

// View carries one slice. Every value starts with a varint tag; a numeric
// record entry fuses its key delta into the tag's high bits. A child view can
// therefore include the entry tag and still be read without its parent.
type View []byte
type Cell struct {
	Kind Kind
	U    uint64
	Raw  View
}

func (c Cell) Uint() uint64  { return c.U }
func (c Cell) Object() View  { return c.Raw }
func (c Cell) Bytes() []byte { _, b := payload(c.Raw); return b }
func head(b []byte) (Kind, []byte) {
	tag, rest := takeU(b)
	if tag == 15 {
		_, rest = takeU(rest)
		return Kind(rest[0]), rest[1:]
	}
	return Kind(tag & 15), rest
}
func payload(v View) (Kind, []byte) { k, rest := head(v); n, b := takeU(rest); return k, b[:int(n)] }
func readBody(k Kind, b []byte) (Cell, []byte) {
	c := Cell{Kind: k}
	switch k {
	case Null, False, True:
		return c, b
	case Uint:
		c.U, b = takeU(b)
		return c, b
	case Int:
		v, n := binary.Varint(b)
		if n <= 0 {
			panic("varint")
		}
		c.U = uint64(v)
		return c, b[n:]
	case Float:
		c.U = binary.LittleEndian.Uint64(b)
		return c, b[8:]
	default:
		n, rest := takeU(b)
		return c, rest[int(n):]
	}
}
func readValue(b []byte) (Cell, []byte) {
	k, body := head(b)
	c, rest := readBody(k, body)
	c.Raw = b[:len(b)-len(rest)]
	return c, rest
}
func cellAt(cell, body []byte) Cell {
	k := Kind(cell[0])
	u := binary.LittleEndian.Uint64(cell[1:])
	if scalar(k) {
		return Cell{Kind: k, U: u}
	}
	off, n := int(u>>32), int(uint32(u))
	c, _ := readValue(body[off : off+n])
	return c
}
func (v View) Get(key uint64) (Cell, bool) {
	k, b := payload(v)
	n, rest := takeU(b)
	if k == IndexedRecord || k == OffsetRecord {
		kw := int(rest[0])
		rest = rest[1:]
		ow := 0
		if k == OffsetRecord {
			ow = int(rest[0])
			rest = rest[1:]
		}
		keys := rest[:int(n)*kw]
		lo, hi := 0, int(n)
		for lo < hi {
			m := (lo + hi) / 2
			if getWidth(keys[m*kw:], kw) < key {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo == int(n) || getWidth(keys[lo*kw:], kw) != key {
			return Cell{}, false
		}
		values := rest[len(keys):]
		if k == IndexedRecord {
			return cellAt(values[lo*9:], b), true
		}
		off, end := int(getWidth(values[lo*ow:], ow)), int(getWidth(values[(lo+1)*ow:], ow))
		values = values[(int(n)+1)*ow:]
		c, _ := readValue(values[off:end])
		return c, true
	}
	if k != Record {
		panic("not record")
	}
	var previous uint64
	for i := uint64(0); i < n; i++ {
		tag, tail := takeU(rest)
		delta := tag >> 4
		if tag == 15 {
			delta, _ = takeU(tail)
		}
		previous += delta
		c, end := readValue(rest)
		if previous == key {
			return c, true
		}
		if previous > key {
			return Cell{}, false
		}
		rest = end
	}
	return Cell{}, false
}
func (v View) GetString(key string) (Cell, bool) {
	k, b := payload(v)
	n, rest := takeU(b)
	if k == OffsetDict {
		ow := int(rest[0])
		offsets := rest[1:]
		entries := offsets[(int(n)+1)*ow:]
		lo, hi := 0, int(n)
		for lo < hi {
			m := (lo + hi) / 2
			off := int(getWidth(offsets[m*ow:], ow))
			l, t := takeU(entries[off:])
			if string(t[:int(l)]) < key {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo == int(n) {
			return Cell{}, false
		}
		off, end := int(getWidth(offsets[lo*ow:], ow)), int(getWidth(offsets[(lo+1)*ow:], ow))
		l, t := takeU(entries[off:end])
		if string(t[:int(l)]) != key {
			return Cell{}, false
		}
		c, _ := readValue(t[int(l):])
		return c, true
	}
	if k == IndexedDict {
		kw := int(rest[0])
		keys := rest[1:]
		lo, hi := 0, int(n)
		for lo < hi {
			m := (lo + hi) / 2
			ref := getWidth(keys[m*kw:], kw)
			text := b[int(ref>>32) : int(ref>>32)+int(uint32(ref))]
			if string(text) < key {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo == int(n) {
			return Cell{}, false
		}
		ref := getWidth(keys[lo*kw:], kw)
		if string(b[int(ref>>32):int(ref>>32)+int(uint32(ref))]) != key {
			return Cell{}, false
		}
		return cellAt(keys[int(n)*kw+lo*9:], b), true
	}
	if k != Dict {
		panic("not dictionary")
	}
	for i := uint64(0); i < n; i++ {
		l, tail := takeU(rest)
		text := tail[:int(l)]
		c, end := readValue(tail[int(l):])
		if string(text) == key {
			return c, true
		}
		if string(text) > key {
			return Cell{}, false
		}
		rest = end
	}
	return Cell{}, false
}
func (v View) Walk(fn func(uint64, Cell) bool) {
	k, b := payload(v)
	n, rest := takeU(b)
	if k == IndexedRecord {
		kw := int(rest[0])
		keys := rest[1:]
		for i := 0; i < int(n); i++ {
			if !fn(getWidth(keys[i*kw:], kw), cellAt(keys[int(n)*kw+i*9:], b)) {
				break
			}
		}
		return
	}
	if k == OffsetRecord {
		kw, ow := int(rest[0]), int(rest[1])
		keys := rest[2:]
		offsets := keys[int(n)*kw:]
		values := offsets[(int(n)+1)*ow:]
		for i := 0; i < int(n); i++ {
			off, end := int(getWidth(offsets[i*ow:], ow)), int(getWidth(offsets[(i+1)*ow:], ow))
			c, _ := readValue(values[off:end])
			if !fn(getWidth(keys[i*kw:], kw), c) {
				break
			}
		}
		return
	}
	if k != Record {
		panic("not record")
	}
	var previous uint64
	for i := uint64(0); i < n; i++ {
		tag, tail := takeU(rest)
		delta := tag >> 4
		if tag == 15 {
			delta, _ = takeU(tail)
		}
		previous += delta
		c, end := readValue(rest)
		if !fn(previous, c) {
			break
		}
		rest = end
	}
}

// SetExisting changes a scalar cell in caller-owned Indexed bytes. Never use on mmap bytes.
func (v View) SetExisting(key, value uint64) bool {
	k, b := payload(v)
	if k != IndexedRecord {
		panic("not indexed")
	}
	n, rest := takeU(b)
	kw := int(rest[0])
	keys := rest[1:]
	lo, hi := 0, int(n)
	for lo < hi {
		m := (lo + hi) / 2
		if getWidth(keys[m*kw:], kw) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == int(n) || getWidth(keys[lo*kw:], kw) != key {
		return false
	}
	cell := keys[int(n)*kw+lo*9:]
	if Kind(cell[0]) != Uint {
		panic("not uint")
	}
	binary.LittleEndian.PutUint64(cell[1:], value)
	return true
}
func Sort(v *Value) {
	sort.Slice(v.Fields, func(i, j int) bool { return v.Fields[i].Key < v.Fields[j].Key })
	sort.Slice(v.Entries, func(i, j int) bool { return v.Entries[i].Key < v.Entries[j].Key })
	for i := range v.Fields {
		Sort(&v.Fields[i].Value)
	}
	for i := range v.Entries {
		Sort(&v.Entries[i].Value)
	}
	for i := range v.Items {
		Sort(&v.Items[i])
	}
}
func (v View) String() string { return fmt.Sprintf("experimental view: %d bytes", len(v)) }

func appendOffsetDict(dst []byte, v Value, children Policy) []byte {
	entries := make([]byte, 0)
	offsets := make([]int, 0, len(v.Entries)+1)
	for _, e := range v.Entries {
		offsets = append(offsets, len(entries))
		entries = binary.AppendUvarint(entries, uint64(len(e.Key)))
		entries = append(entries, e.Key...)
		entries = appendValue(entries, e.Value, children, children)
	}
	offsets = append(offsets, len(entries))
	ow := width(uint64(len(entries)))
	body := binary.AppendUvarint(nil, uint64(len(v.Entries)))
	body = append(body, byte(ow))
	for _, o := range offsets {
		start := len(body)
		body = append(body, make([]byte, ow)...)
		putWidth(body[start:], uint64(o), ow)
	}
	body = append(body, entries...)
	return frame(dst, OffsetDict, body)
}
