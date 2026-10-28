package kvolab

import "encoding/binary"

// Words is a homogeneous scalar-map candidate. It deliberately excludes the
// per-value kind byte that a schema-known accessor need not carry.
// Header: count:u32 keywidth:u8 reserved:24, fixed-width sorted keys, u64 values.
type Words []byte

func EncodeWords(v Value) Words {
	n := len(v.Fields)
	kw := 1
	if n > 0 {
		kw = width(v.Fields[n-1].Key)
	}
	out := make([]byte, 8+n*(kw+8))
	binary.LittleEndian.PutUint32(out, uint32(n))
	out[4] = byte(kw)
	for i, f := range v.Fields {
		putWidth(out[8+i*kw:], f.Key, kw)
		binary.LittleEndian.PutUint64(out[8+n*kw+i*8:], f.Value.U)
	}
	return out
}
func (v Words) Get(key uint64) uint64 {
	n := int(binary.LittleEndian.Uint32(v))
	kw := int(v[4])
	keys := v[8:]
	lo, hi := 0, n
	for lo < hi {
		m := (lo + hi) / 2
		if getWidth(keys[m*kw:], kw) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == n || getWidth(keys[lo*kw:], kw) != key {
		return 0
	}
	return binary.LittleEndian.Uint64(keys[n*kw+lo*8:])
}
func (v Words) SetExisting(key, value uint64) bool {
	n := int(binary.LittleEndian.Uint32(v))
	kw := int(v[4])
	keys := v[8:]
	lo, hi := 0, n
	for lo < hi {
		m := (lo + hi) / 2
		if getWidth(keys[m*kw:], kw) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == n || getWidth(keys[lo*kw:], kw) != key {
		return false
	}
	binary.LittleEndian.PutUint64(keys[n*kw+lo*8:], value)
	return true
}

// WordEditor verifies bounded, pointer-free stack scratch. A production generic
// arena could use the same numeric cells with offsets into an owned byte arena.
type WordEdit struct{ Key, Value uint64 }
type WordEditor struct {
	Base  Words
	Edits [8]WordEdit
	Count int
}

func (e *WordEditor) Set(k, v uint64) bool {
	for i := 0; i < e.Count; i++ {
		if e.Edits[i].Key == k {
			e.Edits[i].Value = v
			return true
		}
	}
	if e.Count == len(e.Edits) {
		return false
	}
	e.Edits[e.Count] = WordEdit{k, v}
	e.Count++
	return true
}
func (e *WordEditor) Get(k uint64) uint64 {
	for i := 0; i < e.Count; i++ {
		if e.Edits[i].Key == k {
			return e.Edits[i].Value
		}
	}
	return e.Base.Get(k)
}
func (e *WordEditor) Append(dst []byte) []byte {
	off := len(dst)
	dst = append(dst, e.Base...)
	for i := 0; i < e.Count; i++ {
		if !Words(dst[off:]).SetExisting(e.Edits[i].Key, e.Edits[i].Value) {
			panic("word insertion not implemented")
		}
	}
	return dst
}
