package kvolab

import (
	"encoding/binary"
	"math"
)

// Edit stores final assignments, not increments. Raw owns/borrows an encoded
// value; its lifetime must cover encoding. Scalar-only edits carry no pointers.
type Edit struct {
	Key    uint64
	Value  Cell
	Delete bool
}
type Editor struct {
	Base  View
	Edits []Edit
}

func (e *Editor) Get(key uint64) (Cell, bool) {
	for _, x := range e.Edits {
		if x.Key == key {
			return x.Value, !x.Delete
		}
		if x.Key > key {
			break
		}
	}
	return e.Base.Get(key)
}
func (e *Editor) Set(key uint64, value Cell) { e.set(Edit{Key: key, Value: value}) }
func (e *Editor) Delete(key uint64)          { e.set(Edit{Key: key, Delete: true}) }
func (e *Editor) set(edit Edit) {
	i := 0
	for i < len(e.Edits) && e.Edits[i].Key < edit.Key {
		i++
	}
	if i < len(e.Edits) && e.Edits[i].Key == edit.Key {
		e.Edits[i] = edit
		return
	}
	if c, ok := e.Base.Get(edit.Key); edit.Delete && !ok || !edit.Delete && ok && scalar(edit.Value.Kind) && c.Kind == edit.Value.Kind && c.U == edit.Value.U {
		return
	}
	e.Edits = append(e.Edits, Edit{})
	copy(e.Edits[i+1:], e.Edits[i:])
	e.Edits[i] = edit
}
func appendField(dst []byte, delta uint64, c Cell) []byte {
	if delta > math.MaxUint64>>4 || c.Kind >= 15 {
		dst = append(dst, 15)
		dst = binary.AppendUvarint(dst, delta)
		dst = append(dst, byte(c.Kind))
	} else {
		dst = binary.AppendUvarint(dst, delta<<4|uint64(c.Kind))
	}
	switch c.Kind {
	case Null, False, True:
		return dst
	case Uint:
		return binary.AppendUvarint(dst, c.U)
	case Int:
		return binary.AppendVarint(dst, int64(c.U))
	case Float:
		return binary.LittleEndian.AppendUint64(dst, c.U)
	default:
		_, body := head(c.Raw)
		return append(dst, body...)
	}
}

// AppendCompact merges sorted edits with the base once, without a key hash map
// or sorting the entire original object. It reuses opaque unknown subtrees.
func (e *Editor) AppendCompact(dst []byte) []byte {
	count := 0
	e.Base.Walk(func(_ uint64, _ Cell) bool { count++; return true })
	for _, edit := range e.Edits {
		_, exists := e.Base.Get(edit.Key)
		if edit.Delete && exists {
			count--
		} else if !edit.Delete && !exists {
			count++
		}
	}
	start := len(dst)
	dst = append(dst, make([]byte, 11)...)
	bodyStart := len(dst)
	dst = binary.AppendUvarint(dst, uint64(count))
	var previous uint64
	next := 0
	emit := func(k uint64, c Cell) { dst = appendField(dst, k-previous, c); previous = k }
	e.Base.Walk(func(k uint64, c Cell) bool {
		for next < len(e.Edits) && e.Edits[next].Key < k {
			x := e.Edits[next]
			if !x.Delete {
				emit(x.Key, x.Value)
			}
			next++
		}
		if next < len(e.Edits) && e.Edits[next].Key == k {
			x := e.Edits[next]
			if !x.Delete {
				emit(x.Key, x.Value)
			}
			next++
		} else {
			emit(k, c)
		}
		return true
	})
	for ; next < len(e.Edits); next++ {
		x := e.Edits[next]
		if !x.Delete {
			emit(x.Key, x.Value)
		}
	}
	var header [11]byte
	header[0] = byte(Record)
	n := 1 + binary.PutUvarint(header[1:], uint64(len(dst)-bodyStart))
	copy(dst[start:], header[:n])
	copy(dst[start+n:], dst[bodyStart:])
	return dst[:len(dst)-(11-n)]
}
