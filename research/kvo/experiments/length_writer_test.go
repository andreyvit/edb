package kvolab

import (
	"encoding/binary"
	"math"
	"math/bits"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

// One destination, a metadata stack, and one byte-key arena per active depth.
// Both persistence layouts and all container kinds use this same builder.
type lengthWriter struct {
	out    []byte
	frames []lengthFrame
	depth  int
}
type lengthFrame struct {
	entries               []classMeta
	keys                  []byte
	last, parentKey       uint64
	start, rollback, kind int
	indexed, seen         bool
	shared                byte
}

func (w *lengthWriter) Reset(dst []byte) {

	w.out, w.depth = dst, 0
}
func (w *lengthWriter) prepare(key uint64, text string) (uint64, int) {
	f := &w.frames[w.depth-1]
	switch f.kind {
	case 0:
		if f.seen && key <= f.last {
			panic(draft.ErrOrder)
		}
	case 1:
		if f.seen {
			at, n := int(uint32(f.last)), int(f.last>>32)
			if string(f.keys[at:at+n]) >= text {
				panic(draft.ErrOrder)
			}
		}
	case 2:
		key = uint64(len(f.entries))
		f.last = uint64(len(f.keys)) | uint64(len(text))<<32
		f.keys = append(f.keys, text...)
		if f.indexed {
			key = uint64(len(w.out) - f.start)
		}
	default:
		panic(draft.ErrKind)
	}
	f.seen, f.last = true, key
	rollback := len(w.out)
	if f.kind == 1 {
		if len(text) > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		f.last = uint64(len(f.keys)) | uint64(len(text))<<32
		f.keys = append(f.keys, text...)
		if f.indexed {
			key = uint64(len(w.out) - f.start)
			w.out = binary.AppendUvarint(w.out, uint64(len(text)))
			w.out = append(w.out, text...)
		} else {
			key = f.last
		}
	}
	return key, rollback
}
func (w *lengthWriter) add(key uint64, code byte, word uint64) {
	f := &w.frames[w.depth-1]
	if f.indexed && code != 9 {
		code = 8
	}
	if len(f.entries) == 0 {
		f.shared = code
	} else if f.shared != code {
		f.shared = 15
	}
	f.entries = append(f.entries, classMeta{key: key, word: word, wire: draft.Wire(code)})
	if len(f.entries) > tagMaxEntries || len(w.out)-f.start > tagMaxBytes {
		panic(draft.ErrLimit)
	}
}
func (w *lengthWriter) Begin(key uint64, text string, kind int, indexed bool) {
	if kind < 0 || kind > 2 {
		panic(draft.ErrKind)
	}
	rollback := len(w.out)
	if w.depth > 0 {
		key, rollback = w.prepare(key, text)
	}
	if w.depth == len(w.frames) {
		w.frames = append(w.frames, lengthFrame{})
	}
	old := &w.frames[w.depth]
	*old = lengthFrame{entries: old.entries[:0], keys: old.keys[:0], start: len(w.out), rollback: rollback, parentKey: key, kind: kind, indexed: indexed, shared: 15}
	w.depth++
}
func (w *lengthWriter) Put(key uint64, text string, value lengthValue) {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	key, rollback := w.prepare(key, text)
	f := &w.frames[w.depth-1]
	if value.IsZero() && f.kind != 2 {
		w.out = w.out[:rollback]
		return
	}
	code := byte((bits.Len64(value.num) + 7) / 8)
	word := value.num
	if value.ptr != nil {
		code = 9
		if value.num > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		word = uint64(len(w.out)-f.start) | value.num<<32
		w.out = append(w.out, value.Bytes()...)
	} else if f.indexed {
		code = 8
		if f.kind != 0 {
			w.out = binary.LittleEndian.AppendUint64(w.out, word)
		}
	} else {
		w.out = tagAppendWord(w.out, word, int(code))
	}
	w.add(key, code, word)
}

// ReserveEntries is optional capacity advice, not a count or framing promise.
// A source slice/model can supply it without sizing or encoding any child.
func (w *lengthWriter) ReserveEntries(n int) {
	if w.depth == 0 || n < 0 {
		panic(draft.ErrValue)
	}
	n = min(n, tagMaxEntries)
	f := &w.frames[w.depth-1]
	if n > cap(f.entries) {
		grown := make([]classMeta, len(f.entries), n)
		copy(grown, f.entries)
		f.entries = grown
	}
}

func (w *lengthWriter) End() {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) == 0 && w.depth > 1 {
		key, rollback := f.parentKey, f.rollback
		w.out = w.out[:rollback]
		w.depth--
		if w.frames[w.depth-1].kind == 2 {
			if w.frames[w.depth-1].indexed {
				w.out = binary.LittleEndian.AppendUint64(w.out, 0)
			}
			w.add(key, 0, 0)
		}
		return
	}
	if f.indexed {
		w.out = lengthAppendIndex(w.out, *f)
	} else {
		w.out = lengthAppendCompact(w.out, *f)
	}
	n := len(w.out) - f.start
	if n > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	key, start := f.parentKey, f.start
	w.depth--
	if w.depth > 0 {
		w.add(key, 9, uint64(start-w.frames[w.depth-1].start)|uint64(n)<<32)
	}
}
func (w *lengthWriter) BytesWritten() []byte {
	if w.depth != 0 {
		panic(draft.ErrKind)
	}
	return w.out
}
func lengthAppendCompact(dst []byte, f lengthFrame) []byte {
	cat := len(dst)
	dst = binary.AppendUvarint(dst, uint64(len(f.entries))<<4|uint64(f.shared))
	var previous uint64
	for _, m := range f.entries {
		if f.kind == 0 {
			delta := m.key - previous
			if f.shared != 15 {
				dst = binary.AppendUvarint(dst, delta)
			} else if delta <= math.MaxUint64>>4 {
				dst = binary.AppendUvarint(dst, delta<<4|uint64(m.wire))
			} else {
				dst = append(dst, 15)
				dst = binary.AppendUvarint(dst, delta)
				dst = append(dst, byte(m.wire))
			}
			previous = m.key
		} else {
			if f.kind == 1 {
				start, n := int(uint32(m.key)), int(m.key>>32)
				dst = binary.AppendUvarint(dst, uint64(n))
				dst = append(dst, f.keys[start:start+n]...)
			}
			if f.shared == 15 {
				dst = append(dst, byte(m.wire))
			}
		}
		if m.wire == 9 {
			dst = binary.AppendUvarint(dst, m.word>>32)
		}
	}
	return tagAppendTail(dst, uint64(len(dst)-cat)<<3|uint64(f.kind)<<1)
}
func lengthAppendIndex(dst []byte, f lengthFrame) []byte {
	width := 1
	if len(f.entries) > 0 {
		width = tagKeyWidth(f.entries[len(f.entries)-1].key)

	}
	for _, m := range f.entries {
		dst = tagAppendWord(dst, m.key, width)
	}
	mode := byte(0)
	if f.shared == 9 {
		mode = 1
	} else if f.shared == 15 && len(f.entries) > 0 {
		mode = 2
		at := len(dst)
		dst = append(dst, make([]byte, (len(f.entries)+7)/8)...)
		for i, m := range f.entries {
			if m.wire == 9 {
				dst[at+i/8] |= 1 << uint(i%8)
			}
		}
	}
	if f.kind == 0 {
		for _, m := range f.entries {
			dst = binary.LittleEndian.AppendUint64(dst, m.word)
		}
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(f.entries)))
	return append(dst, 1|byte(f.kind)<<1|mode<<3|byte(bits.Len(uint(width))-1)<<6)
}
func tagKeyWidth(k uint64) int {
	switch {
	case k <= 255:
		return 1
	case k <= 65535:
		return 2
	case k <= math.MaxUint32:
		return 4
	default:
		return 8
	}
}

// A fixture adapter, kept separate from the builder. Depth policy permits mixed
// child layouts; it is not automatic runtime representation switching.
func lengthEncode(w *lengthWriter, dst []byte, v Value, policy func(depth int, v Value) bool) []byte {
	w.Reset(dst)
	var walk func(uint64, string, Value, int)
	walk = func(key uint64, text string, x Value, depth int) {
		switch x.Kind {
		case Record, Dict, List:
			kind := 0
			if x.Kind == Dict {
				kind = 1
			} else if x.Kind == List {
				kind = 2
			}
			indexed := policy != nil && policy(depth, x)
			w.Begin(key, text, kind, indexed)
			switch x.Kind {
			case Record:
				w.ReserveEntries(len(x.Fields))
				for _, f := range x.Fields {
					walk(f.Key, "", f.Value, depth+1)
				}
			case Dict:
				w.ReserveEntries(len(x.Entries))
				for _, p := range x.Entries {
					walk(0, p.Key, p.Value, depth+1)
				}
			case List:
				w.ReserveEntries(len(x.Items))
				for _, v := range x.Items {
					walk(0, "", v, depth+1)
				}
			}
			w.End()
		default:
			word := x.U
			switch x.Kind {
			case Null, False:
				word = 0
			case True:
				word = 1
			case Int:
				i := int64(x.U)
				word = uint64(i<<1) ^ uint64(i>>63)
			case Float:
				if math.Float64frombits(x.U) == 0 {
					word = 0
				}
			case Text:
				w.Put(key, text, lengthBytes(tagStringBytes(x.S)))
				return
			case Blob:
				w.Put(key, text, lengthBytes(x.B))
				return
			}
			w.Put(key, text, lengthWord(word))
		}
	}
	walk(0, "", v, 0)
	return w.BytesWritten()
}
