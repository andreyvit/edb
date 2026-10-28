package kvolab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

// One overlay representation for numeric maps, dictionaries, and sequence
// replacements/deletions. Public list indices address the current sequence;
// the overlay translates them to stable base positions internally. Insertion,
// splicing and a recursive path API are outside this skeleton.
type lengthEditor struct {
	base    lengthView
	edits   []classMeta
	arena   []byte
	kind    int
	deleted int
}

func newLengthEditor(base lengthView, scratch []classMeta, arena []byte) lengthEditor {
	return lengthEditor{base: base, edits: scratch[:0], arena: arena[:0], kind: base.Kind()}
}
func (e *lengthEditor) span(at uint64) []byte   { n, b := lengthU(e.arena[int(at):]); return b[:int(n)] }
func (e *lengthEditor) text(m classMeta) []byte { return e.span(m.key) }
func (e *lengthEditor) copySpan(src []byte) uint64 {
	at := uint64(len(e.arena))
	var header [10]byte
	n := binary.PutUvarint(header[:], uint64(len(src)))
	e.copyArena(header[:n])
	e.copyArena(src)
	return at
}
func (e *lengthEditor) compare(m classMeta, key uint64, text string) int {
	if e.kind == 1 {
		return bytes.Compare(e.text(m), tagStringBytes(text))
	}
	if m.key < key {
		return -1
	}
	if m.key > key {
		return 1
	}
	return 0
}
func (e *lengthEditor) locate(key uint64, text string) int {
	lo, hi := 0, len(e.edits)
	for lo < hi {
		m := lo + (hi-lo)/2
		if e.compare(e.edits[m], key, text) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}
func (e *lengthEditor) store(key uint64, text string, value lengthValue, deleted bool) {
	if e.kind == 2 && key >= uint64(e.base.Len()) {
		panic(draft.ErrIndex)
	}
	i := e.locate(key, text)
	existing := i < len(e.edits) && e.compare(e.edits[i], key, text) == 0
	m := classMeta{key: key, word: value.num, wire: draft.Wire((bits.Len64(value.num) + 7) / 8)}
	if e.kind == 1 {
		if existing {
			m.key = e.edits[i].key
		} else {
			m.key = e.copySpan(tagStringBytes(text))
		}
	}
	if deleted || e.kind != 2 && value.IsZero() {
		m.wire = 10
		m.word = 0
	} else if value.ptr != nil {
		m.wire = 9
		m.word = e.copySpan(value.Bytes())
	}
	if existing {
		e.edits[i] = m
	} else {
		if len(e.edits) == cap(e.edits) {
			grown := make([]classMeta, len(e.edits), max(8, len(e.edits)*2))
			copy(grown, e.edits)
			e.edits = grown
		}
		e.edits = e.edits[:len(e.edits)+1]
		copy(e.edits[i+1:], e.edits[i:])
		e.edits[i] = m
	}
}
func (e *lengthEditor) copyArena(src []byte) {
	if len(src) > int(^uint(0)>>1)-len(e.arena) {
		panic(draft.ErrLimit)
	}
	n := len(e.arena) + len(src)
	if n > cap(e.arena) {
		grown := make([]byte, len(e.arena), max(n, max(64, 2*cap(e.arena))))
		copy(grown, e.arena)
		e.arena = grown
	}
	start := len(e.arena)
	e.arena = e.arena[:n]
	copy(e.arena[start:], src)
}
func (e *lengthEditor) Len() int {
	if e.kind != 2 {
		panic(draft.ErrKind)
	}
	return e.base.Len() - e.deleted
}
func (e *lengthEditor) baseIndex(index uint64) uint64 {
	if index >= uint64(e.Len()) {
		panic(draft.ErrIndex)
	}
	for _, m := range e.edits {
		if m.key > index {
			break
		}
		if m.wire == 10 {
			index++
		}
	}
	return index
}
func (e *lengthEditor) Set(key uint64, value lengthValue) {
	if e.kind == 1 {
		panic(draft.ErrKind)
	}
	if e.kind == 2 {
		key = e.baseIndex(key)
	}
	e.store(key, "", value, false)
}
func (e *lengthEditor) SetString(key string, value lengthValue) {
	if e.kind != 1 {
		panic(draft.ErrKind)
	}
	e.store(0, key, value, false)
}
func (e *lengthEditor) Delete(key uint64) {
	if e.kind == 1 {
		panic(draft.ErrKind)
	}
	if e.kind == 2 {
		key = e.baseIndex(key)
		e.deleted++
	}
	e.store(key, "", lengthValue{}, true)
}
func (e *lengthEditor) DeleteString(key string) {
	if e.kind != 1 {
		panic(draft.ErrKind)
	}
	e.store(0, key, lengthValue{}, true)
}
func (e *lengthEditor) value(m classMeta) lengthValue {
	if m.wire == 10 {
		return lengthValue{}
	}
	if m.wire == 9 {
		return lengthBytes(e.span(m.word))
	}
	return lengthWord(m.word)
}
func (e *lengthEditor) Get(key uint64, text string) lengthValue {
	if e.kind == 2 {
		key = e.baseIndex(key)
	}
	i := e.locate(key, text)
	if i < len(e.edits) && e.compare(e.edits[i], key, text) == 0 {
		return e.value(e.edits[i])
	}
	switch e.kind {
	case 0:
		return e.base.Get(key)
	case 1:
		return e.base.GetString(text)
	default:
		return e.base.At(int(key))
	}
}
func (e lengthEditor) Append(w *lengthWriter, dst []byte, indexed bool) []byte {
	w.Reset(dst)
	w.Begin(0, "", e.kind, indexed)
	it := e.base.Iter()
	key, text, value, ok := it.Next()
	i := 0
	emitEdit := func(m classMeta) {
		if m.wire == 10 {
			return
		}
		s := ""
		if e.kind == 1 {
			s = unsafe.String(unsafe.SliceData(e.text(m)), len(e.text(m)))
		}
		w.Put(m.key, s, e.value(m))
	}
	for ok || i < len(e.edits) {
		cmp := 1
		if i < len(e.edits) {
			if !ok {
				cmp = -1
			} else {
				cmp = e.compare(e.edits[i], key, unsafe.String(unsafe.SliceData(text), len(text)))
			}
		}
		if cmp <= 0 {
			emitEdit(e.edits[i])
			i++
		}
		if cmp >= 0 {
			if cmp > 0 {
				w.Put(key, unsafe.String(unsafe.SliceData(text), len(text)), value)
			}
			key, text, value, ok = it.Next()
		}
	}
	w.End()
	return w.BytesWritten()
}

func TestLengthEditor(t *testing.T) {
	sources := []Value{
		Obj(Field{1, U(7)}, Field{2, S("keep opaque")}, Field{3, U(8)}),
		{Kind: Dict, Entries: []Entry{{"a", U(7)}, {"b", S("keep opaque")}, {"c", U(8)}}},
		{Kind: List, Items: []Value{U(7), S("keep opaque"), U(8)}},
	}
	for _, source := range sources {
		for _, indexed := range []bool{false, true} {
			var w lengthWriter
			raw := lengthEncode(&w, nil, source, func(int, Value) bool { return indexed })
			original := bytes.Clone(raw)
			view, err := lengthOpen(raw)
			if err != nil {
				t.Fatal(err)
			}
			var scratch [4]classMeta
			var arena [64]byte
			e := newLengthEditor(view, scratch[:0], arena[:0])
			switch source.Kind {
			case Record:
				e.Set(1, lengthWord(99))
				e.Set(3, lengthWord(0))
				e.Set(4, lengthBytes([]byte("new")))
			case Dict:
				e.SetString("a", lengthWord(99))
				e.SetString("c", lengthWord(0))
				e.SetString("d", lengthBytes([]byte("new")))
			case List:
				e.Set(0, lengthWord(0))
				e.Delete(2)
			}
			for _, outIndex := range []bool{false, true} {
				out := e.Append(&w, nil, outIndex)
				v, err := lengthOpen(out)
				if err != nil {
					t.Fatal(err)
				}
				switch source.Kind {
				case Record:
					if v.Get(1).Uint() != 99 || string(v.Get(2).Bytes()) != "keep opaque" || !v.Get(3).IsZero() || string(v.Get(4).Bytes()) != "new" {
						t.Fatal("numeric")
					}
				case Dict:
					if v.GetString("a").Uint() != 99 || string(v.GetString("b").Bytes()) != "keep opaque" || !v.GetString("c").IsZero() || string(v.GetString("d").Bytes()) != "new" {
						t.Fatal("dict")
					}
				case List:
					if v.Len() != 2 || !v.At(0).IsZero() || string(v.At(1).Bytes()) != "keep opaque" {
						t.Fatal("list")
					}
				}
			}
			if !bytes.Equal(raw, original) {
				t.Fatal("mutated borrowed base")
			}
		}
	}
	// Growth replaces fixed scratch automatically; setters never return capacity errors.
	e := newLengthEditor(lengthView{}, nil, nil)
	for i := uint64(1); i <= 100; i++ {
		e.Set(i, lengthBytes(bytes.Repeat([]byte{byte(i)}, 100)))
	}
	var w lengthWriter
	raw := e.Append(&w, nil, false)
	if v, err := lengthOpen(raw); err != nil || v.Len() != 100 {
		t.Fatal("growth", err)
	}
}
func BenchmarkLengthEdit(b *testing.B) {
	source := numeric(128, 0)
	var w lengthWriter
	for _, indexed := range []bool{false, true} {
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return indexed })
		view, _ := lengthOpen(raw)
		for _, count := range []int{1, 8, 64} {
			b.Run(fmt.Sprintf("index%v/edits%d", indexed, count), func(b *testing.B) {
				var scratch [64]classMeta
				var arena [128]byte
				e := newLengthEditor(view, scratch[:0], arena[:0])
				for j := 0; j < count; j++ {
					e.Set(uint64(j+1), lengthWord(uint64(j+999)))
				}
				dst := e.Append(&w, nil, indexed)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					e = newLengthEditor(view, scratch[:0], arena[:0])
					for j := 0; j < count; j++ {
						e.Set(uint64(j+1), lengthWord(uint64(j+999)))
					}
					dst = e.Append(&w, dst[:0], indexed)
				}
				sinkBytes = dst
			})
		}
	}
}

func TestLengthListLogicalEdits(t *testing.T) {
	var w lengthWriter
	raw := lengthEncode(&w, nil, Array(U(1), U(2), U(3), U(4)), nil)
	var scratch [8]classMeta
	e := newLengthEditor(lengthTrusted(raw), scratch[:0], nil)
	e.Delete(0)
	e.Set(1, lengthWord(99))
	if e.Len() != 3 || e.Get(1, "").Uint() != 99 {
		t.Fatal("indices did not shift")
	}
	e.Delete(1)
	e.Set(1, lengthWord(0))
	if e.Len() != 2 || !e.Get(1, "").IsZero() {
		t.Fatal("zero removed position")
	}
	out := e.Append(&w, nil, false)
	checkLengthTree(t, Array(U(2), U(0)), out)
	e.Delete(0)
	e.Delete(0)
	out = e.Append(&w, nil, false)
	checkLengthTree(t, Array(), out)
}
