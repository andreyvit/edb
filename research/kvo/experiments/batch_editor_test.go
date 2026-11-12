package kvolab

// Mutation controls share arena ownership and the same final merge writer.
// The added overlay modes support Object/Dictionary edits and List replacement;
// positional List deletion remains the existing editor's separate operation.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
	"github.com/cespare/xxhash/v2"
)

type batchChange struct {
	key, word uint64
	sequence  uint32
	wire      draft.Wire
}

func (m batchChange) meta() classMeta { return classMeta{key: m.key, word: m.word, wire: m.wire} }

type batchEditor struct {
	storage  lengthEditor
	changes  []batchChange
	table    []uint32
	mode     string
	sequence uint32
	dirty    bool
}

func (e *batchEditor) Reset(base lengthView, kind int, mode string) {
	e.storage = newLengthEditor(base, e.storage.edits[:0], e.storage.arena[:0])
	e.storage.kind = kind
	e.changes = e.changes[:0]
	clear(e.table)
	e.sequence = 0
	e.dirty = false
	e.mode = mode
}
func (e *batchEditor) text(m batchChange) []byte { return e.storage.span(m.key) }
func (e *batchEditor) compare(m batchChange, key uint64, text string) int {
	return e.storage.compare(m.meta(), key, text)
}
func (e *batchEditor) hash(key uint64, text string) uint64 {
	if e.storage.kind == 1 {
		return xxhash.Sum64String(text)
	}
	key ^= key >> 30
	key *= 0xbf58476d1ce4e5b9
	key ^= key >> 27
	key *= 0x94d049bb133111eb
	return key ^ (key >> 31)
}
func (e *batchEditor) index(key uint64, text string) (at, slot int) {
	mask := len(e.table) - 1
	slot = int(e.hash(key, text)) & mask
	for e.table[slot] != 0 {
		at = int(e.table[slot]) - 1
		if e.compare(e.changes[at], key, text) == 0 {
			return
		}
		slot = (slot + 1) & mask
	}
	return -1, slot
}
func (e *batchEditor) reindex() {
	n := max(16, 1<<bits.Len(uint(len(e.changes)*2)))
	if len(e.table) < n {
		e.table = make([]uint32, n)
	} else {
		clear(e.table)
	}
	for i, m := range e.changes {
		text := ""
		if e.storage.kind == 1 {
			b := e.text(m)
			text = unsafe.String(unsafe.SliceData(b), len(b))
		}
		_, slot := e.index(m.key, text)
		e.table[slot] = uint32(i + 1)
	}
}
func (e *batchEditor) Set(key uint64, text string, v lengthValue, keepZero bool) {
	e.store(key, text, v, false, keepZero)
}
func (e *batchEditor) Delete(key uint64, text string) {
	if e.storage.kind == 2 {
		panic(draft.ErrKind)
	}
	e.store(key, text, lengthValue{}, true, false)
}
func (e *batchEditor) store(key uint64, text string, v lengthValue, deleted, keepZero bool) {
	if e.mode == "tail" && !keepZero {
		sortedTailStore(&e.storage, key, text, v, deleted)
		return
	}
	if e.mode == "sorted" && !keepZero {
		e.storage.store(key, text, v, deleted)
		return
	}
	if e.mode == "sorted" || e.mode == "tail" {
		panic("presence control uses batch modes")
	}
	if e.storage.kind == 2 && key >= uint64(e.storage.base.Len()) {
		panic(draft.ErrIndex)
	}
	if e.sequence == math.MaxUint32 {
		e.normalize()
		for i := range e.changes {
			e.changes[i].sequence = uint32(i)
		}
		e.sequence = uint32(len(e.changes))
	}
	at, slot := -1, 0
	if e.mode == "hash" {
		if len(e.table) == 0 || (len(e.changes)+1)*2 >= len(e.table) {
			e.reindex()
		}
		at, slot = e.index(key, text)
	} else if e.mode == "upsert" {
		for i := len(e.changes) - 1; i >= 0; i-- {
			if e.compare(e.changes[i], key, text) == 0 {
				at = i
				break
			}
		}
	}
	m := batchChange{key: key, word: v.num, wire: draft.Wire((bits.Len64(v.num) + 7) / 8), sequence: e.sequence}
	e.sequence++
	if e.storage.kind == 1 {
		if at >= 0 {
			m.key = e.changes[at].key
		} else {
			m.key = e.storage.copySpan(tagStringBytes(text))
		}
	}
	if deleted || !keepZero && e.storage.kind != 2 && v.IsZero() {
		m.wire = 10
		m.word = 0
	} else if v.ptr != nil {
		m.wire = 9
		m.word = e.storage.copySpan(v.Bytes())
	}
	if at >= 0 {
		e.changes[at] = m
	} else {
		e.changes = append(e.changes, m)
		if e.mode == "hash" {
			e.table[slot] = uint32(len(e.changes))
		}
	}
	e.dirty = true
}
func (e *batchEditor) normalize() {
	if !e.dirty {
		return
	}
	slices.SortFunc(e.changes, func(a, b batchChange) int {
		if e.storage.kind == 1 {
			return bytes.Compare(e.text(a), e.text(b))
		}
		if a.key < b.key {
			return -1
		}
		if a.key > b.key {
			return 1
		}
		return 0
	})
	n := 0
	for i := 0; i < len(e.changes); {
		best := e.changes[i]
		j := i + 1
		for j < len(e.changes) {
			other := e.changes[j]
			same := other.key == best.key
			if e.storage.kind == 1 {
				same = bytes.Equal(e.text(other), e.text(best))
			}
			if !same {
				break
			}
			if other.sequence > best.sequence {
				best = other
			}
			j++
		}
		e.changes[n] = best
		n++
		i = j
	}
	e.changes = e.changes[:n]
	e.dirty = false
	if e.mode == "hash" {
		e.reindex()
	}
}
func (e *batchEditor) Get(key uint64, text string) lengthValue {
	if e.mode == "sorted" || e.mode == "tail" {
		return e.storage.Get(key, text)
	}
	if e.mode == "lazy" {
		e.normalize()
	}
	at := -1
	if e.mode == "hash" && len(e.table) > 0 {
		at, _ = e.index(key, text)
	} else if !e.dirty {
		lo, hi := 0, len(e.changes)
		for lo < hi {
			m := lo + (hi-lo)/2
			if e.compare(e.changes[m], key, text) < 0 {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo < len(e.changes) && e.compare(e.changes[lo], key, text) == 0 {
			at = lo
		}
	} else {
		for i := len(e.changes) - 1; i >= 0; i-- {
			if e.compare(e.changes[i], key, text) == 0 {
				at = i
				break
			}
		}
	}
	if at >= 0 {
		return e.storage.value(e.changes[at].meta())
	}
	switch e.storage.kind {
	case 0:
		return e.storage.base.Get(key)
	case 1:
		return e.storage.base.GetString(text)
	default:
		return e.storage.base.At(int(key))
	}
}
func batchPut(w *lengthWriter, key uint64, text string, v lengthValue) {
	if !v.IsZero() {
		w.Put(key, text, v)
		return
	}
	key, _ = w.prepare(key, text)
	f := &w.frames[w.depth-1]
	if f.indexed && f.kind != 0 {
		w.out = binary.LittleEndian.AppendUint64(w.out, 0)
	}
	w.add(key, 0, 0)
}

// visit emits a sorted merged view, preserving opaque base spans and explicit
// base zeros. An editor's ordinary Set-zero creates a deletion, not an ambiguity.
func (e *batchEditor) visit(emit func(uint64, string, lengthValue)) {
	if e.mode == "sorted" || e.mode == "tail" {
		e.changes = e.changes[:0]
		for _, m := range e.storage.edits {
			e.changes = append(e.changes, batchChange{key: m.key, word: m.word, wire: m.wire})
		}
		e.dirty = false
	} else {
		e.normalize()
	}
	it := e.storage.base.Iter()
	key, text, value, ok := it.Next()
	i := 0
	for ok || i < len(e.changes) {
		cmp := 1
		s := unsafe.String(unsafe.SliceData(text), len(text))
		if i < len(e.changes) {
			if !ok {
				cmp = -1
			} else {
				cmp = e.compare(e.changes[i], key, s)
			}
		}
		if cmp <= 0 {
			m := e.changes[i]
			if m.wire != 10 {
				k := m.key
				ms := ""
				if e.storage.kind == 1 {
					b := e.text(m)
					ms = unsafe.String(unsafe.SliceData(b), len(b))
					k = 0 // Dictionary identity is the string, not its arena offset.
				}
				emit(k, ms, e.storage.value(m.meta()))
			}
			i++
		}
		if cmp >= 0 {
			if cmp > 0 {
				emit(key, s, value)
			}
			key, text, value, ok = it.Next()
		}
	}
}
func (e *batchEditor) Append(w *lengthWriter, dst []byte, indexed bool) []byte {
	if e.mode == "sorted" || e.mode == "tail" {
		return e.storage.Append(w, dst, indexed)
	}
	w.Reset(dst)
	w.Begin(0, "", e.storage.kind, indexed)
	e.visit(func(key uint64, text string, v lengthValue) { batchPut(w, key, text, v) })
	w.End()
	return w.BytesWritten()
}
func (e *batchEditor) ScratchBytes() int {
	return cap(e.storage.arena) + 24*cap(e.storage.edits) + 24*cap(e.changes) + 4*cap(e.table)
}

func TestBatchEditor(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		for _, kind := range []int{0, 1} {
			source := numeric(32, 0)
			if kind == 1 {
				source = Value{Kind: Dict}
				for i := 0; i < 32; i++ {
					source.Entries = append(source.Entries, Entry{fmt.Sprintf("key%02d", i+1), U(uint64(i + 1))})
				}
			}
			var w lengthWriter
			raw := lengthEncode(&w, nil, source, func(int, Value) bool { return indexed })
			saved := bytes.Clone(raw)
			for _, mode := range []string{"sorted", "tail", "append", "lazy", "upsert", "hash"} {
				var e batchEditor
				e.Reset(lengthTrusted(raw), kind, mode)
				expected := map[uint64]uint64{}
				for i := uint64(1); i <= 32; i++ {
					if kind == 0 {
						expected[i] = source.Fields[i-1].Value.U
					} else {
						expected[i] = i
					}
				}
				for i := 0; i < 400; i++ {
					key := uint64((i*17)%48 + 1)
					text := fmt.Sprintf("key%02d", key)
					value := uint64(i + 100)
					if i%7 == 0 {
						value = 0
					}
					e.Set(key, text, lengthWord(value), false)
					expected[key] = value
					if i%3 == 0 {
						for k, want := range expected {
							if got := e.Get(k, fmt.Sprintf("key%02d", k)).Uint(); got != want {
								t.Fatalf("%s got %d want %d", mode, got, want)
							}
						}
					}
				}
				out := e.Append(&w, nil, indexed)
				view, err := lengthOpen(out)
				if err != nil {
					t.Fatal(err)
				}
				for k, want := range expected {
					var got uint64
					if kind == 0 {
						got = view.Get(k).Uint()
					} else {
						got = view.GetString(fmt.Sprintf("key%02d", k)).Uint()
					}
					if got != want {
						t.Fatal(mode, "merge")
					}
				}
				// Editing after a normalized/materialized view must still use the right hash slots.
				e.Set(1, "key01", lengthWord(77), false)
				if e.Get(1, "key01").Uint() != 77 {
					t.Fatal("after write")
				}
				out = e.Append(&w, out[:0], indexed)
				_ = out
				if !bytes.Equal(raw, saved) {
					t.Fatal("borrowed base changed")
				}
			}
		}
	}
	for _, mode := range []string{"append", "lazy", "upsert", "hash"} {
		var w lengthWriter
		w.Begin(0, "", 0, true)
		batchPut(&w, 1, "", lengthWord(0))
		w.Put(2, "", lengthBytes([]byte("opaque")))
		w.End()
		var e batchEditor
		e.Reset(lengthTrusted(w.BytesWritten()), 0, mode)
		e.Set(3, "", lengthWord(0), true)
		e.Set(4, "", lengthWord(4), false)
		out := e.Append(&w, nil, true)
		it := lengthTrusted(out).Iter()
		keys := []uint64{}
		for {
			k, _, _, ok := it.Next()
			if !ok {
				break
			}
			keys = append(keys, k)
		}
		if !slices.Equal(keys, []uint64{1, 2, 3, 4}) {
			t.Fatal("presence", keys)
		}
	}
	if unsafe.Sizeof(batchChange{}) != 24 {
		t.Fatal("entry size")
	}
}
