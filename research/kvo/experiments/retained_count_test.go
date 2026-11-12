package kvolab

// Retained reconstruction of the rejected kind-in-count-header catalog.
// The original experiment was overwritten before preservation was requested.
// This copy restores its grammar; new timings must not be called the exact
// historical implementation. Default classEncoder remains unchanged.

import (
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math"
	"math/bits"
	"testing"
)

type countKindEncoder classEncoder

func (e *countKindEncoder) object(dst []byte, v Value, depth int) []byte {
	for len(e.levels) <= depth {
		e.levels = append(e.levels, classLevel{})
	}
	entries := e.levels[depth].entries[:0]
	keys := e.levels[depth].keys[:0]
	start := len(dst)
	indexed := e.indexedRoot && depth == 0 && v.Kind == Record
	lengths := e.rawLengths && !indexed
	span, mixed, classBits := draft.Bytes, draft.Wire(7), uint(3)
	if lengths {
		span, mixed, classBits = 9, 15, 4
	}
	shared := mixed
	put := func(key uint64, text string, x Value) {
		if v.Kind != List && tagZero(x) {
			return
		}
		m := classMeta{key: key}
		if v.Kind == Dict {
			if len(text) > tagMaxBytes-len(keys) {
				panic(errTag)
			}
			m.key = uint64(len(keys)) | uint64(len(text))<<32
			keys = append(keys, text...)
		}
		p := len(dst)
		switch x.Kind {
		case Record, Dict, List:
			if !tagZero(x) {
				dst = e.object(dst, x, depth+1)
				m.wire = span
			}
		case Text:
			if x.S != "" {
				dst = append(dst, x.S...)
				m.wire = span
			}
		case Blob:
			if len(x.B) != 0 {
				dst = append(dst, x.B...)
				m.wire = span
			}
		default:
			if e.rawLengths {
				m.word = x.U
				switch x.Kind {
				case Null, False:
					m.word = 0
				case True:
					m.word = 1
				case Int:
					i := int64(x.U)
					m.word = uint64(i<<1) ^ uint64(i>>63)
				case Float:
					if math.Float64frombits(x.U) == 0 {
						m.word = 0
					}
				}
				if indexed {
					m.wire = draft.Fixed8 // indexed scalar slots are always words
					if m.word == 0 {
						m.wire = draft.Zero
					}
				} else {
					n := (bits.Len64(m.word) + 7) / 8
					m.wire = draft.Wire(n)
					dst = tagAppendWord(dst, m.word, n)
				}
			} else {
				value := draftValue(x, draft.Compact)
				m.wire, m.word = value.Wire(), value.Uint()
				if !indexed {
					switch m.wire {
					case draft.Varint:
						dst = binary.AppendUvarint(dst, m.word)
					case draft.Fixed1:
						dst = append(dst, byte(m.word))
					case draft.Fixed2:
						dst = binary.LittleEndian.AppendUint16(dst, uint16(m.word))
					case draft.Fixed4:
						dst = binary.LittleEndian.AppendUint32(dst, uint32(m.word))
					case draft.Fixed8:
						dst = binary.LittleEndian.AppendUint64(dst, m.word)
					}
				}
			}
		}
		if m.wire == span {
			m.word = uint64(p-start) | uint64(len(dst)-p)<<32
		}
		if len(entries) == 0 {
			shared = m.wire
		} else if shared != m.wire {
			shared = mixed
		}
		entries = append(entries, m)
		if len(entries) > tagMaxEntries || len(dst)-start > tagMaxBytes {
			panic(errTag)
		}
	}
	switch v.Kind {
	case Record:
		for i, x := range v.Fields {
			if i > 0 && x.Key <= v.Fields[i-1].Key {
				panic(errTag)
			}
			put(x.Key, "", x.Value)
		}
	case Dict:
		for i, x := range v.Entries {
			if i > 0 && x.Key <= v.Entries[i-1].Key {
				panic(errTag)
			}
			put(0, x.Key, x.Value)
		}
	case List:
		for _, x := range v.Items {
			put(0, "", x)
		}
	default:
		panic(errTag)
	}
	e.levels[depth].entries = entries
	e.levels[depth].keys = keys
	if indexed {
		return classAppendIndex(dst, entries, shared)
	}
	cat := len(dst)
	dst = binary.AppendUvarint(dst, uint64(len(entries))<<(classBits+2)|tagContainerKind(v.Kind)<<classBits|uint64(shared))
	var prev uint64
	for _, m := range entries {
		if v.Kind == Record {
			delta := m.key - prev
			if shared != mixed {
				dst = binary.AppendUvarint(dst, delta)
			} else if delta <= math.MaxUint64>>classBits {
				dst = binary.AppendUvarint(dst, delta<<classBits|uint64(m.wire))
			} else {
				dst = append(dst, byte(mixed))
				dst = binary.AppendUvarint(dst, delta)
				dst = append(dst, byte(m.wire))
			}
			prev = m.key
		} else {
			if v.Kind == Dict {
				start, n := int(uint32(m.key)), int(m.key>>32)
				dst = binary.AppendUvarint(dst, uint64(n))
				dst = append(dst, keys[start:start+n]...)
			}
			if shared == mixed {
				dst = append(dst, byte(m.wire))
			}
		}
		if m.wire == span {
			dst = binary.AppendUvarint(dst, m.word>>32)
		}
	}
	dst = tagAppendTail(dst, uint64(len(dst)-cat)<<1)
	if len(dst)-start > tagMaxBytes {
		panic(errTag)
	}
	return dst
}

func countKindIter(raw []byte, rawLengths ...bool) classIterator {
	tail, b := tagTail(raw)
	n := tail >> 1
	if tail&1 != 0 || n > uint64(len(b)) {
		panic(errTag)
	}
	payload, meta := b[:len(b)-int(n)], b[len(b)-int(n):]
	h, meta := tagReadU(meta)
	classBits := uint(3)
	lengths := len(rawLengths) != 0 && rawLengths[0]
	if lengths {
		classBits = 4
	}
	kind, count, shared := h>>classBits&3, h>>(classBits+2), h&((1<<classBits)-1)
	if kind == 3 || count > tagMaxEntries || lengths && shared > 9 && shared != 15 {
		panic(errTag)
	}
	return classIterator{meta: meta, payload: payload, kind: kind, shared: draft.Wire(shared), left: int(count), rawLengths: lengths}
}
func checkCountKindTree(t testing.TB, want Value, raw []byte, rawLengths ...bool) {
	t.Helper()
	it := countKindIter(raw, rawLengths...)
	check := func(w Value, c tagCell) {
		t.Helper()
		switch w.Kind {
		case Record, Dict, List:
			if tagZero(w) {
				if c.uint() != 0 {
					t.Fatal("empty")
				}
			} else {
				checkCountKindTree(t, w, c.raw(), rawLengths...)
			}
		case Text:
			if string(c.raw()) != w.S {
				t.Fatal("text")
			}
		case Blob:
			if string(c.raw()) != string(w.B) {
				t.Fatal("blob")
			}
		default:
			if c.uint() != draftValue(w, draft.Compact).Uint() {
				t.Fatal("word")
			}
		}
	}
	switch want.Kind {
	case Record:
		for _, x := range want.Fields {
			if tagZero(x.Value) {
				continue
			}
			k, _, c, ok := it.next()
			if !ok || k != x.Key {
				t.Fatal("key")
			}
			check(x.Value, c)
		}
	case Dict:
		for _, x := range want.Entries {
			if tagZero(x.Value) {
				continue
			}
			_, k, c, ok := it.next()
			if !ok || string(k) != x.Key {
				t.Fatal("string key")
			}
			check(x.Value, c)
		}
	case List:
		for _, x := range want.Items {
			_, _, c, ok := it.next()
			if !ok {
				t.Fatal("position")
			}
			check(x, c)
		}
	}
	if _, _, _, ok := it.next(); ok {
		t.Fatal("count")
	}
}

func TestRetainedCountKind(t *testing.T) {
	for name, source := range tagExtraFixtures() {
		for _, lengths := range []bool{false, true} {
			e := countKindEncoder{rawLengths: lengths}
			raw := e.object(nil, source, 0)
			checkCountKindTree(t, source, raw, lengths)
			base := classEncoder{rawLengths: lengths}
			t.Logf("%s lengths=%v kind-in-footer=%d kind-in-count=%d", name, lengths, len(base.object(nil, source, 0)), len(raw))
		}
	}
}
func BenchmarkRetainedCountKind(b *testing.B) {
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128"} {
		source := fixtures()[name]
		for _, lengths := range []bool{false, true} {
			for _, inCount := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/lengths%v/count%v", name, lengths, inCount), func(b *testing.B) {
					e := countKindEncoder{rawLengths: lengths}
					old := classEncoder{rawLengths: lengths}
					var out []byte
					if inCount {
						out = e.object(nil, source, 0)
					} else {
						out = old.object(nil, source, 0)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if inCount {
							out = e.object(out[:0], source, 0)
						} else {
							out = old.object(out[:0], source, 0)
						}
					}
					sinkBytes = out
					b.ReportMetric(float64(len(out)), "encoded-B")
				})
			}
		}
	}
}
