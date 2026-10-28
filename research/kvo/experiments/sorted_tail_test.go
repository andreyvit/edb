package kvolab

// Retained baseline plus a last-key shortcut for ordered construction.
import (
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math/bits"
	"testing"
)

func BenchmarkBatchRefinements(b *testing.B) {
	for _, name := range []string{"object64", "object128", "dictionary64"} {
		c := batchFixtures(b)[name]
		for _, pattern := range []string{"few4", "wide70", "empty-sorted", "empty-shuffled", "interleave-random"} {
			p := pattern
			if p == "interleave-random" {
				p = "wide70"
			}
			edits := batchEdits(c, p)
			all := batchEdits(c, "empty-sorted")
			for _, mode := range []string{"sorted", "tail", "append", "lazy", "hash"} {
				b.Run(name+"/"+pattern+"/"+mode, func(b *testing.B) {
					base := lengthTrusted(c.raw)
					kind := base.Kind()
					if pattern == "empty-sorted" || pattern == "empty-shuffled" {
						base = lengthView{}
					}
					var e batchEditor
					var w lengthWriter
					var out []byte
					run := func() {
						e.Reset(base, kind, mode)
						for i, m := range edits {
							e.Set(m.key, m.text, m.value, false)
							if pattern == "interleave-random" {
								q := all[(i*17+3)%len(all)]
								sinkValue = e.Get(q.key, q.text)
							}
						}
						out = e.Append(&w, out[:0], true)
					}
					run()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run()
					}
					b.StopTimer()
					sinkBytes = out
					b.ReportMetric(float64(e.ScratchBytes()), "scratch-B")
				})
			}
		}
	}
}

func sortedTailStore(e *lengthEditor, key uint64, text string, value lengthValue, deleted bool) {
	if e.kind == 2 && key >= uint64(e.base.Len()) {
		panic(draft.ErrIndex)
	}
	i := len(e.edits)
	if i > 0 && e.compare(e.edits[i-1], key, text) >= 0 {
		if e.compare(e.edits[i-1], key, text) == 0 {
			i--
		} else {
			i = e.locate(key, text)
		}
	}
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
