package fast1_test

import (
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/fast1"
	"testing"
)

// This candidate stays in the benchmark to show the dispatch cost avoided by
// resolving property codes before entering Fast1's raw word loops.
func candidateTypedGet[P interface{ Code() uint64 }, M interface{ Get(uint64) uint64 }](m M, prop P) int64 {
	return int64(m.Get(prop.Code()))
}

var typedWordSink int64

func BenchmarkTypedWords(b *testing.B) {
	s := kvo.NewSchema()
	p := kvo.NewSignedProp[int64](s, 1, "points", kvo.TInt64)
	m := fast1.NewRecord(nil)
	m.Set(p.Code(), 42)
	r := m.Record().PackedRoot()
	b.Run("Raw", func(b *testing.B) {
		var total int64
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			total += int64(r.Get(p.Code()))
		}
		typedWordSink = total
	})
	b.Run("Typed", func(b *testing.B) {
		var total int64
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			total += candidateTypedGet(r, p)
		}
		typedWordSink = total
	})
}
