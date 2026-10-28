package kvolab

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func lengthKeyFixtures() map[string]Value {
	all := map[string]Value{"local": numeric(128, 0), "clustered-high": numeric(128, 1<<50)}
	for _, width := range []int{32, 64} {
		v := numeric(128, 0)
		g := rand.New(rand.NewSource(int64(width)))
		for i := range v.Fields {
			v.Fields[i].Key = g.Uint64()
			if width == 32 {
				v.Fields[i].Key >>= 32
			}
		}
		sort.Slice(v.Fields, func(i, j int) bool { return v.Fields[i].Key < v.Fields[j].Key })
		all[fmt.Sprintf("random%d", width)] = v
	}
	return all
}
func TestLengthKeyDistributions(t *testing.T) {
	all := lengthKeyFixtures()
	all["full-key-mixed"] = Obj(Field{0, U(2)}, Field{math.MaxUint64, U(math.MaxUint64)})
	for name, source := range all {
		for _, indexed := range []bool{false, true} {
			layout := draft.Compact
			if indexed {
				layout = draft.Indexed
			}
			old := draftObject(source, layout, layout)
			var w lengthWriter
			raw := lengthEncode(&w, nil, source, func(int, Value) bool { return indexed })
			checkLengthTree(t, source, raw)
			if name != "full-key-mixed" {
				v, _ := lengthOpen(raw)
				if !v.Get(math.MaxUint64).IsZero() {
					t.Fatal("missing high key")
				}
			}
			t.Logf("%s indexed=%v previous=%d next=%d", name, indexed, len(old), len(raw))
		}
	}
}
func BenchmarkLengthKeyDistributions(b *testing.B) {
	all := lengthKeyFixtures()
	for _, name := range []string{"local", "clustered-high", "random32", "random64"} {
		source := all[name]
		for _, indexed := range []bool{false, true} {
			layout := draft.Compact
			if indexed {
				layout = draft.Indexed
			}
			old := draft.Trusted(draftObject(source, layout, layout))
			var w lengthWriter
			next := lengthTrusted(lengthEncode(&w, nil, source, func(int, Value) bool { return indexed }))
			for _, new := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/index%v/new%v", name, indexed, new), func(b *testing.B) {
					var sum uint64
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						key := source.Fields[query(i, 128)-1].Key
						if new {
							sum += next.Get(key).Uint()
						} else {
							sum += old.Get(key).Uint()
						}
					}
					sinkU = sum
				})
			}
		}
	}
}
func BenchmarkLengthRowCache(b *testing.B) {
	const rows = 32768
	source := numeric(128, 0)
	for _, indexed := range []bool{false, true} {
		layout := draft.Compact
		if indexed {
			layout = draft.Indexed
		}
		for _, next := range []bool{false, true} {
			b.Run(fmt.Sprintf("index%v/new%v", indexed, next), func(b *testing.B) {
				var w lengthWriter
				var raw []byte
				if next {
					raw = lengthEncode(&w, nil, source, func(int, Value) bool { return indexed })
				} else {
					raw = draftObject(source, layout, layout)
				}
				size := len(raw)
				data := make([]byte, size*rows)
				for i := 0; i < rows; i++ {
					copy(data[i*size:], raw)
				}
				b.ReportAllocs()
				b.ResetTimer()
				var sum uint64
				for i := 0; i < b.N; i++ {
					row := int(uint(i) * 2654435761 & (rows - 1))
					key := uint64(uint(i)*2246822519>>16&127) + 1
					src := data[row*size : (row+1)*size]
					if next {
						sum += lengthTrusted(src).Get(key).Uint()
					} else {
						sum += draft.Trusted(src).Get(key).Uint()
					}
				}
				sinkU = sum
				b.ReportMetric(float64(size), "row-B")
			})
		}
	}
}
