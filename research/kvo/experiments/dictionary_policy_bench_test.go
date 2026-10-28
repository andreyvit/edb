package kvolab

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

func dictionaryBenchSample(tb testing.TB, zeroEvery int) nativeDictionaryValues[int64] {
	src := nativeDictionaryValues[int64]{Items: make(map[string]int64, 32)}
	// Preserve the anonymous replay's nonuniform scalar widths. The injected
	// zero fraction is an explicit sensitivity case, not an observed workload.
	values := nativeCollectionSample(tb, "row-wide").Values
	for i := 0; i < 32; i++ {
		x := values[i%len(values)]
		if x == 0 {
			x = 1
		}
		if zeroEvery != 0 && i%zeroEvery == 0 {
			x = 0
		}
		src.Items[fmt.Sprintf("entry-%07d", i)] = x
	}
	return src
}

func BenchmarkDictionaryPolicy(b *testing.B) {
	for _, fast := range []bool{true, false} {
		for _, keep := range []bool{false, true} {
			for _, zeroEvery := range []int{0, 4} {
				b.Run(fmt.Sprintf("fast%v/keep%v/zeroEvery%d", fast, keep, zeroEvery), func(b *testing.B) {
					typ := nativeDictionaryValueType[int64](kvo.TInt64, keep)
					c := bindNative[nativeDictionaryValues[int64]](typ)
					p := bindCurrentDictionary(typ.Model(), 1)
					src := dictionaryBenchSample(b, zeroEvery)
					raw := c.Put(nil, &src, fast)
					d := newMmapDataset(b, raw, 1024)
					warmMmapPayloads(d)
					b.Run("get-reuse", func(b *testing.B) {
						var out nativeDictionaryValues[int64]
						c.GetFast(currentTrusted(d.rows[0]), &out)
						var sum uint64
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							c.GetFast(currentTrusted(d.rows[i&1023]), &out)
							sum += uint64(len(out.Items))
						}
						sinkU = sum
					})
					b.Run("put-reuse", func(b *testing.B) {
						out := c.Put(bytes.Clone(raw)[:0], &src, fast)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							out = c.Put(out[:0], &src, fast)
						}
						sinkBytes = out
						b.ReportMetric(float64(len(out)), "row-B")
					})
					b.Run("key-get", func(b *testing.B) {
						var keys [32]string
						for i := range keys {
							keys[i] = fmt.Sprintf("entry-%07d", i)
						}
						var sum uint64
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							child := currentTrusted(currentTrusted(d.rows[i&1023]).Get(1).Bytes())
							if keep {
								x, present := p.Lookup(child, 0, keys[i&31])
								sum += uint64(x.Int()) + uint64(btoi(present))
							} else {
								sum += uint64(p.Get(child, 0, keys[i&31]).Int())
							}
						}
						sinkU = sum
					})

					if keep && fast {
						b.Run("key-get-directory-control", func(b *testing.B) {
							var keys [32]string
							for i := range keys {
								keys[i] = fmt.Sprintf("entry-%07d", i)
							}
							var sum uint64
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								child := currentTrusted(currentTrusted(d.rows[i&1023]).Get(1).Bytes())
								x, present := currentDictionaryLookupDirectory(child, keys[i&31])
								sum += uint64(x.Int()) + uint64(btoi(present))
							}
							sinkU = sum
						})
					}
					b.Run("set-zero-delete-materialize", func(b *testing.B) {
						var w currentWriter
						var scratch [2]classMeta
						var arena [128]byte
						child := currentTrusted(currentTrusted(d.rows[0]).Get(1).Bytes())
						e := p.Edit(child, scratch[:0], arena[:0])
						p.Set(&e, 0, "entry-0000001", currentInt(0))
						p.Delete(&e, 0, "entry-0000002")
						out := p.Append(&e, &w, nil, fast)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							child := currentTrusted(currentTrusted(d.rows[i&1023]).Get(1).Bytes())
							e := p.Edit(child, scratch[:0], arena[:0])
							p.Set(&e, 0, "entry-0000001", currentInt(0))
							p.Delete(&e, 0, "entry-0000002")
							out = p.Append(&e, &w, out[:0], fast)
						}
						sinkBytes = out
					})
				})
			}
		}
	}
}

func TestDictionaryPolicyAllocations(t *testing.T) {
	for _, fast := range []bool{false, true} {
		for _, keep := range []bool{false, true} {
			typ := nativeDictionaryValueType[int64](kvo.TInt64, keep)
			c := bindNative[nativeDictionaryValues[int64]](typ)
			p := bindCurrentDictionary(typ.Model(), 1)
			src := dictionaryBenchSample(t, 4)
			raw := c.Put(nil, &src, fast)
			view := currentTrusted(raw)
			var row nativeDictionaryValues[int64]
			c.GetFast(view, &row)
			out := c.Put(nil, &src, fast)
			if n := testing.AllocsPerRun(100, func() {
				c.GetFast(view, &row)
				out = c.Put(out[:0], &src, fast)
			}); n != 0 {
				t.Fatalf("native fast=%v keep=%v: %v allocations", fast, keep, n)
			}
			child := currentTrusted(view.Get(1).Bytes())
			var w currentWriter
			var scratch [2]classMeta
			var arena [128]byte
			edit := func() {
				e := p.Edit(child, scratch[:0], arena[:0])
				p.Set(&e, 0, "entry-0000001", currentInt(0))
				p.Delete(&e, 0, "entry-0000002")
				if keep {
					_, _ = p.LookupEdit(&e, 0, "entry-0000001")
				}
				out = p.Append(&e, &w, out[:0], fast)
			}
			edit()
			if n := testing.AllocsPerRun(100, edit); n != 0 {
				t.Fatalf("key edits: %v allocations", n)
			}
		}
	}
}
