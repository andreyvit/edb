package kvolab

import (
	"fmt"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func BenchmarkTagEncode(b *testing.B) {
	all := fixtures()
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k"} {
		v := all[name]
		b.Run(name+"/draft", func(b *testing.B) {
			var e tagDraftEncoder
			dst := e.object(nil, v, 0)
			checkDraftTree(b, v, dst)
			b.ReportAllocs()
			b.ReportMetric(float64(len(dst)), "encoded-B")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = e.object(dst[:0], v, 0)
			}
			sinkBytes = dst
		})
		b.Run(name+"/msgpack-generated", func(b *testing.B) {
			dst := appendOwnMP(nil, v, nil, false)
			b.ReportAllocs()
			b.ReportMetric(float64(len(dst)), "encoded-B")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = appendOwnMP(dst[:0], v, nil, false)
			}
			sinkBytes = dst
		})
		for f := tagLength; f <= tagTuned; f++ {
			strategies := []tagStrategy{tagBuffered}
			if f == tagTuned {
				strategies = append(strategies, tagBackpatch, tagCatalog)
			}
			for _, strategy := range strategies {
				suffix := []string{"buffered", "backpatch", "catalog"}[strategy]
				b.Run(name+"/"+tagNames[f]+"-"+suffix, func(b *testing.B) {
					e := tagEncoder{format: f, strategy: strategy}
					dst := e.object(nil, v, 0)
					checkTagTree(b, v, dst, f, strategy == tagCatalog)
					b.ReportAllocs()
					b.ReportMetric(float64(len(dst)), "encoded-B")
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						dst = e.object(dst[:0], v, 0)
					}
					sinkBytes = dst
				})
			}
		}
	}
}
func BenchmarkTagCold(b *testing.B) {
	for _, name := range []string{"settings", "blob-32k"} {
		v := fixtures()[name]
		b.Run(name+"/draft", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var e tagDraftEncoder
				sinkBytes = e.object(nil, v, 0)
			}
		})
		for strategy := tagBuffered; strategy <= tagCatalog; strategy++ {
			b.Run(fmt.Sprintf("%s/%d", name, strategy), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					e := tagEncoder{format: tagTuned, strategy: strategy}
					sinkBytes = e.object(nil, v, 0)
				}
			})
		}
	}
}
func BenchmarkTagDeep(b *testing.B) {
	for _, depth := range []int{1, 4, 16} {
		v := tagDeep(depth, 32768)
		b.Run(fmt.Sprintf("depth%d/draft", depth), func(b *testing.B) {
			var e tagDraftEncoder
			dst := e.object(nil, v, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = e.object(dst[:0], v, 0)
			}
			sinkBytes = dst
		})
		for strategy := tagBuffered; strategy <= tagCatalog; strategy++ {
			b.Run(fmt.Sprintf("depth%d/%d", depth, strategy), func(b *testing.B) {
				e := tagEncoder{format: tagTuned, strategy: strategy}
				dst := e.object(nil, v, 0)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst = e.object(dst[:0], v, 0)
				}
				sinkBytes = dst
			})
		}
	}
}
func BenchmarkTagRead(b *testing.B) {
	for _, n := range []int{8, 128, 512} {
		v := numeric(n, 0)
		for _, layout := range []draft.Layout{draft.Compact, draft.Indexed} {
			view := draft.Trusted(draftObject(v, layout, layout))
			b.Run(fmt.Sprintf("n%d/draft-%d", n, layout), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					sum += view.Get(query(i, n)).Uint()
				}
				sinkU = sum
			})
		}
		for f := tagLength; f <= tagTuned; f++ {
			strategies := []tagStrategy{tagBuffered}
			if f == tagTuned {
				strategies = append(strategies, tagCatalog)
			}
			for _, strategy := range strategies {
				e := tagEncoder{format: f, strategy: strategy}
				raw := e.object(nil, v, 0)
				b.Run(fmt.Sprintf("n%d/%s-%d", n, tagNames[f], strategy), func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					for i := 0; i < b.N; i++ {
						sum += tagGet(raw, f, strategy == tagCatalog, query(i, n)).uint()
					}
					sinkU = sum
				})
			}
		}
	}
}
func BenchmarkTagScan(b *testing.B) {
	v := numeric(128, 0)
	old := draft.Trusted(draftObject(v, draft.Compact, draft.Compact))
	b.Run("draft", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			it := old.Iter()
			for it.Next() {
				sum += it.Entry().Value.Uint()
			}
		}
		sinkU = sum
	})
	for f := tagLength; f <= tagTuned; f++ {
		for _, strategy := range []tagStrategy{tagBuffered, tagCatalog} {
			e := tagEncoder{format: f, strategy: strategy}
			raw := e.object(nil, v, 0)
			b.Run(fmt.Sprintf("%s-%d", tagNames[f], strategy), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					it := tagIter(raw, f, strategy == tagCatalog)
					for {
						_, _, x, ok := it.next()
						if !ok {
							break
						}
						sum += x.uint()
					}
				}
				sinkU = sum
			})
		}
	}
}

func BenchmarkTagFused(b *testing.B) {
	for _, name := range []string{"profile", "state", "settings", "transaction"} {
		v := fixtures()[name]
		for _, strategy := range []tagStrategy{tagBuffered, tagCatalog} {
			b.Run(fmt.Sprintf("%s/%d/encode", name, strategy), func(b *testing.B) {
				e := tagEncoder{format: tagTuned, strategy: strategy, fuseKeys: true}
				dst := e.object(nil, v, 0)
				checkTagTree(b, v, dst, tagTuned, strategy == tagCatalog, true)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst = e.object(dst[:0], v, 0)
				}
				sinkBytes = dst
			})
		}
	}
	v := numeric(128, 0)
	for _, strategy := range []tagStrategy{tagBuffered, tagCatalog} {
		e := tagEncoder{format: tagTuned, strategy: strategy, fuseKeys: true}
		raw := e.object(nil, v, 0)
		b.Run(fmt.Sprintf("numeric128/%d/get", strategy), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				sum += tagGet(raw, tagTuned, strategy == tagCatalog, query(i, 128), true).uint()
			}
			sinkU = sum
		})
	}
}
