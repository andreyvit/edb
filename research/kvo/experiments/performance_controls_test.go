package kvolab

import (
	"testing"

	"github.com/andreyvit/edb/kvo"
)

// Both Compact getters below assume a validated Object. The old common View
// control is retained separately; it dispatches across layouts and kinds.
func BenchmarkCompactMatched(b *testing.B) {
	all := fixtures()
	for name, r := range rowFixtures(b) {
		all[name] = r.source
	}
	all["tiny2"] = numeric(2, 0)
	all["tiny8"] = numeric(8, 0)
	for _, name := range []string{"state", "settings", "tiny2", "tiny8", "row-small", "row-wide", "row-deep", "row-span-heavy"} {
		source := all[name]
		for _, mode := range []string{"literal-common", "literal-specialized", "onebit", "onebit-no-count"} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				var w lengthWriter
				var ow oneBitWriter
				ow.omitCount = mode == "onebit-no-count"
				var raw []byte
				if mode == "onebit" || ow.omitCount {
					raw = oneBitEncode(&ow, nil, source)
				} else {
					raw = lengthEncode(&w, nil, source, nil)
				}
				b.ReportAllocs()
				b.ResetTimer()
				var sum uint64
				for i := 0; i < b.N; i++ {
					f := source.Fields[i%len(source.Fields)]
					var v lengthValue
					switch mode {
					case "literal-common":
						v = lengthTrusted(raw).Get(f.Key)
					case "literal-specialized":
						v, _ = selectorLiteralGetInline(raw, f.Key)
					default:
						v = oneBitTrustedGet(raw, ow.omitCount, f.Key, "")
					}
					if f.Value.Kind == Text || f.Value.Kind == Blob || f.Value.Kind == Record || f.Value.Kind == Dict || f.Value.Kind == List {
						sum += v.num
					} else if mode == "onebit" || ow.omitCount {
						sum += oneBitWord(v)
					} else {
						sum += v.Uint()
					}
				}
				sinkU = sum
			})
		}
	}
}
func BenchmarkImmutableMutation(b *testing.B) {
	for _, name := range []string{"local128", "nested"} {
		source := mmapFixture(name)
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		old := oldKVO(source).Record(nil)
		r := perfRow{source: source, raw: raw, leaves: perfCollect(source, raw)}
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled"} {
			edits := rowEdits(r, pattern)
			for _, mode := range []string{"immutable", "tree-cold", "tree-reuse", "owned-copy"} {
				b.Run(name+"/"+pattern+"/"+mode, func(b *testing.B) {
					if pattern == "empty-shuffled" && mode == "owned-copy" {
						b.Skip("construction has no base to patch")
					}
					var e treeEditor
					var w lengthWriter
					var out []byte
					var refs []patchRef
					run := func() {
						if mode == "immutable" {
							m := kvo.UpdateRecord(old)
							if pattern == "empty-shuffled" {
								m = kvo.NewRecord(nil)
							}
							for _, edit := range edits {
								child := m
								for _, s := range edit.path[:len(edit.path)-1] {
									child = child.UpdateMap(s.key)
								}
								child.Set(edit.path[len(edit.path)-1].key, edit.value.Uint())
							}
							out = m.Record().Pack().Bytes()
							return
						}
						if mode == "owned-copy" {
							var ok bool
							refs, ok = patchPlan(raw, edits, refs)
							if !ok {
								panic("numeric patch")
							}
							out = append(out[:0], raw...)
							patchApplyOwned(out, refs)
							return
						}
						if mode == "tree-cold" {
							e = treeEditor{}
							w = lengthWriter{}
							out = nil
						}
						base := lengthTrusted(raw)
						if pattern == "empty-shuffled" {
							base = lengthView{}
						}
						e.Reset(base, "upsert", true)
						for _, edit := range edits {
							e.Set(edit.path, edit.value)
						}
						out = e.Append(&w, out[:0])
					}
					run()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run()
					}
					b.StopTimer()
					sinkBytes = out
				})
			}
		}
	}
}
