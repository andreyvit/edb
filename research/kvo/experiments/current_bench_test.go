package kvolab

import (
	"bytes"
	"fmt"
	"testing"
)

// These scalar controls isolate signed mapping within the same counted tag
// grammar. Replay shapes preserve word widths, but intentionally not signedness.
func currentScalarSources() map[string]Value {
	positive, negative, boundary := Array(), Array(), Array()
	for i := int64(0); i < 128; i++ {
		positive.Items = append(positive.Items, I(i))
		negative.Items = append(negative.Items, I(-i-1))
	}
	for _, n := range currentSignedBoundaries() {
		boundary.Items = append(boundary.Items, I(n))
	}
	return map[string]Value{"positive": positive, "negative": negative, "boundaries": boundary}
}

func BenchmarkCurrentScalars(b *testing.B) {
	for name, source := range currentScalarSources() {
		for _, codec := range []string{"zigzag-tags", "current-tags"} {
			b.Run(name+"/"+codec, func(b *testing.B) {
				var old oneBitWriter
				var w currentWriter
				var out []byte
				write := func() {
					if codec == "zigzag-tags" {
						out = oneBitEncode(&old, out[:0], source)
					} else {
						out = currentEncode(&w, out[:0], source, nil)
					}
				}
				write()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					write()
				}
				b.ReportMetric(float64(len(out)), "row-B")
				sinkBytes = out
			})
		}
	}
}

func TestCurrentReplaySizes(t *testing.T) {
	var current, literal, zigzagTags int
	for _, source := range workloadFixtures(t) {
		var a currentWriter
		var b lengthWriter
		var c oneBitWriter
		current += len(currentEncode(&a, nil, source, nil))
		literal += len(lengthEncode(&b, nil, source, nil))
		zigzagTags += len(oneBitEncode(&c, nil, source))
	}
	t.Logf("replay roots=%d: current=%d, literal=%d, previous-counted-tags=%d bytes; replay words are unsigned", len(workloadFixtures(t)), current, literal, zigzagTags)
	for name, source := range currentScalarSources() {
		var a currentWriter
		var b oneBitWriter
		t.Logf("signed-%s: current=%d, zigzag-tags=%d bytes", name, len(currentEncode(&a, nil, source, nil)), len(oneBitEncode(&b, nil, source)))
	}
}

func BenchmarkCurrentMmap(b *testing.B) {
	for _, name := range []string{"local128", "global128", "nested"} {
		source := mmapFixture(name)
		for _, codec := range []string{"previous", "current", "value-call", "wrapped", "immutable"} {
			b.Run(name+"/"+codec, func(b *testing.B) {
				var raw []byte
				switch codec {
				case "previous":
					var w keysLastWriter
					raw = keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
				case "current", "value-call", "wrapped":
					var w currentWriter
					raw = currentEncode(&w, nil, source, func(int, Value) bool { return true })
				case "immutable":
					raw = mmapEncode(source, "immutable")
				}
				d := newMmapDataset(b, raw, 32768)
				warmMmapPayloads(d)
				b.ReportAllocs()
				b.ResetTimer()
				rng, sum := uint64(0xdedabcde12345678), uint64(0)
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					data := d.rows[int(rng>>16)&32767]
					if codec == "immutable" {
						sum += mmapRead(data, codec, name, rng)
						continue
					}
					key := rng&127 + 1
					if name == "global128" {
						key += 1 << 50
					}
					if name == "nested" {
						key = rng&7 + 1
					}
					if codec == "wrapped" {
						v := currentTrusted(data).GetWrapped(key)
						if name == "nested" {
							v = currentTrusted(v.Bytes()).GetWrapped(rng>>3&15 + 1)
						}
						sum += v.Uint()
					} else if codec == "current" || codec == "value-call" {
						v := currentTrusted(data).Get(key)
						if name == "nested" {
							v = currentTrusted(v.Bytes()).Get(rng>>3&15 + 1)
						}
						if codec == "value-call" {
							sum += v.UintBeforeInlining()
						} else {
							sum += v.Uint()
						}
					} else {
						v := keysLastTrusted(data).Get(key)
						if name == "nested" {
							v = keysLastTrusted(v.Bytes()).Get(rng>>3&15 + 1)
						}
						sum += v.Uint()
					}
				}
				b.ReportMetric(float64(len(raw)), "row-B")
				sinkU = sum
			})
		}
	}
}

// Complete writes include materializing output from an actual warm Bolt mmap.
// Writer, edit/plan scratch and output are reused; this is not a durable commit.
func BenchmarkCurrentRows(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, fast := range []bool{false, true} {
			policy := func(int, Value) bool { return fast }
			for _, codec := range []string{"previous", "current"} {
				b.Run(fmt.Sprintf("%s/fast%v/%s", name, fast, codec), func(b *testing.B) {
					var w currentWriter
					var old keysLastWriter
					var raw []byte
					if codec == "current" {
						raw = currentEncode(&w, nil, r.source, policy)
					} else {
						raw = keysLastEncode(&old, nil, r.source, policy)
					}
					d := newMmapDataset(b, raw, 1024)
					warmMmapPayloads(d)
					b.Run("encode", func(b *testing.B) {
						out := bytes.Clone(raw)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if codec == "current" {
								out = currentEncode(&w, out[:0], r.source, policy)
							} else {
								out = keysLastEncode(&old, out[:0], r.source, policy)
							}
						}
						b.ReportMetric(float64(len(out)), "row-B")
						sinkBytes = out
					})
					for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled"} {
						oldEdits := rowEdits(r, pattern)
						// Replay fixtures synthesize unsigned words; conversion here
						// preserves their existing logical values, not ZigZag bits.
						edits := make([]currentLeaf, len(oldEdits))
						for i, edit := range oldEdits {
							var value currentValue
							if edit.value.ptr == nil {
								value = currentWord(edit.value.num)
							} else {
								value = currentBytes(edit.value.Bytes())
							}
							edits[i] = currentLeaf{edit.path, value}
						}
						b.Run(pattern, func(b *testing.B) {
							var merge currentLeafMerger
							var oldMerge leafMerger
							var out []byte
							var offsets []patchOffset
							write := func(base []byte) {
								if pattern == "empty-shuffled" {
									base = nil
								}
								var ok bool
								if codec == "current" {
									offsets, ok = currentPatchPlan(base, edits, offsets)
								} else {
									offsets, ok = patchPlanOffsets(base, oldEdits, offsets)
								}
								if ok {
									out = append(out[:0], base...)
									if codec == "current" {
										currentPatchApplyOwned(out, offsets, edits)
									} else {
										patchOffsetsApplyOwned(out, offsets, oldEdits)
									}
								} else if codec == "current" {
									out = merge.Append(&w, out[:0], currentTrusted(base), edits, nil, fast)
								} else {
									out = oldMerge.Append(&old, out[:0], keysLastTrusted(base), oldEdits, nil, fast)
								}
							}
							write(d.rows[0])
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								write(d.rows[i&1023])
							}
							b.ReportMetric(float64(len(out)), "row-B")
							sinkBytes = out
						})
					}
				})
			}
		}
	}
}

func BenchmarkCurrentChangeScratch(b *testing.B) {
	c := currentChanges{Base: 1, Next: 2, Items: []currentChange{{Value: currentInt(-129)}}}
	// Include a nonempty path so the retained scratch boundary is measured.
	old, _ := lengthChangeFixture(1)
	c.Items[0].Path = old.Items[0].Path
	var w currentChangeWriter
	raw := w.Append(nil, c)
	for _, fresh := range []bool{false, true} {
		b.Run(fmt.Sprintf("fresh%v", fresh), func(b *testing.B) {
			var reused [8]lengthPathPart
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				scratch := reused[:0]
				if fresh {
					scratch = make([]lengthPathPart, 0, 8)
				}
				r, err := openCurrentChanges(raw, scratch)
				if err != nil {
					b.Fatal(err)
				}
				item, _ := r.NextChange()
				sinkU = uint64(item.Value.Int())
			}
		})
	}
}

func BenchmarkCurrentIteration(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		for _, fast := range []bool{false, true} {
			var w currentWriter
			raw := currentEncode(&w, nil, rowFixtures(b)[name].source, func(int, Value) bool { return fast })
			view := currentTrusted(raw)
			for _, mode := range []string{"wrapped", "fused-large", "current"} {
				b.Run(fmt.Sprintf("%s/fast%v/%s", name, fast, mode), func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					for i := 0; i < b.N; i++ {
						if mode == "current" {
							it := view.Iter()
							for {
								key, text, value, ok := it.Next()
								if !ok {
									break
								}
								sum += key + uint64(len(text)) + value.num
							}
						} else if mode == "fused-large" {
							it := view.IterWrapped()
							for {
								key, text, value, ok := it.NextFused()
								if !ok {
									break
								}
								sum += key + uint64(len(text)) + value.num
							}
						} else {
							it := view.IterWrapped()
							for {
								key, text, value, ok := it.Next()
								if !ok {
									break
								}
								sum += key + uint64(len(text)) + value.num
							}
						}
					}
					sinkU = sum
				})
			}
		}
	}
}
