package kvolab

import (
	"bytes"
	"fmt"
	"testing"
)

// This placement control retains the previous patch planner; PatchLookup
// measures the later fused planner independently.
// Complete writes borrow the base from real Bolt mappings and change rows
// between operations. Earlier overlay controls isolate ordering on hot heap rows.
func BenchmarkKeysLastMmap(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, codec := range []string{"back", "keys-last"} {
			raw := r.raw
			if codec == "keys-last" {
				var uw keysLastWriter
				raw = keysLastEncode(&uw, nil, r.source, func(int, Value) bool { return true })
			}
			for _, rows := range []int{1, 1024} {
				b.Run(fmt.Sprintf("%s/%s/rows%d", name, codec, rows), func(b *testing.B) {
					d := newMmapDataset(b, raw, rows)
					warmMmapPayloads(d)
					for _, pattern := range []string{"few-shallow", "few-deep", "wide70"} {
						edits := rowEdits(r, pattern)
						for _, mode := range []string{"tree", "hybrid"} {
							b.Run(pattern+"/"+mode, func(b *testing.B) {
								var e treeEditor
								var w lengthWriter
								var ue keysLastTreeEditor
								var uw keysLastWriter
								var out []byte
								var refs []patchRef
								materialize := func(base []byte) {
									if mode == "hybrid" {
										var ok bool
										if codec == "keys-last" {
											refs, ok = keysLastPatchPlanPrevious(base, edits, refs)
										} else {
											refs, ok = patchPlan(base, edits, refs)
										}
										if ok {
											out = append(out[:0], base...)
											patchApplyOwned(out, refs)
											return
										}
									}
									if codec == "keys-last" {
										ue.Reset(keysLastTrusted(base), "append", true)
										for _, m := range edits {
											ue.Set(m.path, m.value)
										}
										out = ue.Append(&uw, out[:0])
									} else {
										e.Reset(lengthTrusted(base), "append", true)
										for _, m := range edits {
											e.Set(m.path, m.value)
										}
										out = e.Append(&w, out[:0])
									}
								}
								materialize(d.rows[0])
								if !bytes.Equal(d.rows[0], raw) {
									b.Fatal("borrowed row changed")
								}
								b.ReportAllocs()
								b.ResetTimer()
								rng := uint64(0x12345678fedabcde)
								for i := 0; i < b.N; i++ {
									rng ^= rng << 13
									rng ^= rng >> 7
									rng ^= rng << 17
									materialize(d.rows[int(rng>>16)&(rows-1)])
								}
								b.StopTimer()
								sinkBytes = out
								b.ReportMetric(float64(len(raw)), "row-B")
								b.ReportMetric(float64(len(edits)), "edits/op")
							})
						}
					}
				})
			}
		}
	}
}
