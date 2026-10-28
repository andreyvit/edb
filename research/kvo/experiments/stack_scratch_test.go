package kvolab

import "testing"

// A flat plan has a much simpler lifetime than the recursive editor. This
// control checks whether a truly small one-shot patch stays on the stack.
func patchApplyStack(dst, base []byte, edits []perfLeaf) ([]byte, bool) {
	var small [8]patchRef
	refs, ok := patchPlan(base, edits, small[:0])
	if !ok {
		return dst, false
	}
	dst = append(dst[:0], base...)
	patchApplyOwned(dst, refs)
	return dst, true
}
func BenchmarkPatchStack(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, pattern := range []string{"few-shallow", "few-deep"} {
			edits := rowEdits(r, pattern)
			if _, ok := patchPlan(r.raw, edits, nil); !ok {
				continue
			}
			b.Run(name+"/"+pattern, func(b *testing.B) {
				out := make([]byte, 0, len(r.raw))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var ok bool
					out, ok = patchApplyStack(out, r.raw, edits)
					if !ok {
						b.Fatal("patch")
					}
				}
				b.StopTimer()
				sinkBytes = out
			})
		}
	}
}

// A deliberately generous stack-scratch control. Measure actual escape behavior
// and clearing cost; declaring arrays locally does not prove zero allocations.
func treeApplyStack(dst []byte, base lengthView, edits []perfLeaf) []byte {
	var nodes [8]treeNode
	var changes [8][8]batchChange
	var arenas [8][64]byte
	var frames [8]lengthFrame
	var metadata [8][128]classMeta
	var keys [8][64]byte
	for i := range nodes {
		nodes[i].edit.changes = changes[i][:0]
		nodes[i].edit.storage.arena = arenas[i][:0]
		frames[i].entries = metadata[i][:0]
		frames[i].keys = keys[i][:0]
	}
	e := treeEditor{nodes: nodes[:]}
	w := lengthWriter{frames: frames[:]}
	e.Reset(base, "append", true)
	for _, m := range edits {
		e.Set(m.path, m.value)
	}
	return e.Append(&w, dst)
}
func TestStackScratch(t *testing.T) {
	for _, r := range rowFixtures(t) {
		edits := rowEdits(r, "few-deep")
		out := treeApplyStack(nil, lengthTrusted(r.raw), edits)
		for _, e := range edits {
			if !equalLengthValue(perfPath(lengthTrusted(out), e.path, "common"), e.value) {
				t.Fatal("stack scratch")
			}
		}
	}
}
func BenchmarkStackScratch(b *testing.B) {
	for _, name := range []string{"local128", "nested", "row-small", "row-deep"} {
		r, ok := rowFixtures(b)[name]
		if !ok {
			source := mmapFixture(name)
			raw := mmapEncode(source, "indexed-all")
			r = perfRow{source: source, raw: raw, leaves: perfCollect(source, raw)}
		}
		edits := rowEdits(r, "few-deep")
		for _, mode := range []string{"cold", "stack", "reuse"} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				out := make([]byte, 0, len(r.raw)+1024)
				var e treeEditor
				var w lengthWriter
				run := func() {
					if mode == "stack" {
						out = treeApplyStack(out[:0], lengthTrusted(r.raw), edits)
						return
					}
					if mode == "cold" {
						e = treeEditor{}
						w = lengthWriter{}
					}
					e.Reset(lengthTrusted(r.raw), "append", true)
					for _, m := range edits {
						e.Set(m.path, m.value)
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
