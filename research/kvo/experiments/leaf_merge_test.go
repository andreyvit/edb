package kvolab

// A one-shot merge for resolved leaf changes. It borrows inputs for this call,
// sorts only their indexes, and writes directly into one destination. Duplicate
// leaf paths use the last input. Ancestor/descendant replacements require the
// ordered tree editor instead; this API rejects them before writing output.
// Destination storage must not alias base bytes or borrowed edit spans.
import (
	"bytes"
	"slices"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type leafMerger struct {
	order []int
	edits []perfLeaf
	zeros []bool
	fast  bool
}

func (e *leafMerger) Append(w *keysLastWriter, dst []byte, base keysLastView, edits []perfLeaf, keepZero []bool, fast bool) []byte {
	if len(edits) > tagMaxEntries {
		panic(draft.ErrLimit)
	}
	if len(keepZero) != 0 && len(keepZero) != len(edits) {
		panic(draft.ErrValue)
	}
	e.edits, e.zeros, e.fast = edits, keepZero, fast
	e.order = slices.Grow(e.order[:0], len(edits))[:len(edits)]
	for i, edit := range edits {
		if len(edit.path) == 0 {
			panic(draft.ErrKind)
		}
		e.order[i] = i
	}
	slices.SortFunc(e.order, func(a, b int) int {
		if c := comparePerfPath(edits[a].path, edits[b].path); c != 0 {
			return c
		}
		return a - b
	})
	n := 0
	for _, i := range e.order {
		if n > 0 {
			previous, current := edits[e.order[n-1]].path, edits[i].path
			if comparePerfPath(previous, current) == 0 {
				e.order[n-1] = i
				continue
			}
			if len(previous) < len(current) && comparePerfPath(previous, current[:len(previous)]) == 0 {
				panic(draft.ErrKind)
			}
		}
		e.order[n] = i
		n++
	}
	e.order = e.order[:n]
	w.Reset(dst)
	e.emit(w, base, e.order, 0, 0, 0, "")
	out := w.BytesWritten()
	e.edits, e.zeros = nil, nil
	return out
}

func leafStepCompare(kind int, step perfStep, key uint64, text string) int {
	if step.kind != kind {
		panic(draft.ErrKind)
	}
	if kind == 1 {
		if step.text < text {
			return -1
		}
		if step.text > text {
			return 1
		}
	} else {
		if step.key < key {
			return -1
		}
		if step.key > key {
			return 1
		}
	}
	return 0
}

func (e *leafMerger) emit(w *keysLastWriter, base keysLastView, order []int, depth, kind int, key uint64, text string) {
	if base.n != 0 && base.Kind() != kind {
		panic(draft.ErrKind)
	}
	// Existing containers keep their layout. Changing the layout of an opaque
	// subtree requires its model; the fast flag is a hint for newly built rows.
	indexed := e.fast
	if base.n != 0 {
		indexed = base.Indexed()
	}
	w.Begin(key, text, kind, indexed)
	it := base.Iter()
	k, s, v, ok := it.Next()
	listNext := uint64(0)
	for ok || len(order) > 0 {
		cmp := 1
		var step perfStep
		if len(order) > 0 {
			step = e.edits[order[0]].path[depth]
			if step.kind != kind {
				panic(draft.ErrKind)
			}
			if !ok {
				cmp = -1
			} else {
				cmp = leafStepCompare(kind, step, k, unsafe.String(unsafe.SliceData(s), len(s)))
			}
		}
		if cmp > 0 {
			keysLastPut(w, k, unsafe.String(unsafe.SliceData(s), len(s)), v)
			if kind == 2 {
				listNext = k + 1
			}
			k, s, v, ok = it.Next()
			continue
		}
		end := 1
		for end < len(order) && leafStepCompare(kind, e.edits[order[end]].path[depth], step.key, step.text) == 0 {
			end++
		}
		if kind == 2 {
			if base.n != 0 && step.key >= uint64(base.Len()) {
				panic(draft.ErrIndex)
			}
			for listNext < step.key {
				keysLastPut(w, listNext, "", lengthValue{})
				listNext++
			}
			listNext++
		}
		edit := e.edits[order[0]]
		if len(edit.path) == depth+1 {
			keep := kind == 2 || len(e.zeros) != 0 && e.zeros[order[0]]
			if keep || !edit.value.IsZero() {
				keysLastPut(w, step.key, step.text, edit.value)
			}
		} else {
			var child keysLastView
			if cmp == 0 && !v.IsZero() {
				child = keysLastTrusted(v.Bytes())
			}
			e.emit(w, child, order[:end], depth+1, edit.path[depth+1].kind, step.key, step.text)
		}
		if cmp == 0 {
			k, s, v, ok = it.Next()
		}
		order = order[end:]
	}
	w.End()
}

func TestLeafMerge(t *testing.T) {
	for name, r := range rowFixtures(t) {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var w keysLastWriter
			raw := keysLastEncode(&w, nil, r.source, policy)
			original := bytes.Clone(raw)
			for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled"} {
				base := keysLastTrusted(raw)
				if pattern == "empty-shuffled" {
					base = keysLastView{}
				}
				edits := rowEdits(r, pattern)
				var tree keysLastTreeEditor
				tree.Reset(base, "append", true)
				for _, edit := range edits {
					tree.Set(edit.path, edit.value)
				}
				want := tree.Append(&w, nil)
				var e leafMerger
				for repeat := 0; repeat < 2; repeat++ {
					got := e.Append(&w, nil, base, edits, nil, true)
					if !bytes.Equal(got, want) {
						t.Fatal(name, pattern, "tree comparison")
					}
				}
			}
			if !bytes.Equal(raw, original) {
				t.Fatal("borrowed row changed")
			}
		}
	}
	var w keysLastWriter
	w.Begin(0, "", 0, true)
	keysLastPut(&w, 1, "", lengthWord(0)) // Opaque present zero.
	w.Put(2, "", lengthWord(3))
	w.End()
	base := keysLastTrusted(bytes.Clone(w.BytesWritten()))
	edits := []perfLeaf{
		{path: []perfStep{{key: 3}}, value: lengthWord(4)},
		{path: []perfStep{{key: 2}}, value: lengthWord(0)},
		{path: []perfStep{{key: 3}}, value: lengthWord(0)},
	}
	var e leafMerger
	out := e.Append(&w, nil, base, edits, []bool{false, false, true}, true)
	v := keysLastTrusted(out)
	if v.Len() != 2 {
		t.Fatal("zero preservation and duplicate last write")
	}
	it := v.Iter()
	k, _, _, ok := it.Next()
	if !ok || k != 1 {
		t.Fatal("unknown zero lost")
	}
	k, _, value, ok := it.Next()
	if !ok || k != 3 || !value.IsZero() {
		t.Fatal("explicit zero lost")
	}
	t.Run("ancestor-overlap", func(t *testing.T) {
		defer func() {
			if recover() != draft.ErrKind {
				t.Fatal("overlapping paths must be rejected")
			}
		}()
		e.Append(&w, nil, base, []perfLeaf{{path: []perfStep{{key: 2}}, value: lengthWord(7)}, {path: []perfStep{{key: 2}, {key: 1}}, value: lengthWord(8)}}, nil, true)
	})
}

func TestLeafMergeReplay(t *testing.T) {
	for name, source := range workloadFixtures(t) {
		var old lengthWriter
		back := lengthEncode(&old, nil, source, func(int, Value) bool { return true })
		r := perfRow{source: source, raw: back, leaves: perfCollect(source, back)}
		if len(r.leaves) == 0 {
			continue
		}
		edits := rowEdits(r, "wide70")
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		var tree keysLastTreeEditor
		tree.Reset(keysLastTrusted(raw), "append", true)
		for _, edit := range edits {
			tree.Set(edit.path, edit.value)
		}
		want := tree.Append(&w, nil)
		var merger leafMerger
		got := merger.Append(&w, nil, keysLastTrusted(raw), edits, nil, true)
		if !bytes.Equal(got, want) {
			t.Log(name, "sizes", len(got), len(want), "leaves", len(r.leaves))
			for _, leaf := range r.leaves {
				a, b := keysLastPath(got, leaf.path), keysLastPath(want, leaf.path)
				if !equalLengthValue(a, b) {
					t.Log("differing path", leaf.path, "words/lengths", a.num, b.num, "spans", a.ptr != nil, b.ptr != nil)
					break
				}
			}
			t.Fatal(name, "replay mismatch")
		}
		values := make([]lengthValue, len(r.leaves))
		for i, leaf := range r.leaves {
			values[i] = leaf.value
		}
		for i, slot := range bindShapeBinary(nil, r.leaves, edits) {
			values[slot] = edits[i].value
		}
		for i, leaf := range r.leaves {
			if !equalLengthValue(keysLastPath(got, leaf.path), values[i]) {
				t.Fatal(name, "replay semantics", leaf.path)
			}
		}
	}
}

func BenchmarkLeafMerge(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		var writer keysLastWriter
		raw := keysLastEncode(&writer, nil, r.source, func(int, Value) bool { return true })
		d := newMmapDataset(b, raw, 1024)
		warmMmapPayloads(d)
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled"} {
			edits := rowEdits(r, pattern)
			for _, mode := range []string{"tree", "merge", "patch-merge"} {
				for _, cold := range []bool{false, true} {
					label := "reuse"
					if cold {
						label = "cold"
					}
					b.Run(name+"/"+pattern+"/"+mode+"/"+label, func(b *testing.B) {
						var tree keysLastTreeEditor
						var merger leafMerger
						var w keysLastWriter
						var refs []patchRef
						var out []byte
						rng := uint64(0xabc123def987654)
						run := func() {
							rng ^= rng << 13
							rng ^= rng >> 7
							rng ^= rng << 17
							base := keysLastTrusted(d.rows[int(rng>>16)&1023])
							if pattern == "empty-shuffled" {
								base = keysLastView{}
							}
							if cold {
								tree = keysLastTreeEditor{}
								merger = leafMerger{}
								refs = nil
							}
							if mode == "tree" {
								tree.Reset(base, "append", true)
								for _, edit := range edits {
									tree.Set(edit.path, edit.value)
								}
								out = tree.Append(&w, out[:0])
								return
							}
							if mode == "patch-merge" && base.n != 0 {
								var ok bool
								refs, ok = patchPlanFused(base.Raw(), edits, refs)
								if ok {
									out = append(out[:0], base.Raw()...)
									patchApplyOwned(out, refs)
									return
								}
							}
							out = merger.Append(&w, out[:0], base, edits, nil, true)
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
}
