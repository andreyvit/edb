package kvolab

// A sparse container tree, used only for containers on edited paths. Bytes are
// emitted into one destination: edited children never use temporary payloads.
// Node handles survive slice growth; plain Go pointers to nodes would not.
import (
	"bytes"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type treeNode struct {
	edit                batchEditor
	parent, first, next int
	step                perfStep
	active, indexed     bool
}
type treeEditor struct {
	nodes   []treeNode
	used    int
	mode    string
	indexed bool
}

func (e *treeEditor) Reset(base lengthView, mode string, indexed bool) {
	e.used = 0
	e.mode = mode
	e.indexed = indexed
	e.newNode(-1, perfStep{}, base, 0)
}
func (e *treeEditor) newNode(parent int, step perfStep, base lengthView, kind int) int {
	i := e.used
	e.used++
	if i == len(e.nodes) {
		e.nodes = append(e.nodes, treeNode{})
	}
	n := &e.nodes[i]
	n.parent = parent
	n.step = step
	n.active = true
	n.first = -1
	n.next = -1
	n.indexed = e.indexed && (base.n == 0 || base.Indexed())
	if parent >= 0 {
		n.indexed = n.indexed && e.nodes[parent].indexed
	}
	n.edit.Reset(base, kind, e.mode)
	if parent >= 0 {
		n.next = e.nodes[parent].first
		e.nodes[parent].first = i
	}
	return i
}
func (e *treeEditor) child(parent int, s perfStep) int {
	for i := e.nodes[parent].first; i >= 0; i = e.nodes[i].next {
		n := &e.nodes[i]
		if n.active && n.step.key == s.key && n.step.text == s.text {
			return i
		}
	}
	return -1
}
func (e *treeEditor) Set(path []perfStep, v lengthValue) {
	e.setPath(path, v, false)
}

// A schema binding with PresenceIsSignificant calls SetPresent for its leaf.
func (e *treeEditor) SetPresent(path []perfStep, v lengthValue) {
	e.setPath(path, v, true)
}
func (e *treeEditor) setPath(path []perfStep, v lengthValue, keepZero bool) {
	if len(path) == 0 {
		panic(draft.ErrKind)
	}
	parent := 0
	for i, s := range path {
		if i == len(path)-1 {
			if child := e.child(parent, s); child >= 0 {
				e.nodes[child].active = false
			}
			e.set(parent, s, v, keepZero)
			return
		}
		child := e.child(parent, s)
		if child < 0 {
			base := e.lookup(parent, s)
			child = e.newNode(parent, s, base.Object(), path[i+1].kind)
			// The placeholder keeps a newly inserted child in the parent merge.
			// Its actual value is emitted recursively and never stored on disk.
			e.set(parent, s, lengthWord(1), false)
		}
		parent = child
	}
}
func (e *treeEditor) set(node int, s perfStep, v lengthValue, keepZero bool) {
	n := &e.nodes[node]
	if n.edit.storage.kind == 2 && n.edit.storage.base.n == 0 {
		// Construction can add List slots. A temporary kind change bypasses the
		// replacement-only baseline guard; the writer fills skipped positions.
		n.edit.storage.kind = 0
		n.edit.Set(s.key, s.text, v, true)
		n.edit.storage.kind = 2
	} else {
		n.edit.Set(s.key, s.text, v, keepZero || s.kind == 2)
	}
}
func (e *treeEditor) lookup(node int, s perfStep) lengthValue {
	n := &e.nodes[node]
	if n.edit.storage.kind == 2 && n.edit.storage.base.n == 0 {
		n.edit.storage.kind = 0
		v := n.edit.Get(s.key, s.text)
		n.edit.storage.kind = 2
		return v
	}
	return n.edit.Get(s.key, s.text)
}

// GetLeaf reads a leaf through the overlay. An edited container needs a mutable
// container handle, outside this path-only research skeleton; never expose its
// internal parent placeholder as a scalar value.
func (e *treeEditor) GetLeaf(path []perfStep) lengthValue {
	if len(path) == 0 {
		panic(draft.ErrKind)
	}
	parent := 0
	for i, s := range path {
		if child := e.child(parent, s); child >= 0 {
			if i+1 == len(path) {
				panic(draft.ErrKind)
			}
			parent = child
			continue
		}
		v := e.lookup(parent, s)
		if i+1 < len(path) {
			return perfPath(v.Object(), path[i+1:], "equal")
		}
		return v
	}
	return lengthValue{}
}
func (e *treeEditor) Append(w *lengthWriter, dst []byte) []byte {
	w.Reset(dst)
	e.emit(w, 0, 0, "")
	return w.BytesWritten()
}
func (e *treeEditor) emit(w *lengthWriter, node int, key uint64, text string) {
	n := &e.nodes[node]
	w.Begin(key, text, n.edit.storage.kind, n.indexed)
	listNext := uint64(0)
	n.edit.visit(func(k uint64, s string, v lengthValue) {
		if n.edit.storage.kind == 2 {
			for listNext < k {
				batchPut(w, listNext, "", lengthValue{})
				listNext++
			}
			listNext = k + 1
		}
		if child := e.child(node, perfStep{key: k, text: s}); child >= 0 {
			e.emit(w, child, k, s)
		} else {
			batchPut(w, k, s, v)
		}
	})
	w.End()
}
func (e *treeEditor) ScratchBytes() int {
	n := cap(e.nodes) * int(unsafe.Sizeof(treeNode{}))
	for i := range e.nodes {
		n += e.nodes[i].edit.ScratchBytes()
	}
	return n
}
func TestTreeEditor(t *testing.T) {
	for name, r := range rowFixtures(t) {
		original := bytes.Clone(r.raw)
		for _, indexed := range []bool{false, true} {
			for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled", "empty-sorted"} {
				edits := rowEdits(r, pattern)
				base := lengthTrusted(r.raw)
				if pattern == "empty-shuffled" || pattern == "empty-sorted" {
					base = lengthView{}
				}
				for _, mode := range []string{"append", "upsert", "hash"} {
					var e treeEditor
					var w lengthWriter
					e.Reset(base, mode, indexed)
					for _, m := range edits {
						e.Set(m.path, m.value)
						got := e.GetLeaf(m.path)
						if !equalLengthValue(got, m.value) {
							t.Fatal(name, pattern, mode, "Get")
						}
					}
					out := e.Append(&w, nil)
					for _, m := range edits {
						if !equalLengthValue(perfPath(lengthTrusted(out), m.path, "common"), m.value) {
							t.Fatal(name, pattern, mode, "output")
						}
					}
					if base.n > 0 {
						for _, l := range r.leaves {
							want := l.value
							for _, m := range edits {
								if samePerfPath(m.path, l.path) {
									want = m.value
									break
								}
							}
							if !equalLengthValue(perfPath(lengthTrusted(out), l.path, "common"), want) {
								t.Fatal(name, pattern, "untouched")
							}
						}
					}
				}
			}
		}
		if !bytes.Equal(original, r.raw) {
			t.Fatal("borrowed source modified")
		}
	}
}

func TestTreePreservation(t *testing.T) {
	var w lengthWriter
	w.Begin(0, "", 0, true)
	batchPut(&w, 1, "", lengthWord(0)) // An unknown, explicitly present zero.
	w.Begin(2, "", 0, false)
	w.Put(3, "", lengthWord(4))
	w.End()
	w.End()
	base := append([]byte(nil), w.BytesWritten()...)
	var e treeEditor
	e.Reset(lengthTrusted(base), "append", true)
	e.Set([]perfStep{{kind: 0, key: 2}, {kind: 0, key: 5}, {kind: 0, key: 6}}, lengthWord(9))
	e.SetPresent([]perfStep{{kind: 0, key: 9}}, lengthWord(0))
	func() {
		defer func() {
			if recover() != draft.ErrKind {
				t.Fatal("edited container exposed as a leaf")
			}
		}()
		e.GetLeaf([]perfStep{{kind: 0, key: 2}})
	}()
	out := e.Append(&w, nil)
	view := lengthTrusted(out)
	it := view.Iter()
	k, _, v, ok := it.Next()
	if !ok || k != 1 || !v.IsZero() {
		t.Fatal("lost unknown zero")
	}
	it.Next()
	k, _, v, ok = it.Next()
	if !ok || k != 9 || !v.IsZero() {
		t.Fatal("lost known present zero")
	}
	child := view.Get(2).Object()
	if child.Indexed() || child.Get(5).Object().Indexed() {
		t.Fatal("Compact child promoted")
	}
	// Replacing an ancestor invalidates its old child edit handle.
	var replacement lengthWriter
	blob := lengthEncode(&replacement, nil, Obj(Field{7, U(11)}), nil)
	e.Set([]perfStep{{kind: 0, key: 2}}, lengthBytes(blob))
	e.Set([]perfStep{{kind: 0, key: 2}, {kind: 0, key: 8}}, lengthWord(12))
	out = e.Append(&w, nil)
	child = lengthTrusted(out).Get(2).Object()
	if child.Get(7).Uint() != 11 || child.Get(8).Uint() != 12 || !child.Get(5).IsZero() {
		t.Fatal("ancestor replacement")
	}
}
func equalLengthValue(a, b lengthValue) bool {
	if a.ptr == nil || b.ptr == nil {
		return a == b
	}
	return bytes.Equal(a.Bytes(), b.Bytes())
}
func samePerfPath(a, b []perfStep) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func BenchmarkTreeMutation(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled", "empty-sorted"} {
			edits := rowEdits(r, pattern)
			for _, mode := range []string{"append", "upsert", "hash"} {
				for _, op := range []string{"stage", "finish"} {
					b.Run(name+"/"+pattern+"/"+mode+"/"+op, func(b *testing.B) {
						base := lengthTrusted(r.raw)
						if pattern == "empty-shuffled" || pattern == "empty-sorted" {
							base = lengthView{}
						}
						var e treeEditor
						var w lengthWriter
						var out []byte
						run := func() {
							e.Reset(base, mode, true)
							for _, m := range edits {
								e.Set(m.path, m.value)
							}
							if op == "finish" {
								out = e.Append(&w, out[:0])
							}
						}
						run()
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							run()
						}
						b.StopTimer()
						sinkBytes = out
						sinkU = uint64(e.used)
						b.ReportMetric(float64(e.ScratchBytes()), "scratch-B")
						b.ReportMetric(float64(len(edits)), "edits/op")
						b.ReportMetric(float64(len(r.raw)), "base-B")
					})
				}
			}
		}
	}
}
