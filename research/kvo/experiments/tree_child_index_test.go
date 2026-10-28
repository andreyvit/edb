package kvolab

// Two simple in-memory child indexes for broad recursive edits. The original
// sibling-list editor remains intact. Copied navigation/emission methods keep
// the representation and overlay policy identical while isolating this choice.
import (
	"bytes"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type treeChildKey struct {
	parent int
	key    uint64
	text   string
}
type indexedTreeEditor struct {
	keysLastTreeEditor
	orders    [][]int
	byChild   map[treeChildKey]int
	childMode string
}

func (e *indexedTreeEditor) Reset(base keysLastView, mode string, indexed bool, childMode string) {
	e.childMode = childMode
	if childMode == "hash" {
		if e.byChild == nil {
			e.byChild = make(map[treeChildKey]int)
		} else {
			clear(e.byChild)
		}
	}
	e.used = 0
	e.mode = mode
	e.indexed = indexed
	e.newNode(-1, perfStep{}, base, 0)
}
func (e *indexedTreeEditor) newNode(parent int, step perfStep, base keysLastView, kind int) int {
	i := e.used
	e.used++
	if i == len(e.nodes) {
		e.nodes = append(e.nodes, keysLastTreeNode{})
	}
	if i == len(e.orders) {
		e.orders = append(e.orders, nil)
	}
	e.orders[i] = e.orders[i][:0]
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
		if e.childMode == "hash" {
			e.byChild[treeChildKey{parent, step.key, step.text}] = i
		} else {
			at, exists := e.childPosition(parent, step)
			order := e.orders[parent]
			if exists {
				order[at] = i
			} else {
				order = append(order, 0)
				copy(order[at+1:], order[at:])
				order[at] = i
			}
			e.orders[parent] = order
		}
	}
	return i
}
func (e *indexedTreeEditor) childPosition(parent int, s perfStep) (int, bool) {
	order := e.orders[parent]
	lo, hi := 0, len(order)
	for lo < hi {
		m := lo + (hi-lo)/2
		x := e.nodes[order[m]].step
		if x.key < s.key || x.key == s.key && x.text < s.text {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(order) && e.nodes[order[lo]].step.key == s.key && e.nodes[order[lo]].step.text == s.text
}

// Keep the overwhelmingly common no-edited-child case small enough to inline.
// The original all-index lookup remains frozen in source-index-20261016.tar.gz.
func (e *indexedTreeEditor) child(parent int, s perfStep) int {
	if e.nodes[parent].first < 0 {
		return -1
	}
	return e.childPopulated(parent, s)
}
func (e *indexedTreeEditor) childPopulated(parent int, s perfStep) int {
	first := e.nodes[parent].first
	if e.nodes[first].next < 0 {
		n := &e.nodes[first]
		if n.active && n.step.key == s.key && n.step.text == s.text {
			return first
		}
		return -1
	}
	if e.childMode == "hash" {
		if at, ok := e.byChild[treeChildKey{parent, s.key, s.text}]; ok && e.nodes[at].active {
			return at
		}
		return -1
	}
	at, ok := e.childPosition(parent, s)
	if !ok {
		return -1
	}
	i := e.orders[parent][at]
	if e.nodes[i].active {
		return i
	}
	return -1
}
func (e *indexedTreeEditor) Set(path []perfStep, v lengthValue) {
	e.setPath(path, v, false)
}

// A schema binding with PresenceIsSignificant calls SetPresent for its leaf.
func (e *indexedTreeEditor) SetPresent(path []perfStep, v lengthValue) {
	e.setPath(path, v, true)
}
func (e *indexedTreeEditor) setPath(path []perfStep, v lengthValue, keepZero bool) {
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
			child = e.newNode(parent, s, keysLastTrusted(base.Bytes()), path[i+1].kind)
			// The placeholder keeps a newly inserted child in the parent merge.
			// Its actual value is emitted recursively and never stored on disk.
			e.set(parent, s, lengthWord(1), false)
		}
		parent = child
	}
}
func (e *indexedTreeEditor) set(node int, s perfStep, v lengthValue, keepZero bool) {
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
func (e *indexedTreeEditor) lookup(node int, s perfStep) lengthValue {
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
func (e *indexedTreeEditor) GetLeaf(path []perfStep) lengthValue {
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
			return keysLastPath(v.Bytes(), path[i+1:])
		}
		return v
	}
	return lengthValue{}
}
func (e *indexedTreeEditor) Append(w *keysLastWriter, dst []byte) []byte {
	w.Reset(dst)
	e.emit(w, 0, 0, "")
	return w.BytesWritten()
}
func (e *indexedTreeEditor) emit(w *keysLastWriter, node int, key uint64, text string) {
	n := &e.nodes[node]
	w.Begin(key, text, n.edit.storage.kind, n.indexed)
	listNext := uint64(0)
	n.edit.visit(func(k uint64, s string, v lengthValue) {
		if n.edit.storage.kind == 2 {
			for listNext < k {
				keysLastPut(w, listNext, "", lengthValue{})
				listNext++
			}
			listNext = k + 1
		}
		if child := e.child(node, perfStep{key: k, text: s}); child >= 0 {
			e.emit(w, child, k, s)
		} else {
			keysLastPut(w, k, s, v)
		}
	})
	w.End()
}
func (e *indexedTreeEditor) ScratchBytes() int {
	n := e.keysLastTreeEditor.ScratchBytes() + cap(e.orders)*24
	for _, order := range e.orders {
		n += cap(order) * 8
	}
	return n // Hash-map runtime storage is measured through cold allocations.
}

func TestTreeChildIndex(t *testing.T) {
	for name, r := range rowFixtures(t) {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var w keysLastWriter
			raw := keysLastEncode(&w, nil, r.source, policy)
			for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled"} {
				base := keysLastTrusted(raw)
				if pattern == "empty-shuffled" {
					base = keysLastView{}
				}
				edits := rowEdits(r, pattern)
				var original keysLastTreeEditor
				original.Reset(base, "append", true)
				for _, edit := range edits {
					original.Set(edit.path, edit.value)
				}
				want := original.Append(&w, nil)
				for _, mode := range []string{"sorted", "hash"} {
					var e indexedTreeEditor
					for repetition := 0; repetition < 2; repetition++ {
						e.Reset(base, "append", true, mode)
						for _, edit := range edits {
							e.Set(edit.path, edit.value)
							if !equalLengthValue(e.GetLeaf(edit.path), edit.value) {
								t.Fatal(name, mode, pattern, "read after set")
							}
						}
						if got := e.Append(&w, nil); !bytes.Equal(got, want) {
							t.Fatal(name, mode, pattern, "output differs")
						}
					}
				}
			}
		}
	}
	// Replacing an ancestor must retire its old child index, including after
	// reuse of scratch. An explicit zero must survive the normalized output.
	for _, mode := range []string{"sorted", "hash"} {
		var e indexedTreeEditor
		var w keysLastWriter
		source := Obj(Field{1, Obj(Field{2, U(3)}, Field{4, U(5)})})
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		for repeat := 0; repeat < 3; repeat++ {
			e.Reset(keysLastTrusted(raw), "append", true, mode)
			e.Set([]perfStep{{key: 1}, {key: 2}}, lengthWord(7))
			e.Set([]perfStep{{key: 1}}, lengthValue{})
			e.Set([]perfStep{{key: 1}, {key: 9}}, lengthWord(11))
			e.SetPresent([]perfStep{{key: 1}, {key: 10}}, lengthWord(0))
			out := e.Append(&w, nil)
			child := keysLastTrusted(keysLastTrusted(out).Get(1).Bytes())
			if child.Len() != 2 || child.Get(9).Uint() != 11 || child.Get(2).Uint() != 0 || child.Get(4).Uint() != 0 {
				t.Fatal(mode, "ancestor replacement")
			}
		}
	}
}

func BenchmarkTreeChildIndex(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		var writer keysLastWriter
		raw := keysLastEncode(&writer, nil, r.source, func(int, Value) bool { return true })
		d := newMmapDataset(b, raw, 1024)
		warmMmapPayloads(d)
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "empty-shuffled", "interleave"} {
			p := pattern
			if p == "interleave" {
				p = "wide70"
			}
			edits := rowEdits(r, p)
			for _, mode := range []string{"linear", "sorted", "hash"} {
				for _, cold := range []bool{false, true} {
					label := "reuse"
					if cold {
						label = "cold"
					}
					b.Run(name+"/"+pattern+"/"+mode+"/"+label, func(b *testing.B) {
						var original keysLastTreeEditor
						var e indexedTreeEditor
						var w keysLastWriter
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
								original = keysLastTreeEditor{}
								e = indexedTreeEditor{}
							}
							if mode == "linear" {
								original.Reset(base, "append", true)
								for i, edit := range edits {
									original.Set(edit.path, edit.value)
									if pattern == "interleave" {
										sinkValue = original.GetLeaf(r.leaves[(i*17+3)%len(r.leaves)].path)
									}
								}
								out = original.Append(&w, out[:0])
							} else {
								e.Reset(base, "append", true, mode)
								for i, edit := range edits {
									e.Set(edit.path, edit.value)
									if pattern == "interleave" {
										sinkValue = e.GetLeaf(r.leaves[(i*17+3)%len(r.leaves)].path)
									}
								}
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
						b.ReportMetric(float64(len(edits)), "edits/op")
						b.ReportMetric(float64(len(raw)), "row-B")
					})
				}
			}
		}
	}
}
