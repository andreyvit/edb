package kvolab

// Same one-shot leaf merge and preflighted physical patch algorithms as the
// preceding candidate, with the selected signed value and container adapters.
import (
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"slices"
	"unsafe"
)

type currentLeaf struct {
	path  []perfStep
	value currentValue
}

func currentPath(raw []byte, path []perfStep) currentValue {
	var value currentValue
	for i, step := range path {
		v := currentTrusted(raw)
		switch step.kind {
		case 0:
			value = v.Get(step.key)
		case 1:
			value = v.GetString(step.text)
		case 2:
			value = v.At(int(step.key))
		default:
			panic(draft.ErrKind)
		}
		if i+1 < len(path) {
			raw = value.Bytes()
		}
	}
	return value
}

type currentLeafMerger struct {
	order []int
	edits []currentLeaf
	zeros []bool
	fast  bool
}

func (e *currentLeafMerger) Append(w *currentWriter, dst []byte, base currentView, edits []currentLeaf, keepZero []bool, fast bool) []byte {
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

func (e *currentLeafMerger) emit(w *currentWriter, base currentView, order []int, depth, kind int, key uint64, text string) {
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
			currentPut(w, k, unsafe.String(unsafe.SliceData(s), len(s)), v)
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
				currentPut(w, listNext, "", currentValue{})
				listNext++
			}
			listNext++
		}
		edit := e.edits[order[0]]
		if len(edit.path) == depth+1 {
			keep := kind == 2 || len(e.zeros) != 0 && e.zeros[order[0]]
			if keep || !edit.value.IsZero() {
				currentPut(w, step.key, step.text, edit.value)
			}
		} else {
			var child currentView
			if cmp == 0 && !v.IsZero() {
				child = currentTrusted(v.Bytes())
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

func currentPatchPlan(base []byte, edits []currentLeaf, dst []patchOffset) ([]patchOffset, bool) {
	if len(base) > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	dst = dst[:0]
	for _, edit := range edits {
		if len(edit.path) == 0 {
			panic(draft.ErrKind)
		}
		raw := base
		offset := 0
		for i, s := range edit.path {
			at, size, span, found := patchLocateFused(raw, s)
			if !found {
				return dst[:0], false
			}
			if i+1 < len(edit.path) {
				if !span {
					return dst[:0], false
				}
				raw = raw[at : at+size]
				offset += at
				continue
			}
			v := edit.value
			if s.kind != 2 && v.IsZero() {
				return dst[:0], false
			}
			if span != v.isSpan() || span && int(v.num) != size {
				return dst[:0], false
			}
			if len(dst) == 0 {
				dst = slices.Grow(dst, len(edits))
			}
			dst = append(dst, patchOffset{uint32(offset + at), uint32(size)})
		}
	}
	return dst, true
}

func currentPatchApplyOwned(dst []byte, offsets []patchOffset, edits []currentLeaf) {
	if len(offsets) != len(edits) {
		panic(draft.ErrValue)
	}
	for i, ref := range offsets {
		value := edits[i].value
		if value.isSpan() {
			copy(dst[int(ref.at):int(ref.at)+int(ref.size)], value.Bytes())
		} else {
			binary.LittleEndian.PutUint64(dst[int(ref.at):], value.num)
		}
	}
}
