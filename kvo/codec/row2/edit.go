package row2

import (
	"encoding/binary"
	"slices"

	"github.com/andreyvit/edb/values"
)

// KeyAccessOptions controls property access independently of the field ID.
type KeyAccessOptions uint8

const (
	// Compact selects compact storage when creating a child.
	Compact KeyAccessOptions = 1 << iota
	// Present preserves explicit zero values and present, empty children.
	Present
)

// Step names a schema property and its access options.
type Step struct {
	Key     uint64
	Options KeyAccessOptions
}

// Editor holds a borrowed row until the first write. Snapshot freezes the
// current bytes, including for backends which retain a Put until commit.
// Use one editor by pointer. Do not copy an editor after its first write.
type Editor struct {
	view  View
	owned bool
}

var emptyFast = [...]byte{0, 0, 0, 0, 1}
var emptyCompact = [...]byte{0, 8}

func Empty(layout Layout) View {
	switch layout {
	case Fast2:
		return Trusted(emptyFast[:])
	case Compact2:
		return Trusted(emptyCompact[:])
	default:
		panic(ErrLayout)
	}
}
func Edit(base View) *Editor {
	if base.Missing() || base.Kind() != Object {
		panic(ErrKind)
	}
	return &Editor{view: base}
}
func (e *Editor) Snapshot() View { e.owned = false; return e.view }

// Child returns a missing view for an absent field. A presence-significant
// field can represent a present, empty object using an explicit zero.
func (v View) Child(s Step) View {
	value := v.RawField(s.Key)
	if s.Options&Present != 0 && value.IsZero() && v.Has(s.Key) {
		layout := v.Layout()
		if s.Options&Compact != 0 {
			layout = Compact2
		}
		return Empty(layout)
	}
	return value.Container()
}
func (v View) Has(key uint64) bool {
	if v.Missing() {
		return false
	}
	if v.Kind() != Object {
		panic(ErrKind)
	}
	if v.Layout() == Fast2 {
		h := directory(v.Raw())
		return h.find(key) >= 0
	}
	it := v.Iter()
	for {
		k, _, _, ok := it.Next()
		if !ok || k >= key {
			return ok && k == key
		}
	}
}

// Get borrows from the current edit until the next Set. Snapshot the row
// before taking a view that must survive another write.
func (e *Editor) Get(path []Step, enc Encoding) values.Value {
	v := e.view
	for i, s := range path {
		if i == len(path)-1 {
			return v.Get(s.Key, enc)
		}
		v = v.Child(s)
	}
	panic(ErrKind)
}

// Set replaces a leaf. A scalar with an existing Fast cell changes eight
// bytes in owned storage. Changes in shape or Compact width stream through
// the writer; siblings, including unknown fields, retain their raw payloads.
func (e *Editor) Set(path []Step, value values.Value, enc Encoding) {
	e.SetWithWriter(path, value, enc, nil)
}

// SetWithWriter reuses metadata scratch across edits. The caller can share w
// across editors used sequentially. Each rebuilt row owns its output bytes.
func (e *Editor) SetWithWriter(path []Step, value values.Value, enc Encoding, w *Writer) {
	if len(path) == 0 {
		panic(ErrKind)
	}
	if enc != Data && (!(value.R == 0) || path[len(path)-1].Options&Present != 0) {
		if off, ok := e.cell(path); ok {
			if !e.owned {
				e.view = Trusted(slices.Clone(e.view.Raw()))
				e.owned = true
			}
			binary.LittleEndian.PutUint64(e.view.Raw()[off:], wireWord(value, enc))
			return
		}
	}
	if (value.R == 0) && path[len(path)-1].Options&Present == 0 {
		v := e.view
		for _, step := range path[:len(path)-1] {
			v = v.Child(step)
			if v.Missing() {
				return
			}
		}
		if !v.Has(path[len(path)-1].Key) {
			return
		}
	}
	if w == nil {
		w = AcquireWriter(nil)
		defer w.Release()
	}
	e.rebuild(path, value, enc, w)
}
func (e *Editor) cell(path []Step) (int, bool) {
	v := e.view
	off := 0
	for i, s := range path {
		if v.Layout() != Fast2 || v.Kind() != Object {
			return 0, false
		}
		h := directory(v.Raw())
		idx := h.find(s.Key)
		if idx < 0 {
			return 0, false
		}
		cell := h.cells + idx*8
		if i == len(path)-1 {
			return off + cell, !h.span(idx)
		}
		if !h.span(idx) {
			return 0, false
		}
		u := binary.LittleEndian.Uint64(h.raw[cell:])
		start, size := int(uint32(u)), int(u>>32)
		v = Trusted(h.raw[start : start+size])
		off += start
	}
	return 0, false
}

type suffix struct {
	it        Iterator
	key       uint64
	value     Field
	pending   bool
	keepEmpty bool
}

func (e *Editor) rebuild(path []Step, value values.Value, enc Encoding, w *Writer) {
	w.Reset(make([]byte, 0, len(e.view.Raw())+16))
	var scratch [4]suffix
	tails := scratch[:0]
	v := e.view
	w.Begin(0, "", Object, v.Layout())
	for depth, step := range path {
		tail := suffix{it: v.Iter()}
		if depth > 0 {
			tail.keepEmpty = path[depth-1].Options&Present != 0
		}
		var prev Field
		for {
			k, _, x, ok := tail.it.Next()
			if !ok {
				break
			}
			if k < step.Key {
				w.PutRaw(k, "", x)
				continue
			}
			if k == step.Key {
				prev = x
			} else {
				tail.key, tail.value, tail.pending = k, x, true
			}
			break
		}
		tails = append(tails, tail)
		if depth == len(path)-1 {
			w.put(step.Key, "", value, enc, step.Options&Present != 0)
		} else {
			v = prev.Container()
			if !v.Missing() && v.Kind() != Object {
				panic(ErrKind)
			}
			layout := v.Layout()
			if v.Missing() && step.Options&Compact == 0 {
				layout = Fast2
			}
			w.Begin(step.Key, "", Object, layout)
		}
	}
	for i := len(tails) - 1; i >= 0; i-- {
		t := &tails[i]
		if t.pending {
			w.PutRaw(t.key, "", t.value)
		}
		for {
			k, _, x, ok := t.it.Next()
			if !ok {
				break
			}
			w.PutRaw(k, "", x)
		}
		w.end(t.keepEmpty)
	}
	e.view, e.owned = Trusted(w.BytesWritten()), true
	w.Reset(nil)
}
