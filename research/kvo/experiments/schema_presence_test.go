package kvolab

// Schema-policy experiment over the preserved common checkpoint. The binary
// grammar is unchanged; zero retention is a writer/editor policy.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type propertyFlags uint8

const (
	propertyPresenceIsSignificant propertyFlags = 1 << iota
	propertyFixedSlot
	propertyCompact
)

type propertySpec struct {
	key   uint64
	flags propertyFlags
	span  bool
}
type objectSpec []propertySpec

func (s objectSpec) property(key uint64) propertySpec {
	for _, p := range s {
		if p.key == key {
			return p
		}
	}
	panic(draft.ErrUnknownKey)
}

// A Compact ancestor wins. This is schema policy; opaque bytes can still carry
// preserved unknown children in either physical layout.
func schemaFast(parentFast bool, flags propertyFlags) bool {
	return parentFast && flags&propertyCompact == 0
}

// PutPresent preserves structural presence even for a zero word/empty span.
// Known ordinary fields continue to use Put; rewrites use this for base entries.
func (w *lengthWriter) PutPresent(key uint64, text string, value lengthValue) {
	if !value.IsZero() {
		w.Put(key, text, value)
		return
	}
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	key, _ = w.prepare(key, text)
	f := &w.frames[w.depth-1]
	if f.indexed && f.kind != 0 {
		w.out = binary.LittleEndian.AppendUint64(w.out, 0)
	}
	w.add(key, 0, 0)
}

// Empty modeled children normalize to a present zero when explicitly requested.
func (w *lengthWriter) EndPresent() {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) > 0 || w.depth == 1 {
		w.End()
		return
	}
	key, rollback := f.parentKey, f.rollback
	w.out = w.out[:rollback]
	w.depth--
	parent := &w.frames[w.depth-1]
	if parent.indexed && parent.kind != 0 {
		w.out = binary.LittleEndian.AppendUint64(w.out, 0)
	}
	w.add(key, 0, 0)
}
func presenceLookupIterator(v lengthView, key uint64) (lengthValue, bool) {
	if v.n == 0 {
		return lengthValue{}, false
	}
	if v.Kind() != 0 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		h := lengthIndex(v.Raw())
		i := h.findNumber(key)
		return h.value(i), i >= 0
	}
	// The presence-aware compact path scans the same metadata as Get. Retain this
	// iterator version as a control before specializing a second Get loop.
	it := v.Iter()
	for {
		k, _, value, ok := it.Next()
		if !ok || k > key {
			return lengthValue{}, false
		}
		if k == key {
			return value, true
		}
	}
}

type schemaView struct {
	data lengthView
	spec objectSpec
}

func (v schemaView) Get(key uint64) lengthValue { v.spec.property(key); return v.data.Get(key) }
func (v schemaView) Lookup(key uint64) (lengthValue, bool) {
	p := v.spec.property(key)
	if p.flags&propertyPresenceIsSignificant == 0 {
		panic(draft.ErrValue)
	}
	return presenceLookup(v.data, key)
}
func (v schemaView) Has(key uint64) bool { _, ok := v.Lookup(key); return ok }

type presenceEditor struct {
	lengthEditor
	spec objectSpec
}

func newPresenceEditor(v schemaView, scratch []classMeta, arena []byte) presenceEditor {
	if v.data.n > 0 && v.data.Kind() != 0 {
		panic(draft.ErrKind)
	}
	return presenceEditor{newLengthEditor(v.data, scratch, arena), v.spec}
}
func (e *presenceEditor) Set(key uint64, value lengthValue) {
	p := e.spec.property(key)
	if !value.IsZero() && (value.ptr != nil) != p.span {
		panic(draft.ErrType)
	}
	e.store(key, "", value, false)
	if value.IsZero() && p.flags&propertyPresenceIsSignificant != 0 {
		e.edits[e.locate(key, "")].wire = 0
	}
}
func (e *presenceEditor) Delete(key uint64) {
	e.spec.property(key)
	e.store(key, "", lengthValue{}, true)
}
func (e *presenceEditor) Get(key uint64) lengthValue {
	e.spec.property(key)
	return e.lengthEditor.Get(key, "")
}
func (e *presenceEditor) Lookup(key uint64) (lengthValue, bool) {
	p := e.spec.property(key)
	if p.flags&propertyPresenceIsSignificant == 0 {
		panic(draft.ErrValue)
	}
	i := e.locate(key, "")
	if i < len(e.edits) && e.edits[i].key == key {
		return e.value(e.edits[i]), e.edits[i].wire != 10
	}
	return presenceLookup(e.base, key)
}
func (e *presenceEditor) Append(w *lengthWriter, dst []byte, fast bool) []byte {
	w.Reset(dst)
	w.Begin(0, "", 0, fast)
	w.ReserveEntries(e.base.Len() + len(e.edits))
	it := e.base.Iter()
	key, _, value, ok := it.Next()
	i := 0
	for ok || i < len(e.edits) {
		if i < len(e.edits) && (!ok || e.edits[i].key <= key) {
			m := e.edits[i]
			i++
			if m.wire != 10 {
				w.PutPresent(m.key, "", e.value(m))
			}
			if ok && m.key == key {
				key, _, value, ok = it.Next()
			}
		} else {
			w.PutPresent(key, "", value)
			key, _, value, ok = it.Next()
		}
	}
	w.End()
	return w.BytesWritten()
}

// In-place clear is allowed only when zero remains present. Delete and ordinary
// zero elision rebuild. Raw mapped Bolt values are never passed here.
func (v schemaView) PatchOwned(key, word uint64) bool {
	p := v.spec.property(key)
	if p.span {
		panic(draft.ErrType)
	}
	if word == 0 && p.flags&propertyPresenceIsSignificant == 0 {
		return false
	}
	if !v.data.Indexed() {
		return false
	}
	h := lengthIndex(v.data.Raw())
	i := h.findNumber(key)
	if i < 0 || h.span(i) {
		return false
	}
	binary.LittleEndian.PutUint64(h.raw[h.cells+i*8:], word)
	return true
}

func TestSchemaPresence(t *testing.T) {
	spec := objectSpec{{1, 0, false}, {2, propertyPresenceIsSignificant, false}, {3, propertyPresenceIsSignificant, true}}
	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprint(fast), func(t *testing.T) {
			var w lengthWriter
			w.Reset(nil)
			w.Begin(0, "", 0, fast)
			w.Put(1, "", lengthWord(0))
			w.PutPresent(2, "", lengthWord(0))
			w.Begin(3, "", 0, false)
			w.EndPresent()
			w.PutPresent(98, "", lengthWord(0))
			w.PutPresent(99, "", lengthBytes([]byte{255, 254, 253}))
			w.End()
			raw := w.BytesWritten()
			view, err := lengthOpen(raw)
			if err != nil {
				t.Fatal(err)
			}
			s := schemaView{view, spec}
			if view.Len() != 4 || s.Get(1).Uint() != 0 || !s.Has(2) || !s.Has(3) {
				t.Fatal("presence")
			}
			if _, ok := presenceLookup(view, 1); ok {
				t.Fatal("ordinary zero stored")
			}
			if _, ok := presenceLookup(view, 97); ok {
				t.Fatal("missing")
			}
			if len(s.Get(3).Bytes()) != 0 {
				t.Fatal("empty typed span")
			}
			original := bytes.Clone(raw)
			var records [1]classMeta
			var arena [1]byte
			e := newPresenceEditor(s, records[:0], arena[:0])
			e.Delete(2)
			if _, ok := e.Lookup(2); ok {
				t.Fatal("deleted zero")
			}
			e.Set(2, lengthWord(0))
			if _, ok := e.Lookup(2); !ok {
				t.Fatal("set zero")
			}
			e.Set(1, lengthWord(7))
			e.Set(3, lengthBytes([]byte("owns this span")))
			e.Set(1, lengthWord(0))
			for _, outFast := range []bool{false, true} {
				out := e.Append(&w, nil, outFast)
				v, err := lengthOpen(out)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := presenceLookup(v, 1); ok {
					t.Fatal("ordinary zero returned")
				}
				if x, ok := presenceLookup(v, 2); !ok || !x.IsZero() {
					t.Fatal("known zero lost")
				}
				if x, ok := presenceLookup(v, 98); !ok || !x.IsZero() {
					t.Fatal("unknown zero lost")
				}
				if !bytes.Equal(v.Get(99).Bytes(), []byte{255, 254, 253}) {
					t.Fatal("unknown span changed")
				}
				if string(v.Get(3).Bytes()) != "owns this span" {
					t.Fatal("span edit")
				}
			}
			if !bytes.Equal(original, raw) {
				t.Fatal("borrowed base changed")
			}
			if s.PatchOwned(2, 0) != fast {
				t.Fatal("present zero patch")
			}
			if s.PatchOwned(1, 0) {
				t.Fatal("absent zero patch")
			}
		})
	}
	for _, flags := range []propertyFlags{0, propertyCompact, propertyPresenceIsSignificant} {
		if schemaFast(false, flags) {
			t.Fatal("fast inside compact")
		}
	}
	if !schemaFast(true, 0) || schemaFast(true, propertyCompact) {
		t.Fatal("layout inheritance")
	}
}
func TestSchemaPresenceUsage(t *testing.T) {
	s := schemaView{spec: objectSpec{{1, 0, false}, {2, propertyPresenceIsSignificant, false}}}
	for _, f := range []func(){func() { s.Get(99) }, func() { s.Lookup(1) }, func() { e := newPresenceEditor(s, nil, nil); e.Set(1, lengthBytes([]byte("wrong"))) }} {
		func() {
			defer func() {
				p := recover()
				if p != draft.ErrUnknownKey && p != draft.ErrValue && p != draft.ErrType {
					t.Fatalf("usage panic %v", p)
				}
			}()
			f()
			t.Fatal("missing panic")
		}()
	}
	if unsafe.Sizeof(lengthValue{}) != 16 {
		t.Fatal("value grew")
	}
}
func BenchmarkSchemaPresence(b *testing.B) {
	for _, fast := range []bool{false, true} {
		for _, presentZero := range []bool{false, true} {
			var w lengthWriter
			w.Reset(nil)
			w.Begin(0, "", 0, fast)
			var spec objectSpec
			for i := uint64(1); i <= 128; i++ {
				spec = append(spec, propertySpec{i, propertyPresenceIsSignificant, false})
				v := lengthWord(i)
				if presentZero {
					v = lengthWord(0)
				}
				w.PutPresent(i, "", v)
			}
			w.End()
			raw := w.BytesWritten()
			s := schemaView{lengthTrusted(raw), spec}
			b.Run(fmt.Sprintf("fast%v/zero%v/lookup", fast, presentZero), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					v, ok := presenceLookup(s.data, uint64(i&127)+1)
					if ok {
						sum += v.num
					}
				}
				sinkU = sum
			})
			b.Run(fmt.Sprintf("fast%v/zero%v/edit", fast, presentZero), func(b *testing.B) {
				var records [8]classMeta
				var arena [64]byte
				var writer lengthWriter
				dst := make([]byte, 0, len(raw)+64)
				e := newPresenceEditor(s, records[:0], arena[:0])
				dst = e.Append(&writer, dst[:0], fast)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					e := newPresenceEditor(s, records[:0], arena[:0])
					e.Set(2, lengthWord(uint64(i&1)))
					dst = e.Append(&writer, dst[:0], fast)
				}
				sinkBytes = dst
			})
		}
	}
}

// Same metadata loop as the ordinary getter, with structural presence retained.
func presenceLookup(v lengthView, target uint64) (lengthValue, bool) {
	if v.n == 0 {
		return lengthValue{}, false
	}
	if v.Kind() != 0 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		h := lengthIndex(v.Raw())
		i := h.findNumber(target)
		return h.value(i), i >= 0
	}
	_, n, shared, payload, meta := lengthHeader(v.Raw())
	var key uint64
	pos := 0
	if shared <= 8 {
		// Uniform scalar records do not need to read any value lengths.
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*int(shared), int(shared), shared), true
			}
			if key > target {
				break
			}
		}
		return lengthValue{}, false
	}
	for i := 0; i < n; i++ {
		code := shared
		var d uint64
		if shared == 15 {
			d, code, meta = lengthMixed(meta)
		} else {
			d, meta = lengthU(meta)
		}
		key += d
		size := int(code)
		if code == 9 {
			u, r := lengthU(meta)
			size = int(u)
			meta = r
		}
		if key == target {
			return lengthCell(payload, pos, size, code), true
		}
		if key > target {
			break
		}
		pos += size
	}
	return lengthValue{}, false
}

func BenchmarkSchemaPresenceLookupControl(b *testing.B) {
	for _, fast := range []bool{false, true} {
		for _, zeros := range []bool{false, true} {
			var w lengthWriter
			w.Reset(nil)
			w.Begin(0, "", 0, fast)
			for i := uint64(1); i <= 128; i++ {
				word := i
				if zeros {
					word = 0
				}
				w.PutPresent(i, "", lengthWord(word))
			}
			w.End()
			v := lengthTrusted(w.BytesWritten())
			for _, mode := range []string{"iterator", "direct", "ordinary"} {
				b.Run(fmt.Sprintf("fast%v/zeros%v/%s", fast, zeros, mode), func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					for i := 0; i < b.N; i++ {
						key := uint64(i&127) + 1
						switch mode {
						case "iterator":
							x, ok := presenceLookupIterator(v, key)
							if ok {
								sum += x.num
							}
						case "direct":
							x, ok := presenceLookup(v, key)
							if ok {
								sum += x.num
							}
						default:
							sum += v.Get(key).num
						}
					}
					sinkU = sum
				})
			}
		}
	}
}
