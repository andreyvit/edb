package kvo

import (
	"bytes"
	"slices"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// WithModel supplies the logical field types for normalized access and eager
// generic decoding. It does not attach a native Go binding.
func (r recordView) WithModel(m *Model) recordView {
	if r.layout != nil && r.layout.model == m {
		return r
	}
	return r.withModel(m)
}
func (r recordView) withModel(m *Model) recordView {
	if r.plan != nil && r.plan.model != m {
		panic("record: incompatible model")
	}
	if r.memory != nil && r.memory.model() != m {
		panic("record: attach model before Fastest")
	}
	r.layout = layoutFor(m)
	return r
}

type memoryField struct {
	key      uint64
	value    values.Value
	child    *memoryObject
	present  bool
	encoding row2.Encoding
}
type rawMemoryField struct {
	key   uint64
	field row2.Field
}
type memoryObject struct {
	layout  *modelLayout
	fields  []memoryField
	unknown []rawMemoryField
	storage row2.Layout
	raw     row2.View // a schema-free owned object: interpret only through GetAs/Child
}

func (m *memoryObject) model() *Model {
	if m.layout == nil {
		return nil
	}
	return m.layout.model
}
func (m *memoryObject) field(k uint64) *memoryField {
	if p := m.layout; p != nil {
		if i := k - p.base; i < uint64(len(p.index)) {
			if n := p.index[i]; n != 0 {
				return &m.fields[n-1]
			}
		}
	}
	return m.fieldSlow(k)
}
func (m *memoryObject) fieldSlow(k uint64) *memoryField {
	i, ok := slices.BinarySearchFunc(m.fields, k, func(f memoryField, k uint64) int {
		if f.key < k {
			return -1
		}
		if f.key > k {
			return 1
		}
		return 0
	})
	if ok {
		return &m.fields[i]
	}
	return nil
}
func (m *memoryObject) get(k uint64) values.Value {
	f := m.field(k)
	if f == nil {
		panic("record: typed access requires a modeled property; use GetAs")
	}
	if !f.present {
		return values.Value{}
	}
	if f.child != nil {
		return values.Bytes(f.child.snapshot().Raw())
	}
	return f.value
}
func (m *memoryObject) getAs(k uint64, enc row2.Encoding) values.Value {
	if !m.raw.Missing() {
		return m.raw.Get(k, enc)
	}
	if f := m.field(k); f != nil {
		return m.get(k)
	}
	for _, f := range m.unknown {
		if f.key == k {
			return f.field.Decode(enc)
		}
	}
	return values.Value{}
}
func (m *memoryObject) has(k uint64) bool {
	if !m.raw.Missing() {
		return m.raw.Has(k)
	}
	if f := m.field(k); f != nil {
		return f.present
	}
	for _, f := range m.unknown {
		if f.key == k {
			return true
		}
	}
	return false
}
func decodeMemory(v row2.View, layout *modelLayout, depth int) *memoryObject {
	if depth > 256 {
		panic(row2.ErrLimit)
	}
	if v.Kind() != row2.Object {
		panic(row2.ErrKind)
	}
	m := &memoryObject{layout: layout, storage: v.Layout()}
	if layout == nil {
		m.raw = v
		return m
	}
	m.fields = make([]memoryField, len(layout.fields))
	for i, f := range layout.fields {
		m.fields[i] = memoryField{key: f.key, encoding: f.encoding}
	}
	it := v.Iter()
	for {
		k, _, x, ok := it.Next()
		if !ok {
			break
		}
		f := m.field(k)
		if f == nil {
			m.unknown = append(m.unknown, rawMemoryField{k, x})
			continue
		}
		f.present = true
		f.value = x.Decode(f.encoding)
		p := layout.model.MustPropByCode(k)
		if child := p.TypeModel(); child != nil {
			flags := layout.model.PropFlags(k)
			var options row2.KeyAccessOptions
			if flags&Compact != 0 {
				options |= row2.Compact
			}
			if flags&PresenceIsSignificant != 0 {
				options |= row2.Present
			}
			cv := v.Child(row2.Step{Key: k, Options: options})
			if !cv.Missing() {
				f.child = decodeMemory(cv, layout.field(k).child, depth+1)
				f.value = values.Value{}
			}
		}
	}
	return m
}
func (m *memoryObject) child(s row2.Step) recordView {
	if !m.raw.Missing() {
		v := m.raw.Child(s)
		if v.Missing() {
			return recordView{}
		}
		return recordView{memory: decodeMemory(v, nil, 0)}
	}
	f := m.field(s.Key)
	if f == nil || !f.present {
		return recordView{}
	}
	if f.child != nil {
		return recordView{memory: f.child, layout: f.child.layout}
	}
	v := row2.Trusted(f.value.AsBytes())
	if s.Options&row2.Present != 0 && f.value.R == 0 {
		layout := m.storage
		if s.Options&row2.Compact != 0 {
			layout = row2.Compact2
		}
		v = row2.Empty(layout)
	}
	if v.Missing() {
		return recordView{}
	}
	return recordView{memory: decodeMemory(v, nil, 0)}
}
func (m *memoryObject) emit(w *row2.Writer, key uint64, keep bool) {
	if !m.raw.Missing() {
		w.PutPresent(key, "", values.Bytes(m.raw.Raw()), row2.Data)
		return
	}
	w.Begin(key, "", row2.Object, m.storage)
	u := 0
	for _, f := range m.fields {
		for u < len(m.unknown) && m.unknown[u].key < f.key {
			v := m.unknown[u]
			w.PutRaw(v.key, "", v.field)
			u++
		}
		if !f.present {
			continue
		}
		if f.child != nil {
			f.child.emit(w, f.key, true)
		} else {
			w.PutPresent(f.key, "", f.value, f.encoding)
		}
	}
	for ; u < len(m.unknown); u++ {
		v := m.unknown[u]
		w.PutRaw(v.key, "", v.field)
	}
	w.Finish(keep)
}
func (m *memoryObject) snapshot() row2.View {
	if !m.raw.Missing() {
		return m.raw
	}
	var w row2.Writer
	m.emit(&w, 0, true)
	return row2.Trusted(w.BytesWritten())
}
func (m *memoryObject) empty() bool {
	if !m.raw.Missing() {
		return m.raw.Len() == 0
	}
	for _, f := range m.fields {
		if f.present {
			return false
		}
	}
	return len(m.unknown) == 0
}
func (m *memoryObject) set(path []row2.Step, x values.Value, enc row2.Encoding) {
	if !m.raw.Missing() {
		e := row2.Edit(m.raw)
		e.Set(path, x, enc)
		m.raw = e.Snapshot()
		return
	}
	step := path[0]
	f := m.field(step.Key)
	if f == nil {
		panic("record: editing unknown properties requires an explicitly typed raw editor")
	}
	if len(path) == 1 {
		if enc == row2.Timestamp && x.P != nil {
			x = values.Bytes(bytes.Clone(x.AsBytes()))
		}
		next := memoryField{key: f.key, encoding: f.encoding, present: x.R != 0 || step.Options&row2.Present != 0, value: x}
		if next.encoding == row2.Data {
			if f.child == nil && f.present == next.present && bytes.Equal(f.value.AsBytes(), x.AsBytes()) {
				return
			}
			next.value = values.Bytes(bytes.Clone(x.AsBytes()))
		}
		if next.present && next.encoding == row2.Data {
			if cm := m.layout.field(step.Key).child; cm != nil {
				v := row2.Trusted(next.value.AsBytes())
				if v.Missing() && step.Options&row2.Present != 0 {
					layout := row2.Fast2
					if step.Options&row2.Compact != 0 {
						layout = row2.Compact2
					}
					v = row2.Empty(layout)
				}
				if !v.Missing() {
					next.child = decodeMemory(v, cm, 0)
					next.value = values.Value{}
				}
			}
		}
		*f = next
		return
	}
	if f.child == nil {
		if !f.present && x.R == 0 && path[len(path)-1].Options&row2.Present == 0 {
			return
		}
		v := row2.Trusted(f.value.AsBytes())
		if v.Missing() {
			layout := row2.Fast2
			if step.Options&row2.Compact != 0 {
				layout = row2.Compact2
			}
			v = row2.Empty(layout)
		}
		f.child = decodeMemory(v, m.layout.field(step.Key).child, 0)
		f.value = values.Value{}
	}
	f.child.set(path[1:], x, enc)
	f.present = !f.child.empty() || step.Options&row2.Present != 0
	if !f.present {
		f.child = nil
	}
}
func (e *Editor) setMemory(path []row2.Step, x values.Value) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = recoveredError("decoded property assignment", v)
		}
	}()
	if len(path) == 0 {
		return row2.ErrKind
	}
	if len(path) > 256 {
		return row2.ErrLimit
	}
	e.view.memory.set(path, x, row2.Word)
	if len(path) == 1 {
		e.detachChild(path[0].Key)
		e.detachCollection(path[0].Key)
	}
	e.touch()
	return nil
}
