package kvo

import (
	"reflect"
	"strings"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Encoding describes a property's wire conversion, independently of its runtime
// carrier. Containers remain explicit encoded byte spans; Child navigates them.
func Encoding(t AnyType) row2.Encoding {
	if t == TTimestamp {
		return row2.Timestamp
	}
	if w, ok := t.(*WordType); ok {
		switch w.NativeType().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return row2.Signed
		case reflect.Float32:
			return row2.Float32
		default:
			return row2.Word
		}
	}
	return row2.Data
}
func (p *nativePlan) encoding() row2.Encoding { return p.wireEncoding }
func (r *recordView) propertyEncoding(key uint64) row2.Encoding {
	if r.layout != nil {
		return r.layout.encoding(key)
	}
	if r.plan != nil {
		return r.plan.field(key).plan.encoding()
	}
	panic("record: typed access requires a model; use GetAs for an explicitly typed raw field")
}

// GetAs supplies a wire interpretation for a model-less record. Prefer Get on
// modeled records. The returned string/bytes borrow the record's lifetime.
func (r recordView) GetAs(key uint64, enc row2.Encoding) values.Value {
	if r.memory != nil {
		return r.memory.getAs(key, enc)
	}
	if r.object != nil || !r.msg.Missing() {
		return r.Get(key)
	}
	return r.wire.Get(key, enc)
}

// GetOwnedString returns a string safe to retain beyond the source transaction.
// Native strings and owned decoded memory need no copy. It is only valid for
// a string property; raw byte or collection properties are not strings.
func (r recordView) GetOwnedString(key uint64) values.Value {
	v := r.Get(key)
	if r.object == nil && r.memory == nil {
		return values.String(strings.Clone(v.AsString()))
	}
	return v
}
func (e *Editor) WithModel(model *Model) *Editor { e.view = e.view.WithModel(model); return e }
func (e *Editor) pathEncoding(path []row2.Step) row2.Encoding {
	if len(path) == 0 {
		panic(row2.ErrKind)
	}
	layout := e.view.layout
	if layout != nil {
		for _, s := range path[:len(path)-1] {
			layout = layout.field(s.Key).child
		}
		key := path[len(path)-1].Key
		if enc := layout.encodingSlot(key); enc != 0 {
			return row2.Encoding(enc - 1)
		}
		return layout.fieldSlow(key).encoding
	}
	plan := e.view.plan
	if plan != nil {
		for _, s := range path {
			plan = indirectPlan(plan).field(s.Key).plan
		}
		return plan.encoding()
	}
	panic("record: typed access requires a model; use SetAs for an explicitly typed raw field")
}

// OwnsData reports whether scalar spans are backed by Go memory independent of
// the source transaction. It does not promise immutability of the record itself.
func (r recordView) OwnsData() bool { return r.object != nil || r.memory != nil }

// GetAs and SetAs supply the leaf encoding for schema-free editors.
func (e *Editor) GetAs(path []row2.Step, enc row2.Encoding) values.Value {
	e.checkAttached()
	if len(path) == 0 {
		panic(row2.ErrKind)
	}
	if !e.view.msg.Missing() && len(path) == 1 {
		return e.GetPathValue(path)
	}
	if len(path) > 1 && e.Model() != nil && e.packedMode != sharedPackedEdit {
		if child := e.editChild(path[0].Key); child != nil {
			return child.GetAs(path[1:], enc)
		}
		return values.Value{}
	}
	if e.wire != nil {
		if f := e.pendingField(path[0].Key); f != nil {
			if f.encoding != enc {
				return e.Snapshot().Get(path[0].Key, enc)
			}
			return editFieldValue(*f)
		}
		return e.wire.Get(path, enc)
	}
	r := e.view
	for _, s := range path[:len(path)-1] {
		r = r.Child(s)
	}
	return r.GetAs(path[len(path)-1].Key, enc)
}

// SetAs panics on error. Use TrySetAs to handle errors explicitly.
func (e *Editor) SetAs(path []row2.Step, x values.Value, enc row2.Encoding) {
	err := e.TrySetAs(path, x, enc)
	if err != nil {
		panic(err)
	}
}

func (e *Editor) TrySetAs(path []row2.Step, x values.Value, enc row2.Encoding) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = recoveredError("property assignment", v)
		}
	}()
	e.checkAttached()
	if len(path) == 0 {
		return row2.ErrKind
	}
	if e.wire != nil {
		if len(path) == 1 {
			e.setPackedField(path[0], x, enc)
			return nil
		}
		e.flush()
		e.wire.Set(path, x, enc)
		e.touch()
		return nil
	}
	if e.view.memory != nil && e.view.layout == nil {
		e.view.memory.set(path, x, enc)
		e.touch()
		return nil
	}
	return e.TryFastSetPathValue(path, x)
}

func (e *Editor) OwnsData() bool { return e.wire == nil && e.view.OwnsData() }
