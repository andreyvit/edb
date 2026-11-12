package kvo

import (
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// ListValue borrows a list property without constructing a slice or converting its
// encoding. Its lifetime is the source record's lifetime. Scalar and string
// elements are read without allocations; aggregate elements use GetValue's encoding.
type ListValue struct {
	data     values.Value
	plan     *nativePlan
	encoding row2.Encoding
	kind     representationKind
}

// ListValue returns a borrowed view of a modeled list property. Unlike GetValue, it does
// not materialize the list. Retain the record with Own before retaining its list
// beyond the source transaction. It panics for non-list properties.
func (r Record) ListValue(key uint64) ListValue {
	if r.Missing() {
		return ListValue{}
	}
	e := r.representation()
	if e.plan != nil {
		f := e.plan.field(key)
		p := f.plan
		if p.kind != nativeList {
			panic(row2.ErrKind)
		}
		v := ListValue{plan: p, encoding: p.elem.wireEncoding, kind: e.kind}
		switch e.kind {
		case messagePackRepresentation, ownedMessagePackRepresentation:
			raw := r.expanded().messagePackField(f)
			if p.msgFallback {
				return ListValue{data: r.GetValue(key), encoding: p.elem.wireEncoding, kind: compactRepresentation}
			}
			v.data = values.Bytes(raw.Raw())
			return v
		case nativeRepresentation:
			v.data = f.access.Get(r.value.P)
			return v
		}
	}
	if e.layout == nil {
		panic("record: ListValue requires a model")
	}
	f := e.layout.field(key)
	if !f.list {
		panic(row2.ErrKind)
	}
	return ListValue{data: r.GetValue(key), encoding: f.itemEncoding, kind: compactRepresentation}
}

func (l ListValue) native() reflect.Value { return reflect.NewAt(l.plan.typ, l.data.P).Elem() }

func (l ListValue) Missing() bool {
	if l.data.P == nil {
		return true
	}
	switch l.kind {
	case nativeRepresentation:
		v := l.native()
		return (v.Kind() == reflect.Slice || v.Kind() == reflect.Map) && v.IsNil()
	case messagePackRepresentation, ownedMessagePackRepresentation:
		return msgpack.Trusted(l.data.AsBytes()).Nil()
	default:
		return false
	}
}

func (l ListValue) Len() int {
	if l.plan != nil && l.plan.typ.Kind() == reflect.Array {
		return l.plan.typ.Len()
	}
	if l.data.P == nil {
		return 0
	}
	switch l.kind {
	case messagePackRepresentation, ownedMessagePackRepresentation:
		raw := msgpack.Trusted(l.data.AsBytes())
		if l.plan.typ.Elem().Kind() == reflect.Uint8 {
			return len(raw.Bytes())
		}
		it := raw.Iter()
		return it.Len()
	case nativeRepresentation:
		return l.native().Len()
	default:
		return row2.Trusted(l.data.AsBytes()).Len()
	}
}

// At reads one element; MessagePack and Compact2 require a linear scan. Prefer
// Iter for sequential access. The returned string/bytes may borrow source data.
func (l ListValue) At(index int) values.Value {
	if index < 0 || index >= l.Len() {
		panic("record: list index out of range")
	}
	if l.kind != nativeRepresentation && l.kind != messagePackRepresentation && l.kind != ownedMessagePackRepresentation {
		return row2.Trusted(l.data.AsBytes()).At(index, l.encoding)
	}
	if l.kind == nativeRepresentation {
		return nativeValue(l.plan.elem, l.native().Index(index), row2.Fast2)
	}
	it := l.Iter()
	for i := 0; ; i++ {
		v, _ := it.Next()
		if i == index {
			return v
		}
	}
}

// ListValueIterator visits scalar elements without allocating or rescanning earlier
// elements. Keep it as a local value; it borrows the list's source data.
type ListValueIterator struct {
	list  ListValue
	msg   msgpack.Iterator
	wire  row2.Iterator
	index int
}

func (l ListValue) Iter() ListValueIterator {
	it := ListValueIterator{list: l}
	if l.data.P == nil {
		return it
	}
	switch l.kind {
	case messagePackRepresentation, ownedMessagePackRepresentation:
		if l.plan.typ.Elem().Kind() != reflect.Uint8 {
			it.msg = msgpack.Trusted(l.data.AsBytes()).Iter()
		}
	case nativeRepresentation:
	default:
		it.wire = row2.Trusted(l.data.AsBytes()).Iter()
	}
	return it
}
func (it *ListValueIterator) Next() (values.Value, bool) {
	l := &it.list
	if l.data.P == nil && l.Len() == 0 {
		return values.Value{}, false
	}
	switch l.kind {
	case messagePackRepresentation, ownedMessagePackRepresentation:
		if it.index >= l.Len() {
			return values.Value{}, false
		}
		i := it.index
		it.index++
		if l.plan.typ.Elem().Kind() == reflect.Uint8 {
			data := msgpack.Trusted(l.data.AsBytes()).Bytes()
			if i >= len(data) {
				return values.Value{}, true
			}
			return values.Uint(uint64(data[i])), true
		}
		_, v, ok := it.msg.Next()
		if !ok {
			return values.Value{}, l.plan.typ.Kind() == reflect.Array
		}
		p := l.plan.elem
		if p.msgFallback || p.kind == nativeJSON {
			return propertyValue(p, v, row2.Fast2), true
		}
		if p.kind <= nativeBytes || p.kind == nativeTimestamp {
			return p.messagePackScalar(v), true
		}
		return p.messagePackValue(v, row2.Fast2), true
	case nativeRepresentation:
		if it.index >= l.native().Len() {
			return values.Value{}, false
		}
		v := nativeValue(l.plan.elem, l.native().Index(it.index), row2.Fast2)
		it.index++
		return v, true
	default:
		_, _, v, ok := it.wire.Next()
		if !ok {
			return values.Value{}, false
		}
		return v.Decode(l.encoding), true
	}
}

// Kept with the layout compiler to avoid per-read model inspection.
func listEncoding(t AnyType) (row2.Encoding, bool) {
	if l, ok := t.(*ListType); ok {
		return Encoding(l.ItemType()), true
	}
	return 0, false
}
