package kvo

import (
	"iter"
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// RecordCursor traverses object elements without requiring their native Go
// struct type or decoding them. Each element carries its own model metadata.
type RecordCursor struct {
	list      ListValue
	model     *Model
	msg       msgpack.Iterator
	wire      row2.Iterator
	index     int
	remaining int
}

func (c RecordCursor) Len() int { return c.remaining }
func (c *RecordCursor) Next() (Record, bool) {
	r, ok := c.next()
	if ok {
		c.remaining--
	} else {
		c.remaining = 0
	}
	return r, ok
}
func (c *RecordCursor) next() (Record, bool) {
	l := c.list
	switch l.kind {
	case messagePackRepresentation, ownedMessagePackRepresentation:
		_, v, ok := c.msg.Next()
		if !ok {
			return Record{}, false
		}
		if v.Nil() {
			return Record{}, true
		}
		return planMessagePack(indirectPlan(l.plan.elem), v.Raw()), true
	case nativeRepresentation:
		v := l.native()
		if c.index >= v.Len() {
			return Record{}, false
		}
		item := v.Index(c.index)
		c.index++
		for item.Kind() == reflect.Pointer {
			if item.IsNil() {
				return Record{}, true
			}
			item = item.Elem()
		}
		return planRecord(indirectPlan(l.plan.elem), item.Addr()), true
	default:
		_, _, v, ok := c.wire.Next()
		if !ok {
			return Record{}, false
		}
		return Encoded(v.Container()).WithModel(c.model), true
	}
}
func (c RecordCursor) Slice() []Record {
	return c.slice(borrowElements)
}

// OwnedSlice retains each element with Own; native mutable objects may still
// be shared. Use CloneSlice for independent records, including native objects.
func (c RecordCursor) OwnedSlice() []Record { return c.slice(ownElements) }
func (c RecordCursor) CloneSlice() []Record { return c.slice(cloneElements) }

func (c RecordCursor) slice(mode collectionCopy) []Record {
	if c.Len() == 0 {
		return nil
	}
	result := make([]Record, 0, c.Len())
	for {
		r, ok := c.Next()
		if !ok {
			return result
		}
		switch mode {
		case ownElements:
			r = r.Own()
		case cloneElements:
			r = r.Clone()
		}
		result = append(result, r)
	}
}

// Iter returns a single-pass sequence over a copy of the current position.
func (c RecordCursor) Iter() iter.Seq[Record] {
	return func(yield func(Record) bool) {
		for {
			r, ok := c.Next()
			if !ok || !yield(r) {
				return
			}
		}
	}
}

// RecordList is a 16-byte borrowed view of object elements. Each element
// carries its KVO model; no native Go struct type parameter is needed.
type RecordList struct{ cell ListCell }

func (l RecordList) Cursor() RecordCursor {
	e := l.cell.representation()
	if e == nil {
		return RecordCursor{}
	}
	raw := l.cell.expand(e)
	c := RecordCursor{list: raw, model: e.model, remaining: raw.Len()}
	if raw.data.P == nil {
		return c
	}
	switch raw.kind {
	case messagePackRepresentation, ownedMessagePackRepresentation:
		c.msg = msgpack.Trusted(raw.data.AsBytes()).Iter()
	case nativeRepresentation:
	default:
		c.wire = row2.Trusted(raw.data.AsBytes()).Iter()
	}
	return c
}

// Iter returns a single-pass sequence. Reusing this sequence resumes its
// position; call Iter again to start a separate traversal.
func (l RecordList) Iter() iter.Seq[Record] {
	var c RecordCursor
	started := false
	return func(yield func(Record) bool) {
		if !started {
			c = l.Cursor()
			started = true
		}
		for {
			v, ok := c.Next()
			if !ok || !yield(v) {
				return
			}
		}
	}
}
func (l RecordList) Slice() []Record      { return l.Cursor().Slice() }
func (l RecordList) OwnedSlice() []Record { return l.Cursor().OwnedSlice() }
func (l RecordList) CloneSlice() []Record { return l.Cursor().CloneSlice() }

type RecordListPropRef struct {
	*PropRef[RecordList]
	access *collectionAccess
	entity *EntityType
}

func (p *RecordListPropRef) readEditor(e *Editor) RecordList {
	return RecordList{p.access.wrap(e.collectionValue(p.Code(), p.entity))}
}
func BindRecordListProp(schema *Schema, code uint64) *RecordListPropRef {
	typ, ok := schema.MustPropByCode(code).AnyType().(*ListType)
	if !ok {
		panic("record: list property required")
	}
	entity, ok := typ.ItemType().(*EntityType)
	if !ok {
		panic("record: object list required")
	}
	access := newCollectionAccess(nil, entity, typ.ItemType())
	p := &RecordListPropRef{access: access, entity: entity}
	p.PropRef = &PropRef[RecordList]{code: code, view: func(r Record, k uint64) RecordList {
		return RecordList{access.wrap(r.ListValue(k))}
	}, encode: func(l RecordList) values.Value {
		if l.Missing() {
			return values.Value{}
		}
		w := row2.AcquireWriter(nil)
		defer w.Release()
		w.Begin(0, "", row2.List, row2.Compact2)
		c := l.Cursor()
		for {
			r, ok := c.Next()
			if !ok {
				break
			}
			if !r.Missing() && r.Model() != entity.Model() {
				panic("record: wrong object list model")
			}
			w.Put(0, "", values.Bytes(r.Snapshot().Raw()), row2.Data)
		}
		w.Finish(true)
		return values.Bytes(w.BytesWritten())
	}}
	return p
}
func NewRecordListProp(schema *Schema, code uint64, name string, typ AnyType) *RecordListPropRef {
	list, ok := typ.(*ListType)
	if !ok {
		panic("record: list property required")
	}
	if _, ok := list.ItemType().(*EntityType); !ok {
		panic("record: object list required")
	}
	NewProp(schema, code, name, typ, nil)
	return BindRecordListProp(schema, code)
}

func (l RecordList) Len() int            { return l.cell.Len() }
func (l RecordList) Missing() bool       { return l.cell.View().Missing() }
func (l RecordList) Value() values.Value { return l.cell.Value() }
