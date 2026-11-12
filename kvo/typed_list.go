package kvo

import (
	"bytes"
	"iter"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Cursor traverses a borrowed scalar collection once. Next does not repeat
// the record property lookup. Slice allocates a slice of the remaining values;
// strings and byte spans still borrow the source record's storage.
// A copied cursor has an independent traversal position.
type Cursor[T any] struct {
	raw       ListValueIterator
	codec     *values.Codec[T]
	remaining int
}

func (it Cursor[T]) Len() int { return it.remaining }
func (it *Cursor[T]) Next() (T, bool) {
	v, ok := it.raw.Next()
	if !ok {
		it.remaining = 0
		var zero T
		return zero, false
	}
	it.remaining--
	return it.codec.Decode(v), true
}
func (it Cursor[T]) Slice() []T {
	return it.slice(borrowElements)
}

// OwnedSlice materializes the remaining elements and detaches their string or
// byte storage. Like Slice, it does not advance the original cursor.
func (it Cursor[T]) OwnedSlice() []T { return it.slice(ownElements) }

func (it Cursor[T]) slice(mode collectionCopy) []T {
	if it.Len() == 0 {
		return nil
	}
	out := make([]T, 0, it.Len())
	for {
		v, ok := it.raw.Next()
		if !ok {
			return out
		}
		if mode == ownElements && v.P != nil {
			v = values.Bytes(bytes.Clone(v.AsBytes()))
		}
		out = append(out, it.codec.Decode(v))
	}
}

// Iter returns a single-pass sequence over a copy of the current position.
// Breaking a range stops immediately; invoking that sequence again resumes it.
func (it Cursor[T]) Iter() iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			v, ok := it.Next()
			if !ok || !yield(v) {
				return
			}
		}
	}
}

// List is a 16-byte borrowed collection view. Get resolves the property once;
// Cursor and Iter initialize independent, single-pass traversal positions.
// Keeping a List alive keeps its Go backing storage alive, but does not extend
// a database transaction's lifetime.
type List[T any] struct {
	_    [0]*T
	cell ListCell
}

func (l List[T]) Cursor() Cursor[T] {
	e := l.cell.representation()
	if e == nil {
		return Cursor[T]{}
	}
	raw := l.cell.expand(e)
	return Cursor[T]{raw: raw.Iter(), codec: e.codec.(*values.Codec[T]), remaining: raw.Len()}
}

// Iter returns a single-pass sequence. Reusing this sequence resumes its
// position; call Iter again to start a separate traversal.
func (l List[T]) Iter() iter.Seq[T] {
	var c Cursor[T]
	started := false
	return func(yield func(T) bool) {
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
func (l List[T]) Slice() []T { return l.Cursor().Slice() }

// OwnedSlice copies both the slice and any borrowed string/byte elements.
func (l List[T]) OwnedSlice() []T { return l.Cursor().OwnedSlice() }

// ListPropRef shares conversion and representation metadata across all values.
type ListPropRef[T any] struct {
	*PropRef[List[T]]
	access   *collectionAccess
	codec    values.Codec[T]
	encoding row2.Encoding
}

func BindListProp[T any](schema *Schema, code uint64, codec values.Codec[T]) *ListPropRef[T] {
	typ, ok := schema.MustPropByCode(code).AnyType().(*ListType)
	if !ok {
		panic("record: list property required")
	}
	checkCodec(typ.ItemType(), codec)
	p := &ListPropRef[T]{access: newCollectionAccess(&codec, nil, typ.ItemType()), codec: codec, encoding: Encoding(typ.ItemType())}
	p.PropRef = &PropRef[List[T]]{code: code, view: func(r Record, k uint64) List[T] {
		return List[T]{cell: p.access.wrap(r.ListValue(k))}
	}, encode: func(l List[T]) values.Value {
		if l.Missing() {
			return values.Value{}
		}
		w := row2.AcquireWriter(nil)
		defer w.Release()
		w.Begin(0, "", row2.List, row2.Compact2)
		it := l.Cursor()
		for {
			v, ok := it.Next()
			if !ok {
				break
			}
			w.Put(0, "", codec.Encode(v), p.encoding)
		}
		w.Finish(true)
		return values.Bytes(w.BytesWritten())
	}}
	return p
}
func NewListProp[T any](schema *Schema, code uint64, name string, typ AnyType, codec values.Codec[T]) *ListPropRef[T] {
	list, ok := typ.(*ListType)
	if !ok {
		panic("record: list property required")
	}
	checkCodec(list.ItemType(), codec)
	NewProp(schema, code, name, typ, nil)
	return BindListProp(schema, code, codec)
}

func (p *ListPropRef[T]) encodeSlice(items []T) values.Value {
	if items == nil {
		return values.Value{}
	}
	w := row2.AcquireWriter(nil)
	defer w.Release()
	w.Begin(0, "", row2.List, row2.Compact2)
	for _, v := range items {
		w.Put(0, "", p.codec.Encode(v), p.encoding)
	}
	w.Finish(true)
	return values.Bytes(w.BytesWritten())
}

// FromSlice copies items into an owned encoded collection for assignment.
// Reads through Get do not make this copy.
func (p *ListPropRef[T]) FromSlice(items []T) List[T] {
	return List[T]{cell: p.access.wrap(ListValue{data: p.encodeSlice(items), encoding: p.encoding, kind: compactRepresentation})}
}
func (p *ListPropRef[T]) SetSlice(e *Editor, items []T) {
	e.SetValue(p.Code(), p.encodeSlice(items))
}

func (l List[T]) Len() int            { return l.cell.Len() }
func (l List[T]) Missing() bool       { return l.cell.View().Missing() }
func (l List[T]) Value() values.Value { return l.cell.Value() }

func (p *ListPropRef[T]) readEditor(e *Editor) List[T] {
	return List[T]{cell: p.access.wrap(e.collectionValue(p.Code(), p.access.elem))}
}
