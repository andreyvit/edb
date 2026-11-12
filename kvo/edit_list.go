package kvo

import (
	"bytes"
	"iter"
	"reflect"
	"slices"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// ListEditor materializes the selected list on its first mutation, then batches
// subsequent changes. It never decodes the enclosing record.
type ListEditor[T any] struct {
	owner             *Editor
	prop              *ListPropRef[T]
	base              List[T]
	items             []T
	loaded            bool
	detached          bool
	plan              *nativePlan
	native            reflect.Value
	cached            values.Value
	cachedMessagePack []byte
}

// ListProperty selects a scalar or record list editor from its property type.
type ListProperty[E any] interface{ editList(*Editor) E }

func (e *Editor) List[E any, P ListProperty[E]](p P) E { return p.editList(e) }
func (p *ListPropRef[T]) editList(e *Editor) *ListEditor[T] {
	e.checkAttached()
	if l := e.collections[p.Code()]; l != nil {
		return l.(*ListEditor[T])
	}
	v := e.collectionValue(p.Code(), p.access.elem)
	l := &ListEditor[T]{owner: e, prop: p, base: List[T]{cell: p.access.wrap(v)}, plan: v.plan}
	if v.kind == nativeRepresentation {
		l.native = v.native()
	}
	e.registerCollection(p.Code(), l)
	return l
}
func (l *ListEditor[T]) check() {
	l.owner.checkAttached()
	if l.detached {
		panic("record: list editor was replaced or deleted")
	}
}
func (l *ListEditor[T]) detach() { l.detached = true }
func (l *ListEditor[T]) load() {
	l.check()
	if !l.loaded {
		l.items = l.base.Slice()
		l.loaded = true
	}
}
func (l *ListEditor[T]) changed() {
	l.cached = values.Value{}
	l.cachedMessagePack = nil
	if l.native.IsValid() {
		if l.native.Type() == reflect.TypeFor[[]T]() {
			l.native.Set(reflect.ValueOf(l.items))
		} else {
			l.native.Set(nativeCollectionAssignment(l.plan, l.packed()))
		}
	}
	l.owner.collectionChanged(l.prop.Code(), l)
}
func (l *ListEditor[T]) Len() int {
	l.check()
	if l.loaded {
		return len(l.items)
	}
	return l.base.Len()
}
func (l *ListEditor[T]) Get(index int) T {
	l.check()
	if l.loaded {
		return l.items[index]
	}
	return l.prop.codec.Decode(l.base.cell.At(index))
}
func (l *ListEditor[T]) Iter() iter.Seq[T] {
	return func(yield func(T) bool) {
		l.check()
		if l.loaded {
			for _, v := range l.items {
				if !yield(v) {
					return
				}
			}
		} else {
			l.base.Iter()(yield)
		}
	}
}
func (l *ListEditor[T]) Slice() []T {
	l.check()
	if l.loaded {
		return slices.Clone(l.items)
	}
	return l.base.Slice()
}
func (l *ListEditor[T]) Append(items ...T) {
	if len(items) == 0 {
		l.check()
		return
	}
	l.load()
	for _, v := range items {
		l.items = append(l.items, ownCodecValue(l.prop.codec, v))
	}
	l.changed()
}
func equalCodecValue(a, b values.Value) bool {
	return a.R == b.R && (a.P == b.P || a.P != nil && b.P != nil && bytes.Equal(a.AsBytes(), b.AsBytes()))
}
func (l *ListEditor[T]) Contains(v T) bool {
	x := l.prop.codec.Encode(v)
	for item := range l.Iter() {
		if equalCodecValue(x, l.prop.codec.Encode(item)) {
			return true
		}
	}
	return false
}
func (l *ListEditor[T]) AddUnique(v T) bool {
	if l.Contains(v) {
		return false
	}
	l.Append(v)
	return true
}
func (l *ListEditor[T]) Set(index int, v T) {
	l.load()
	if equalCodecValue(l.prop.codec.Encode(l.items[index]), l.prop.codec.Encode(v)) {
		return
	}
	l.items[index] = ownCodecValue(l.prop.codec, v)
	l.changed()
}
func (l *ListEditor[T]) Remove(v T) bool {
	if !l.Contains(v) {
		return false
	}
	l.load()
	x := l.prop.codec.Encode(v)
	l.items = slices.DeleteFunc(l.items, func(item T) bool { return equalCodecValue(x, l.prop.codec.Encode(item)) })
	l.changed()
	return true
}
func (l *ListEditor[T]) RemoveAt(index int) {
	l.load()
	l.items = slices.Delete(l.items, index, index+1)
	l.changed()
}
func (l *ListEditor[T]) Clear() { l.load(); l.items = make([]T, 0); l.changed() }
func (l *ListEditor[T]) packed() values.Value {
	if l.cached.P == nil {
		if l.loaded {
			l.cached = l.prop.encodeSlice(l.items)
		} else {
			l.cached = l.prop.toValue(l.base)
		}
	}
	return l.cached
}
func (l *ListEditor[T]) messagePack() []byte {
	if l.cachedMessagePack == nil {
		if l.native.IsValid() {
			l.cachedMessagePack = msgpack.Encode(nil, l.native)
		} else if l.plan.typ.Elem().Kind() == reflect.Uint8 || l.plan.msgFallback {
			l.cachedMessagePack = encodeMessagePackLeaf(l.plan, row2.Step{Options: row2.Present}, l.packed())
		} else {
			l.cachedMessagePack = msgpack.BuildArray(l.Len()*8, func(yield func(msgpack.View) bool) {
				for v := range l.Iter() {
					raw := encodeMessagePackLeaf(l.plan.elem, row2.Step{Options: row2.Present}, l.prop.codec.Encode(v))
					if !yield(msgpack.Trusted(raw)) {
						return
					}
				}
			}).Raw()
		}
	}
	return l.cachedMessagePack
}
