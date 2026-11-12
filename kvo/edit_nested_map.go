package kvo

import (
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
)

// EditMap opens an attached nested dictionary, returning nil for a missing key.
// The free function infers both maps' key/value types from the outer editor.
func EditMap[K comparable, K2 comparable, V any](outer *MapEditor[K, Map[K2, V]], key K) *MapEditor[K2, V] {
	outer.check()
	if c := outer.nested[key]; c != nil {
		return c.(*MapEditor[K2, V])
	}
	v, ok := outer.Lookup(key)
	if !ok {
		return nil
	}
	return attachMap(outer, key, v)
}
func attachMap[K comparable, K2 comparable, V any](outer *MapEditor[K, Map[K2, V]], key K, v Map[K2, V]) *MapEditor[K2, V] {
	l, c := v.parts()
	inner := &MapEditor[K2, V]{owner: outer.owner, codec: c, base: v, plan: l.plan}
	if l.kind == nativeRepresentation {
		inner.native = l.native()
	}
	if outer.nested == nil {
		outer.nested = make(map[K]collectionEdit)
	}
	outer.nested[key] = inner
	inner.onChange = func() {
		outer.check()
		if outer.native.IsValid() {
			k := reflect.ValueOf(key)
			if k.Type() != outer.native.Type().Key() {
				k = k.Convert(outer.native.Type().Key())
			}
			outer.native.SetMapIndex(k, inner.native)
		}
		outer.remember(key, mapChange[Map[K2, V]]{collection: inner, view: func() Map[K2, V] {
			return c.view(ListValue{data: inner.packed(), encoding: l.encoding, kind: compactRepresentation})
		}})
		outer.changed()
	}
	return inner
}

// NewMap explicitly creates a nested dictionary. Existing entries panic.
func NewMap[K comparable, K2 comparable, V any](outer *MapEditor[K, Map[K2, V]], key K) *MapEditor[K2, V] {
	outer.check()
	if _, ok := outer.Lookup(key); ok {
		panic("record: dictionary entry already exists")
	}
	key = ownCodecValue(outer.codec.key, key)
	v := outer.codec.value.Decode(values.Value{})
	l, c := v.parts()
	if outer.native.IsValid() || outer.plan != nil && outer.plan.elem.msgFallback {
		if outer.native.IsValid() && outer.native.IsNil() {
			outer.native.Set(reflect.MakeMap(outer.native.Type()))
		}
		p := outer.plan.elem
		x := reflect.New(p.typ).Elem()
		x.Set(reflect.MakeMap(p.typ))
		l = ListValue{data: values.Value{P: x.Addr().UnsafePointer()}, plan: p, encoding: l.encoding, kind: nativeRepresentation}
	} else if outer.plan != nil {
		l = ListValue{data: values.Bytes(msgpack.Trusted(emptyMessagePackObject).Raw()), plan: outer.plan.elem, encoding: l.encoding, kind: messagePackRepresentation}
	}
	inner := attachMap(outer, key, c.view(l))
	inner.changed()
	return inner
}
func EditOrCreateMap[K comparable, K2 comparable, V any](outer *MapEditor[K, Map[K2, V]], key K) *MapEditor[K2, V] {
	if c := EditMap(outer, key); c != nil {
		return c
	}
	return NewMap(outer, key)
}
