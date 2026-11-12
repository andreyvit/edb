package kvo

import (
	"bytes"
	"iter"
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type mapChange[V any] struct {
	value      V
	deleted    bool
	child      *Editor
	raw        []byte
	collection collectionEdit
	view       func() V
}

// MapEditor buffers entry changes. Reads observe the buffer immediately; encoded
// dictionaries and their parent records are rebuilt only on snapshot/save.
type MapEditor[K comparable, V any] struct {
	owner             *Editor
	key               uint64
	codec             *MapCodec[K, V]
	base              Map[K, V]
	changes           map[K]mapChange[V]
	children          map[K]*Editor
	cleared           bool
	detached          bool
	cached            values.Value
	cachedMessagePack []byte
	native            reflect.Value
	plan              *nativePlan
	nested            map[K]collectionEdit
	onChange          func()
}

func (e *Editor) Map[K comparable, V any](p *MapPropRef[K, V]) *MapEditor[K, V] {
	e.checkAttached()
	if m := e.collections[p.Code()]; m != nil {
		return m.(*MapEditor[K, V])
	}
	l := e.collectionValue(p.Code(), p.codec.typ.ItemType())
	m := &MapEditor[K, V]{owner: e, key: p.Code(), codec: p.codec, base: p.codec.view(l), plan: l.plan}
	if l.kind == nativeRepresentation {
		m.native = l.native()
	}
	e.registerCollection(p.Code(), m)
	return m
}
func (m *MapEditor[K, V]) check() {
	m.owner.checkAttached()
	if m.detached {
		panic("record: dictionary editor was replaced or deleted")
	}
}
func (m *MapEditor[K, V]) detach() {
	m.detached = true
	for _, c := range m.children {
		c.detached = true
	}
	for _, c := range m.nested {
		c.detach()
	}
}
func (m *MapEditor[K, V]) changed() {
	m.cached = values.Value{}
	m.cachedMessagePack = nil
	if m.onChange != nil {
		m.onChange()
	} else {
		m.owner.collectionChanged(m.key, m)
	}
}
func (m *MapEditor[K, V]) Lookup(key K) (V, bool) {
	m.check()
	if c, ok := m.changes[key]; ok {
		if c.view != nil {
			return c.view(), true
		}
		if c.child != nil {
			return any(c.child.currentView().compact()).(V), true
		}
		return c.value, !c.deleted
	}
	if m.cleared {
		var zero V
		return zero, false
	}
	return m.base.Lookup(key)
}
func (m *MapEditor[K, V]) Get(key K) V { v, _ := m.Lookup(key); return v }
func (m *MapEditor[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		m.check()
		if !m.cleared {
			for k, v := range m.base.Iter() {
				if _, ok := m.changes[k]; ok {
					continue
				}
				if !yield(k, v) {
					return
				}
			}
		}
		for k, c := range m.changes {
			if c.deleted {
				continue
			}
			v := c.value
			if c.view != nil {
				v = c.view()
			}
			if c.child != nil {
				v = any(c.child.currentView().compact()).(V)
			}
			if !yield(k, v) {
				return
			}
		}
	}
}
func (m *MapEditor[K, V]) Len() int {
	n := 0
	for range m.Iter() {
		n++
	}
	return n
}

func ownCodecValue[T any](codec values.Codec[T], v T) T {
	x := codec.Encode(v)
	if x.P != nil {
		x = values.Bytes(bytes.Clone(x.AsBytes()))
	}
	return codec.Decode(x)
}
func (c *MapCodec[K, V]) capture(v V) V {
	if c.captureValue != nil {
		return c.captureValue(v)
	}
	if r, ok := any(v).(Record); ok {
		if r.Missing() || r.Model() != c.typ.ItemType().Model() {
			panic("record: nonmissing record of the dictionary model required")
		}
		return any(r.Clone()).(V)
	}
	return ownCodecValue(c.value, v)
}
func (m *MapEditor[K, V]) remember(key K, change mapChange[V]) {
	if m.changes == nil {
		m.changes = make(map[K]mapChange[V])
	}
	if c := m.children[key]; c != nil && c != change.child {
		c.detached = true
		delete(m.children, key)
	}
	if c := m.nested[key]; c != nil && c != change.collection {
		c.detach()
		delete(m.nested, key)
	}
	m.changes[key] = change
}

// Set preserves zero values as present dictionary entries.
func (m *MapEditor[K, V]) Set(key K, v V) {
	m.check()
	if typ := m.codec.typ.ItemType(); typ.ValueKind() == ValueKindWord || typ.ValueKind() == ValueKindScalarData {
		if old, ok := m.Lookup(key); ok && equalCodecValue(m.codec.value.Encode(old), m.codec.value.Encode(v)) {
			return
		}
	}
	key = ownCodecValue(m.codec.key, key)
	v = m.codec.capture(v)
	change := mapChange[V]{value: v}
	var native reflect.Value
	if m.native.IsValid() {
		if r, ok := any(v).(Record); ok {
			native = assignRecordValue(m.plan.elem, r)
		} else if reflect.TypeFor[V]() == m.plan.typ.Elem() {
			native = reflect.ValueOf(&v).Elem()
		} else {
			native = nativeCollectionAssignment(m.plan.elem, m.codec.value.Encode(v))
		}
	} else if m.plan != nil {
		change.raw = m.encodeMessagePackValue(v)
	}
	if m.native.IsValid() {
		if m.native.IsNil() {
			m.native.Set(reflect.MakeMap(m.native.Type()))
		}
		k := reflect.ValueOf(key)
		if k.Type() != m.native.Type().Key() {
			k = k.Convert(m.native.Type().Key())
		}
		m.native.SetMapIndex(k, native)
	}
	m.remember(key, change)
	m.changed()
}
func (m *MapEditor[K, V]) Delete(key K) bool {
	m.check()
	if _, found := m.Lookup(key); !found {
		return false
	}
	if m.native.IsValid() {
		k := reflect.ValueOf(key)
		if k.Type() != m.native.Type().Key() {
			k = k.Convert(m.native.Type().Key())
		}
		m.native.SetMapIndex(k, reflect.Value{})
	}
	m.remember(ownCodecValue(m.codec.key, key), mapChange[V]{deleted: true})
	m.changed()
	return true
}

// Clear leaves an empty dictionary. Editor.Delete removes the property itself.
func (m *MapEditor[K, V]) Clear() {
	m.check()
	for _, c := range m.children {
		c.detached = true
	}
	clear(m.children)
	for _, c := range m.nested {
		c.detach()
	}
	clear(m.nested)
	clear(m.changes)
	m.cleared = true
	if m.native.IsValid() {
		m.native.Set(reflect.MakeMap(m.native.Type()))
	}
	m.changed()
}
func (m *MapEditor[K, V]) packed() values.Value {
	if m.cached.P == nil {
		m.cached = m.codec.encodeEntries(m.Iter())
	}
	return m.cached
}
func (m *MapEditor[K, V]) encodeMessagePackValue(v V) []byte {
	if m.codec.encodeMessagePack != nil {
		return m.codec.encodeMessagePack(m.plan.elem, v)
	}
	if r, ok := any(v).(Record); ok {
		return encodeRecordMessagePack(m.plan.elem, r)
	}
	return encodeMessagePackLeaf(m.plan.elem, row2.Step{Options: row2.Present}, m.codec.value.Encode(v))
}
func (m *MapEditor[K, V]) messagePack() []byte {
	if m.cachedMessagePack != nil {
		return m.cachedMessagePack
	}
	if m.native.IsValid() {
		m.cachedMessagePack = msgpack.Encode(nil, m.native)
		return m.cachedMessagePack
	}
	l, _ := m.base.parts()
	m.cachedMessagePack = msgpack.BuildMap(int(l.data.R), func(yield func(msgpack.View, msgpack.View) bool) {
		if !m.cleared && l.data.P != nil {
			it := msgpack.Trusted(l.data.AsBytes()).Iter()
			for {
				key, v, ok := it.Next()
				if !ok {
					break
				}
				k := m.codec.key.Decode(m.plan.mapKey.messagePackScalar(key))
				if _, changed := m.changes[k]; !changed && !yield(key, v) {
					return
				}
			}
		}
		for k, c := range m.changes {
			if c.deleted {
				continue
			}
			raw := c.raw
			if c.collection != nil {
				raw = c.collection.messagePack()
			}
			if c.child != nil {
				raw = c.child.MessagePack()
				if raw == nil {
					raw = msgpack.Encode(nil, c.child.view.objectValue())
				}
			}
			key := encodeMessagePackLeaf(m.plan.mapKey, row2.Step{Options: row2.Present}, m.codec.key.Encode(k))
			if !yield(msgpack.Trusted(key), msgpack.Trusted(raw)) {
				return
			}
		}
	}).Raw()
	return m.cachedMessagePack
}

// SetNull stores an explicit null JSON entry. Lookup then returns (nil, true);
// Delete removes it. Non-JSON dictionaries reject null rather than silently zeroing.
func (m *MapEditor[K, V]) SetNull(key K) {
	if m.codec.typ.ItemType() != TJSON {
		panic("record: null requires a JSON dictionary")
	}
	var zero V
	m.Set(key, zero)
}

// Edit opens a record-valued entry. Missing/null entries return nil; New and
// EditOrCreate explicitly create entries. Scalar-valued maps reject this API.
func (m *MapEditor[K, V]) Edit(key K) *Editor {
	m.check()
	if c := m.children[key]; c != nil {
		return c
	}
	v, found := m.Lookup(key)
	if !found {
		return nil
	}
	r, ok := any(v).(Record)
	if !ok {
		panic("record: dictionary entry is not a record")
	}
	if r.Missing() {
		return nil
	}
	return m.attach(key, r.Edit())
}
func (m *MapEditor[K, V]) attach(key K, child *Editor) *Editor {
	if m.children == nil {
		m.children = make(map[K]*Editor)
	}
	m.children[key] = child
	child.parent = m.owner
	child.onChange = func() {
		m.check()
		if m.native.IsValid() {
			v := child.view.objectValue()
			if v.Type() != m.plan.typ.Elem() && v.CanAddr() && v.Addr().Type() == m.plan.typ.Elem() {
				v = v.Addr()
			}
			k := reflect.ValueOf(key)
			if k.Type() != m.native.Type().Key() {
				k = k.Convert(m.native.Type().Key())
			}
			m.native.SetMapIndex(k, v)
		}
		m.remember(key, mapChange[V]{child: child})
		m.changed()
	}
	return child
}
func (m *MapEditor[K, V]) New(key K) *Editor {
	m.check()
	if _, found := m.Lookup(key); found {
		panic("record: dictionary entry already exists")
	}
	model := m.codec.typ.ItemType().Model()
	if model == nil {
		panic("record: dictionary entry is not a record")
	}
	key = ownCodecValue(m.codec.key, key)
	var child *Editor
	if m.native.IsValid() {
		p := indirectPlan(m.plan.elem)
		child = nativeRecord(p, reflect.New(p.typ)).Edit()
		if m.native.IsNil() {
			m.native.Set(reflect.MakeMap(m.native.Type()))
		}
	} else if m.plan != nil {
		p := indirectPlan(m.plan.elem)
		if p.msgFallback {
			child = nativeRecord(p, reflect.New(p.typ)).Edit()
		} else {
			child = (&recordView{plan: p, msg: msgpack.Trusted(emptyMessagePackObject)}).Edit()
		}
	} else {
		child = Encoded(row2.Empty(row2.Fast2)).WithModel(model).Edit()
	}
	m.attach(key, child)
	child.touch()
	return child
}
func (m *MapEditor[K, V]) EditOrCreate(key K) *Editor {
	if c := m.Edit(key); c != nil {
		return c
	}
	return m.New(key)
}
