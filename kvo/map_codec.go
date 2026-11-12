package kvo

import (
	"reflect"
	"slices"
	"strings"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// MapCodec is reusable metadata for scalar, record, or nested-map values.
// Construct it during schema setup; views retain only their 16-byte cell.
type MapCodec[K comparable, V any] struct {
	typ               *MapType
	key               values.Codec[K]
	value             values.Codec[V]
	text              bool
	access            *collectionAccess
	messagePack       func(*nativePlan, msgpack.View) V
	native            func(*nativePlan, reflect.Value) V
	captureValue      func(V) V
	encodeMessagePack func(*nativePlan, V) []byte
}

type MapPropRef[K comparable, V any] struct {
	*PropRef[Map[K, V]]
	codec *MapCodec[K, V]
}

func (c *MapCodec[K, V]) Type() *MapType           { return c.typ }
func (p *MapPropRef[K, V]) Codec() *MapCodec[K, V] { return p.codec }

func NewMapCodec[K comparable, V any](typ *MapType, key values.Codec[K], value values.Codec[V]) *MapCodec[K, V] {
	checkCodec(typ.ItemType(), value)
	return newMapCodec(typ, key, value)
}
func newMapCodec[K comparable, V any](typ *MapType, key values.Codec[K], value values.Codec[V]) *MapCodec[K, V] {
	checkCodec(typ.MapKeyType(), key)
	c := &MapCodec[K, V]{typ: typ, key: key, value: value, text: typ.MapKeyType().ValueKind() == ValueKindScalarData}
	c.access = newCollectionAccess(c, nil, typ.ItemType())
	return c
}
func (c *MapCodec[K, V]) fromMessagePack(p *nativePlan, v msgpack.View) V {
	if c.messagePack != nil {
		return c.messagePack(p, v)
	}
	return c.value.Decode(mapMessagePackValue(p, v))
}
func (c *MapCodec[K, V]) fromNative(p *nativePlan, v reflect.Value) V {
	if c.native != nil {
		return c.native(p, v)
	}
	return c.value.Decode(nativeValue(p, v, row2.Fast2))
}
func (c *MapCodec[K, V]) view(l ListValue) Map[K, V] { return Map[K, V]{c.access.wrap(l)} }

// Prop declares a typed map. Explicit flags replace the default zero-entry
// retention policy, allowing existing schemas to keep their storage contract.
func (c *MapCodec[K, V]) Prop(schema *Schema, code uint64, name string, flags ...PropFlag) *MapPropRef[K, V] {
	defaults := KeepDictionaryZeroValues
	if len(flags) != 0 {
		defaults = 0
		for _, f := range flags {
			defaults |= f
		}
	}
	NewProp(schema, code, name, c.typ, func(b *PropBuilder) { b.Flags = defaults })
	p := &MapPropRef[K, V]{codec: c}
	p.PropRef = &PropRef[Map[K, V]]{code: code, view: func(r Record, k uint64) Map[K, V] { return c.view(mapValue(r, k, c.typ)) }, encode: c.encode}
	return p
}
func (c *MapCodec[K, V]) encode(m Map[K, V]) values.Value {
	if m.Missing() {
		return values.Value{}
	}
	return c.encodeEntries(m.Iter())
}
func (c *MapCodec[K, V]) encodeEntries(entries func(func(K, V) bool)) values.Value {
	type entry struct{ k, v values.Value }
	var all []entry
	entries(func(k K, v V) bool { all = append(all, entry{c.key.Encode(k), c.value.Encode(v)}); return true })
	slices.SortFunc(all, func(a, b entry) int {
		if c.text {
			return strings.Compare(a.k.AsString(), b.k.AsString())
		}
		if a.k.R < b.k.R {
			return -1
		}
		if a.k.R > b.k.R {
			return 1
		}
		return 0
	})
	w := row2.AcquireWriter(nil)
	defer w.Release()
	kind := row2.Object
	if c.text {
		kind = row2.Dictionary
	}
	w.Begin(0, "", kind, row2.Fast2)
	for _, e := range all {
		var text string
		if c.text {
			text = e.k.AsString()
		}
		w.PutPresent(e.k.R, text, e.v, Encoding(c.typ.ItemType()))
	}
	w.Finish(true)
	return values.Bytes(w.BytesWritten())
}

// FromMap creates an owned encoded dictionary for whole-property assignment.
// Ordinary borrowed reads and per-entry edits do not materialize this map.
func (p *MapPropRef[K, V]) FromMap(m map[K]V) Map[K, V] {
	if m == nil {
		return Map[K, V]{}
	}
	v := p.codec.encodeEntries(func(yield func(K, V) bool) {
		for k, v := range m {
			if !yield(k, v) {
				return
			}
		}
	})
	return p.codec.view(ListValue{data: v, encoding: Encoding(p.codec.typ.ItemType()), kind: compactRepresentation})
}

func NewRecordMapCodec[K comparable](typ *MapType, key values.Codec[K]) *MapCodec[K, Record] {
	entity, ok := typ.ItemType().(*EntityType)
	if !ok {
		panic("record: record map requires an entity item type")
	}
	value := values.Codec[Record]{Type: reflect.TypeFor[Record](), Decode: func(v values.Value) Record { return Encoded(row2.Trusted(v.AsBytes())).WithModel(entity.Model()) }, Encode: func(r Record) values.Value {
		if !r.Missing() && r.Model() != entity.Model() {
			panic("record: incorrect map entry model")
		}
		return values.Bytes(r.Snapshot().Raw())
	}}
	c := newMapCodec(typ, key, value)
	c.native = func(p *nativePlan, v reflect.Value) Record {
		p, v = unwrap(p, v, false)
		if !v.IsValid() {
			return Record{}
		}
		if !v.CanAddr() {
			copy := reflect.New(v.Type()).Elem()
			copy.Set(v)
			v = copy
		}
		return planRecord(p, v.Addr())
	}
	c.messagePack = func(p *nativePlan, v msgpack.View) Record {
		if v.Nil() {
			return Record{}
		}
		if p.msgFallback || indirectPlan(p).msgFallback || indirectPlan(p).hasMessagePackInlineAlias(v) {
			return c.fromNative(p, decodeProperty(p, v))
		}
		return planMessagePack(indirectPlan(p), v.Raw())
	}
	return c
}
func NewRecordMapProp[K comparable](schema *Schema, code uint64, name string, typ *MapType, key values.Codec[K]) *MapPropRef[K, Record] {
	return NewRecordMapCodec(typ, key).Prop(schema, code, name)
}

// NewNestedMapCodec builds an outer map whose values are borrowed typed maps.
func NewNestedMapCodec[K comparable, K2 comparable, V any](keyType AnyType, key values.Codec[K], inner *MapCodec[K2, V]) *MapCodec[K, Map[K2, V]] {
	typ := MapOf(keyType, inner.typ)
	value := values.Codec[Map[K2, V]]{Type: reflect.TypeFor[Map[K2, V]](), Decode: func(v values.Value) Map[K2, V] {
		return inner.view(ListValue{data: v, encoding: Encoding(inner.typ.ItemType()), kind: compactRepresentation})
	}, Encode: inner.encode}
	c := newMapCodec(typ, key, value)
	c.messagePack = func(p *nativePlan, v msgpack.View) Map[K2, V] {
		return inner.view(messagePackCollection(p, v, Encoding(inner.typ.ItemType())))
	}
	c.native = func(p *nativePlan, v reflect.Value) Map[K2, V] {
		if !v.CanAddr() {
			copy := reflect.New(v.Type()).Elem()
			copy.Set(v)
			v = copy
		}
		return inner.view(ListValue{data: values.Value{P: v.Addr().UnsafePointer()}, plan: p, encoding: Encoding(inner.typ.ItemType()), kind: nativeRepresentation})
	}
	return c
}

func (p *MapPropRef[K, V]) readEditor(e *Editor) Map[K, V] {
	return p.codec.view(e.collectionValue(p.Code(), p.codec.typ.ItemType()))
}
