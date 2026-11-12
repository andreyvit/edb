package kvo

import (
	"iter"
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Map is a 16-byte borrowed view. Lookup and iteration read the stored encoding;
// no Go map or intermediate encoding is constructed. Retain the owning Record
// with Own before retaining this view across a transaction boundary.
type Map[K comparable, V any] struct{ cell ListCell }

func (m Map[K, V]) Missing() bool { l, _ := m.parts(); return l.Missing() }

func (m Map[K, V]) parts() (ListValue, *MapCodec[K, V]) {
	e := m.cell.representation()
	if e == nil {
		return ListValue{}, nil
	}
	return m.cell.expand(e), e.codec.(*MapCodec[K, V])
}
func (m Map[K, V]) Len() int {
	l, _ := m.parts()
	if l.data.P == nil {
		return 0
	}
	switch l.kind {
	case nativeRepresentation:
		return l.native().Len()
	case messagePackRepresentation, ownedMessagePackRepresentation:
		// Map iteration applies custom key decoders and duplicate-key semantics.
		n := 0
		for range m.Keys() {
			n++
		}
		return n
	default:
		return row2.Trusted(l.data.AsBytes()).Len()
	}
}
func (m Map[K, V]) Get(key K) V { v, _ := m.Lookup(key); return v }
func (m Map[K, V]) Lookup(key K) (out V, found bool) {
	l, c := m.parts()
	if l.data.P == nil {
		return
	}
	switch l.kind {
	case nativeRepresentation:
		if l.plan.typ == reflect.TypeFor[map[K]V]() {
			out, found = (*(*map[K]V)(l.data.P))[key]
			return
		}
		k := reflect.ValueOf(key)
		if k.Type() != l.plan.typ.Key() {
			k = k.Convert(l.plan.typ.Key())
		}
		v := l.native().MapIndex(k)
		if !v.IsValid() {
			return
		}
		return c.fromNative(l.plan.elem, v), true
	case messagePackRepresentation, ownedMessagePackRepresentation:
		it := msgpack.Trusted(l.data.AsBytes()).Iter()
		var raw msgpack.View
		for {
			k, v, ok := it.Next()
			if !ok {
				break
			}
			if c.key.Decode(l.plan.mapKey.messagePackScalar(k)) == key {
				raw = v
				found = true
			}
		}
		if found {
			out = c.fromMessagePack(l.plan.elem, raw)
		}
		return
	default:
		v := row2.Trusted(l.data.AsBytes())
		k := c.key.Encode(key)
		var f row2.Field
		if c.text {
			f = v.RawString(k.AsString())
			found = v.HasString(k.AsString())
		} else {
			f = v.RawField(k.R)
			found = v.Has(k.R)
		}
		if found {
			out = c.value.Decode(f.Decode(l.encoding))
		}
		return
	}
}
func mapMessagePackValue(p *nativePlan, v msgpack.View) values.Value {
	if p.msgFallback || p.kind == nativeJSON {
		return propertyValue(p, v, row2.Fast2)
	}
	return p.messagePackScalar(v)
}
func (m Map[K, V]) Iter() iter.Seq2[K, V] {
	return m.traverse(mapKeysAndValues)
}

// Keys visits keys without decoding values, including dynamic JSON values.
func (m Map[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) { m.traverse(mapKeysOnly)(func(k K, _ V) bool { return yield(k) }) }
}

type mapTraversal uint8

const (
	mapKeysOnly mapTraversal = iota
	mapKeysAndValues
)

func (m mapTraversal) String() string {
	switch m {
	case mapKeysOnly:
		return "keys"
	case mapKeysAndValues:
		return "entries"
	default:
		return "invalid"
	}
}

func (m Map[K, V]) traverse(mode mapTraversal) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		l, c := m.parts()
		if l.data.P == nil {
			return
		}
		switch l.kind {
		case nativeRepresentation:
			if l.plan.typ == reflect.TypeFor[map[K]V]() {
				for k, v := range *(*map[K]V)(l.data.P) {
					if !yield(k, v) {
						return
					}
				}
				return
			}
			it := l.native().MapRange()
			for it.Next() {
				k := c.key.Decode(nativeValue(l.plan.mapKey, it.Key(), row2.Fast2))
				var v V
				if mode == mapKeysAndValues {
					v = c.fromNative(l.plan.elem, it.Value())
				}
				if !yield(k, v) {
					return
				}
			}
		case messagePackRepresentation, ownedMessagePackRepresentation:
			it := msgpack.Trusted(l.data.AsBytes()).Iter()
			// Cache small maps on the stack: duplicate detection compares normalized
			// keys without repeatedly scanning/skipping every encoded value.
			if it.Len() <= 64 {
				var keys [64]K
				var entries [64]msgpack.View
				n := it.Len()
				for i := range n {
					k, v, _ := it.Next()
					keys[i] = c.key.Decode(l.plan.mapKey.messagePackScalar(k))
					entries[i] = v
				}
				for i, k := range keys[:n] {
					duplicate := false
					for _, later := range keys[i+1 : n] {
						if later == k {
							duplicate = true
							break
						}
					}
					if duplicate {
						continue
					}
					var out V
					if mode == mapKeysAndValues {
						out = c.fromMessagePack(l.plan.elem, entries[i])
					}
					if !yield(k, out) {
						return
					}
				}
				return
			}
			for {
				raw, v, ok := it.Next()
				if !ok {
					return
				}
				k := c.key.Decode(l.plan.mapKey.messagePackScalar(raw))
				// Last normalized key wins, as in decoding into a Go map. Lookahead stays
				// on the stack, including historical keys with a different wire encoding.
				rest := it
				duplicate := false
				for {
					later, _, ok := rest.Next()
					if !ok {
						break
					}
					if c.key.Decode(l.plan.mapKey.messagePackScalar(later)) == k {
						duplicate = true
						break
					}
				}
				if !duplicate {
					var out V
					if mode == mapKeysAndValues {
						out = c.fromMessagePack(l.plan.elem, v)
					}
					if !yield(k, out) {
						return
					}
				}
			}
		default:
			it := row2.Trusted(l.data.AsBytes()).Iter()
			for {
				k, s, v, ok := it.Next()
				if !ok {
					return
				}
				key := values.Uint(k)
				if c.text {
					key = values.Bytes(s)
				}
				var out V
				if mode == mapKeysAndValues {
					out = c.value.Decode(v.Decode(l.encoding))
				}
				if !yield(c.key.Decode(key), out) {
					return
				}
			}
		}
	}
}

func mapValue(r Record, key uint64, typ *MapType) ListValue {
	if r.Missing() {
		return ListValue{}
	}
	e := r.representation()
	enc := Encoding(typ.ItemType())
	if e.plan != nil {
		f := e.plan.field(key)
		p := f.plan
		switch e.kind {
		case nativeRepresentation:
			return ListValue{data: f.access.Get(r.value.P), plan: p, encoding: enc, kind: e.kind}
		case messagePackRepresentation, ownedMessagePackRepresentation:
			if !p.msgFallback {
				return ListValue{data: values.Bytes(r.expanded().messagePackField(f).Raw()), plan: p, encoding: enc, kind: e.kind}
			}
		}
	}
	return ListValue{data: r.GetValue(key), encoding: enc, kind: compactRepresentation}
}

// NewMapProp declares a typed scalar dictionary. Keys may be strings or integers,
// including domain enums; custom MessagePack key codecs retain their semantics.
func NewMapProp[K comparable, V any](schema *Schema, code uint64, name string, typ *MapType, key values.Codec[K], value values.Codec[V]) *MapPropRef[K, V] {
	return NewMapCodec(typ, key, value).Prop(schema, code, name)
}
