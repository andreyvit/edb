package kvo

import (
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
)

// NewJSONMapProp borrows the dictionary and decodes only the selected dynamic
// entry. Scalar/record maps have allocation-free access; dynamic object trees
// allocate when materialized as any. Nil is a present JSON null, not deletion.
func NewJSONMapProp[K comparable](schema *Schema, code uint64, name string, keyType AnyType, key values.Codec[K]) *MapPropRef[K, any] {
	c := newMapCodec(MapOf(keyType, TJSON), key, values.Codec[any]{Type: reflect.TypeFor[any](), Decode: func(v values.Value) any { var out any; DecodeJSON(v, &out); return out }, Encode: JSONValue})
	c.messagePack = func(_ *nativePlan, v msgpack.View) any {
		var out any
		if !v.Nil() {
			msgpack.Decode(v.Raw(), reflect.ValueOf(&out).Elem())
		}
		return out
	}
	c.native = func(_ *nativePlan, v reflect.Value) any { return v.Interface() }
	c.captureValue = func(v any) any {
		if v == nil {
			return nil
		}
		return (&nativeCloner{seen: make(map[cloneAddress]reflect.Value)}).value(reflect.ValueOf(v), 0).Interface()
	}
	c.encodeMessagePack = func(_ *nativePlan, v any) []byte { return msgpack.Encode(nil, reflect.ValueOf(&v).Elem()) }
	return c.Prop(schema, code, name)
}
