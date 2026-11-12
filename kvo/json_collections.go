package kvo

import (
	"reflect"
	"strconv"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// JSON traverses the original containers. Materializing row2 first would reject
// valid MessagePack/native nil entries and needlessly allocate encoded copies.
func jsonCollectionField(r recordView, key uint64, typ AnyType, o JSONOptions, depth int) (any, error) {
	if r.plan != nil {
		f := r.plan.field(key)
		if r.object != nil {
			return jsonNativeData(f.plan, reflect.NewAt(f.plan.typ, f.access.Address(r.object)).Elem(), typ, o, depth)
		}
		if !r.msg.Missing() {
			return jsonMessagePackData(f.plan, r.messagePackField(f), typ, o, depth)
		}
	}
	v := r.GetAs(key, Encoding(typ))
	return jsonPresentData(v, typ, o, depth, len(v.AsBytes()) != 0 || r.Has(key))
}

func jsonNativeData(p *nativePlan, v reflect.Value, typ AnyType, o JSONOptions, depth int) (any, error) {
	if err := jsonCheck(o, 32, depth); err != nil {
		return nil, err
	}
	p, v = unwrap(p, v, false)
	if !v.IsValid() || p.nilable() && v.IsNil() {
		return nil, nil
	}
	if m := typ.Model(); m != nil {
		if !v.CanAddr() {
			copy := reflect.New(v.Type()).Elem()
			copy.Set(v)
			v = copy
		}
		return jsonObject(nativeRecord(p, v.Addr()), m, o, depth)
	}
	switch typ := typ.(type) {
	case *ListType:
		if err := jsonCheck(o, uint64(v.Len())*16, depth); err != nil {
			return nil, err
		}
		out := make([]any, v.Len())
		for i := range v.Len() {
			x, err := jsonNativeData(p.elem, v.Index(i), typ.ItemType(), o, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = x
		}
		return out, nil
	case *MapType:
		if err := jsonCheck(o, uint64(v.Len())*128, depth); err != nil {
			return nil, err
		}
		out := make(map[string]any, v.Len())
		it := v.MapRange()
		for it.Next() {
			key := nativeValue(p.mapKey, it.Key(), row2.Fast2)
			if err := jsonCheck(o, row2.PayloadSize(key), depth); err != nil {
				return nil, err
			}
			name := jsonMapKey(key, typ.MapKeyType())
			x, err := jsonNativeData(p.elem, it.Value(), typ.ItemType(), o, depth+1)
			if err != nil {
				return nil, err
			}
			out[name] = x
		}
		return out, nil
	default:
		return jsonData(nativeValue(p, v, row2.Fast2), typ, o, depth)
	}
}

func jsonMessagePackData(p *nativePlan, raw msgpack.View, typ AnyType, o JSONOptions, depth int) (any, error) {
	if err := jsonCheck(o, 32, depth); err != nil {
		return nil, err
	}
	if m := typ.Model(); m != nil {
		r := messagePackChildValue(p, raw)
		if r.Missing() {
			return nil, nil
		}
		return jsonObject(r, m, o, depth)
	}
	if p.kind == nativePointer && raw.Nil() || p.kind == nativeOption && !raw.Lookup("Present").Bool() {
		return nil, nil
	}
	switch typ.(type) {
	case *ListType, *MapType:
	default:
		return jsonData(messagePackFieldValue(&nativeField{plan: p, layout: row2.Fast2}, raw), typ, o, depth)
	}
	if p.msgFallback || indirectPlan(p).msgFallback {
		if err := jsonCheck(o, uint64(len(raw.Raw()))*8, depth); err != nil {
			return nil, err
		}
		return jsonNativeData(p, decodeProperty(p, raw), typ, o, depth)
	}
	if p.kind == nativeOption {
		if !raw.Lookup("Present").Bool() {
			return nil, nil
		}
		p = p.elem
		raw = raw.Lookup("Value")
	}
	for p.kind == nativePointer {
		if raw.Nil() {
			return nil, nil
		}
		p = p.elem
	}
	if p.nilable() && raw.Nil() {
		return nil, nil
	}
	switch typ := typ.(type) {
	case *ListType:
		// Byte slices and arrays use MessagePack binary data, not an array header.
		if p.typ.Elem().Kind() == reflect.Uint8 {
			if err := jsonCheck(o, uint64(len(raw.Raw()))*8, depth); err != nil {
				return nil, err
			}
			return jsonNativeData(p, decodeProperty(p, raw), typ, o, depth)
		}
		it := raw.Iter()
		n := it.Len()
		if p.typ.Kind() == reflect.Array {
			n = p.typ.Len()
			if it.Len() > n {
				panic(row2.ErrValue)
			}
		}
		if err := jsonCheck(o, uint64(n)*16, depth); err != nil {
			return nil, err
		}
		out := make([]any, n)
		for i := range n {
			_, v, _ := it.Next()
			x, err := jsonMessagePackData(p.elem, v, typ.ItemType(), o, depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = x
		}
		return out, nil
	case *MapType:
		it := raw.Iter()
		if err := jsonCheck(o, uint64(it.Len())*128, depth); err != nil {
			return nil, err
		}
		out := make(map[string]any, it.Len())
		for {
			k, v, ok := it.Next()
			if !ok {
				break
			}
			key := p.mapKey.messagePackScalar(k)
			if err := jsonCheck(o, row2.PayloadSize(key), depth); err != nil {
				return nil, err
			}
			name := jsonMapKey(key, typ.MapKeyType())
			x, err := jsonMessagePackData(p.elem, v, typ.ItemType(), o, depth+1)
			if err != nil {
				return nil, err
			}
			out[name] = x
		}
		return out, nil
	}
	panic(row2.ErrKind)
}

func jsonMapKey(v values.Value, typ AnyType) string {
	if typ.ValueKind() == ValueKindScalarData {
		return string(v.AsBytes())
	}
	if scalar, ok := typ.(*WordType); ok {
		return scalar.FormatValue(nil, v.R)
	}
	return strconv.FormatUint(v.R, 10)
}
