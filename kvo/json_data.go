package kvo

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// JSONOptions lets a host meter a detached JSON projection.
type JSONOptions struct {
	Context context.Context
	Charge  func(uint64) error
}

// ToJSON copies schema properties into JSON data, retaining native scalar codecs.
// The returned map and its children do not borrow the record's storage.
func (r recordView) ToJSON(model *Model, options JSONOptions) (map[string]any, error) {
	if r.Missing() {
		return nil, nil
	}
	if options.Context == nil {
		options.Context = context.Background()
	}
	return jsonObject(r, model, options, 0)
}

func jsonCheck(o JSONOptions, n uint64, depth int) error {
	if depth > 128 {
		return fmt.Errorf("JSON nesting exceeds 128")
	}
	if err := o.Context.Err(); err != nil {
		return err
	}
	if o.Charge != nil {
		return o.Charge(n)
	}
	return nil
}
func jsonObject(r recordView, model *Model, o JSONOptions, depth int) (map[string]any, error) {
	props := model.AllProps()
	if err := jsonCheck(o, 64+uint64(len(props))*128, depth); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(props))
	for _, prop := range props {
		var v any
		var err error
		if child := prop.AnyType().Model(); child != nil {
			row := r.Child(row2.Step{Key: prop.Code()})
			if !row.Missing() {
				v, err = jsonObject(row, child, o, depth+1)
			}
		} else {
			switch prop.AnyType().(type) {
			case *ListType, *MapType:
				v, err = jsonCollectionField(r, prop.Code(), prop.AnyType(), o, depth+1)
			default:
				v, err = jsonData(r.GetAs(prop.Code(), Encoding(prop.AnyType())), prop.AnyType(), o, depth+1)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", model.Name(), prop.Name(), err)
		}
		out[prop.Name()] = v
	}
	return out, nil
}
func jsonData(v values.Value, typ AnyType, o JSONOptions, depth int) (any, error) {
	return jsonPresentData(v, typ, o, depth, false)
}

// Empty encoded containers have no payload; their parent carries presence.
func jsonPresentData(v values.Value, typ AnyType, o JSONOptions, depth int, present bool) (any, error) {
	if err := jsonCheck(o, row2.PayloadSize(v)+32, depth); err != nil {
		return nil, err
	}
	if model := typ.Model(); model != nil {
		if row2.Trusted(v.AsBytes()).Missing() && !present {
			return nil, nil
		}
		return jsonObject(encodedView(row2.Trusted(v.AsBytes())).WithModel(model), model, o, depth+1)
	}
	switch typ := typ.(type) {
	case *JSONType:
		if err := jsonCheck(o, uint64(len(v.AsBytes()))*8, depth); err != nil {
			return nil, err
		}
		var out any
		DecodeJSON(v, &out)
		return out, nil
	case *WordType:
		t := typ.NativeType()
		if t == reflect.TypeFor[time.Time]() {
			return Uint64ToTime(v.AsUint()), nil
		}
		out := reflect.New(t).Elem()
		switch t.Kind() {
		case reflect.Bool:
			out.SetBool(v.AsUint() != 0)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			out.SetInt(v.AsInt())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			out.SetUint(v.AsUint())
		case reflect.Float32:
			out.SetFloat(float64(v.AsFloat32()))
		case reflect.Float64:
			out.SetFloat(v.AsFloat())
		default:
			return nil, fmt.Errorf("unsupported scalar %s", typ.Name())
		}
		return out.Interface(), nil
	case *ScalarDataType:
		if typ == TTimestamp {
			return v.AsTime(), nil
		}
		if typ.NativeType().Kind() == reflect.String {
			return string(v.AsBytes()), nil
		}
		return append([]byte(nil), v.AsBytes()...), nil
	case *ListType:
		view := row2.Trusted(v.AsBytes())
		if view.Missing() && !present {
			return nil, nil
		}
		if err := jsonCheck(o, uint64(view.Len())*16, depth); err != nil {
			return nil, err
		}
		out := make([]any, 0, view.Len())
		it := view.Iter()
		for {
			_, _, cell, ok := it.Next()
			if !ok {
				break
			}
			item, err := jsonPresentData(cell.Decode(Encoding(typ.ItemType())), typ.ItemType(), o, depth+1, true)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		return out, nil
	case *MapType:
		view := row2.Trusted(v.AsBytes())
		if view.Missing() && !present {
			return nil, nil
		}
		if err := jsonCheck(o, uint64(view.Len())*128, depth); err != nil {
			return nil, err
		}
		out := make(map[string]any, view.Len())
		it := view.Iter()
		for {
			key, text, cell, ok := it.Next()
			if !ok {
				break
			}
			name := string(text)
			if view.Kind() != row2.Dictionary {
				name = jsonMapKey(values.Uint(key), typ.MapKeyType())
			}
			item, err := jsonPresentData(cell.Decode(Encoding(typ.ItemType())), typ.ItemType(), o, depth+1, true)
			if err != nil {
				return nil, err
			}
			out[name] = item
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported JSON type %s", typ.Name())
}
