package edb

import (
	"reflect"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// indexPrimaryKey uses the shared native-value ABI. Scalars are inline; aggregate
// keys borrow Go storage. The normal write path already has an addressable key.
func indexPrimaryKey(key reflect.Value) values.Value {
	switch key.Kind() {
	case reflect.Bool:
		return values.Bool(key.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return values.Int(key.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return values.Uint(key.Uint())
	case reflect.Float32:
		return values.Float32(float32(key.Float()))
	case reflect.Float64:
		return values.Float(key.Float())
	case reflect.String:
		return values.String(key.String())
	case reflect.Pointer:
		return values.Value{P: key.UnsafePointer()}
	default:
		if !key.CanAddr() {
			copy := reflect.New(key.Type()).Elem()
			copy.Set(key)
			key = copy
		}
		return values.Value{R: values.NativeAddress, P: key.Addr().UnsafePointer()}
	}
}

// IndexPrimaryKey decodes a callback's primary-key cell using the table's exact
// Go key type. Scalars also support AsUint, AsString, etc. Aggregate keys use
// native Go storage, not VM cell arrays. Returned strings and slices are borrowed;
// neither they nor the original cell may be retained or mutated by the callback.
func IndexPrimaryKey[Key any](v values.Value) Key {
	typ := reflect.TypeFor[Key]()
	switch typ.Kind() {
	case reflect.Struct, reflect.Array, reflect.Slice:
		if v.R != values.NativeAddress || v.P == nil {
			panic("invalid native index primary key")
		}
		return *(*Key)(v.P)
	default:
		return indexScalarPrimaryKey[Key](v)
	}
}

func indexScalarPrimaryKey[Key any](v values.Value) Key {
	var key Key
	p := unsafe.Pointer(&key)
	switch reflect.TypeFor[Key]().Kind() {
	case reflect.Bool:
		*(*bool)(p) = v.AsBool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		switch unsafe.Sizeof(key) {
		case 1:
			*(*uint8)(p) = uint8(v.R)
		case 2:
			*(*uint16)(p) = uint16(v.R)
		case 4:
			*(*uint32)(p) = uint32(v.R)
		case 8:
			*(*uint64)(p) = v.R
		}
	case reflect.Float32:
		*(*float32)(p) = v.AsFloat32()
	case reflect.Float64:
		*(*float64)(p) = v.AsFloat()
	case reflect.String:
		*(*string)(p) = v.AsString()
	case reflect.Pointer:
		*(*unsafe.Pointer)(p) = v.P
	default:
		panic("unsupported index primary key type")
	}
	return key
}
