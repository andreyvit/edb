package nativebind

import (
	"reflect"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// Layout describes native memory for a reflected or generated field binding.
// The operations are also used for native addresses retained by EVM.
func Layout(t reflect.Type, fields *Struct, direct bool) *values.NativeLayout {
	l := &values.NativeLayout{Size: uint64(t.Size()), New: func() unsafe.Pointer { return reflect.New(t).UnsafePointer() }, Load: func(p unsafe.Pointer) values.Value { return Load(t, p) }, Store: func(p unsafe.Pointer, v values.Value) { Store(t, p, v) }}
	if direct {
		directScalarLayout(t, l)
	}
	if fields != nil {
		if fields.Type != t {
			panic("nativebind: layout type mismatch")
		}
		l.New = fields.New
		for _, f := range fields.Fields {
			l.Fields = append(l.Fields, values.NativeField{Offset: f.Offset, Get: f.Get, Set: f.Set})
		}
	}
	return l
}

// Scalar addresses may escape a generated field operation. Resolve their
// underlying Go layout once so dereferencing them does not re-enter reflection.
// Named scalar types have the identical representation. Typed assignments keep
// Go write barriers for string and pointer fields.
func directScalarLayout(t reflect.Type, l *values.NativeLayout) {
	switch t.Kind() {
	case reflect.Bool:
		scalarLayout(l, values.Bool, func(v values.Value) bool { return v.R != 0 })
	case reflect.Int:
		signedLayout[int](l)
	case reflect.Int8:
		signedLayout[int8](l)
	case reflect.Int16:
		signedLayout[int16](l)
	case reflect.Int32:
		signedLayout[int32](l)
	case reflect.Int64:
		signedLayout[int64](l)
	case reflect.Uint:
		unsignedLayout[uint](l)
	case reflect.Uint8:
		unsignedLayout[uint8](l)
	case reflect.Uint16:
		unsignedLayout[uint16](l)
	case reflect.Uint32:
		unsignedLayout[uint32](l)
	case reflect.Uint64:
		unsignedLayout[uint64](l)
	case reflect.Uintptr:
		unsignedLayout[uintptr](l)
	case reflect.Float32:
		scalarLayout(l, values.Float32, values.Value.AsFloat32)
	case reflect.Float64:
		scalarLayout(l, values.Float, values.Value.AsFloat)
	case reflect.String:
		scalarLayout(l, values.String, values.Value.AsString)
	case reflect.Pointer:
		scalarLayout(l, func(p unsafe.Pointer) values.Value { return values.Value{P: p} }, func(v values.Value) unsafe.Pointer { return v.P })
	}
}
func scalarLayout[T any](l *values.NativeLayout, load func(T) values.Value, store func(values.Value) T) {
	l.Load = func(p unsafe.Pointer) values.Value { return load(*(*T)(p)) }
	l.Store = func(p unsafe.Pointer, v values.Value) { *(*T)(p) = store(v) }
}
func signedLayout[T ~int | ~int8 | ~int16 | ~int32 | ~int64](l *values.NativeLayout) {
	l.Load = func(p unsafe.Pointer) values.Value { return values.Int(int64(*(*T)(p))) }
	l.Store = func(p unsafe.Pointer, v values.Value) { *(*T)(p) = T(v.AsInt()) }
}
func unsignedLayout[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr](l *values.NativeLayout) {
	l.Load = func(p unsafe.Pointer) values.Value { return values.Uint(uint64(*(*T)(p))) }
	l.Store = func(p unsafe.Pointer, v values.Value) { *(*T)(p) = T(v.R) }
}
