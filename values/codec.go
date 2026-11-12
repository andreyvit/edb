package values

import "reflect"

// Codec converts between a cell and a Go value. Its functions do not inspect
// runtime types. Type is declaration metadata, used only when binding a codec.
// Strings and bytes borrow their input storage in both directions.
type Codec[T any] struct {
	Type   reflect.Type
	Decode func(Value) T
	Encode func(T) Value
}

type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}
type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

func SignedCodec[T Signed]() Codec[T] {
	return Codec[T]{reflect.TypeFor[T](), func(v Value) T { return T(v.AsInt()) }, func(v T) Value { return Int(int64(v)) }}
}
func UnsignedCodec[T Unsigned]() Codec[T] {
	return Codec[T]{reflect.TypeFor[T](), func(v Value) T { return T(v.AsUint()) }, func(v T) Value { return Uint(uint64(v)) }}
}
func BoolCodec[T ~bool]() Codec[T] {
	return Codec[T]{reflect.TypeFor[T](), func(v Value) T { return T(v.AsBool()) }, func(v T) Value { return Bool(bool(v)) }}
}
func Float32Codec[T ~float32]() Codec[T] {
	return Codec[T]{reflect.TypeFor[T](), func(v Value) T { return T(v.AsFloat32()) }, func(v T) Value { return Float32(float32(v)) }}
}
func Float64Codec[T ~float64]() Codec[T] {
	return Codec[T]{reflect.TypeFor[T](), func(v Value) T { return T(v.AsFloat()) }, func(v T) Value { return Float(float64(v)) }}
}
func StringCodec[T ~string]() Codec[T] {
	return Codec[T]{reflect.TypeFor[T](), func(v Value) T { return T(v.AsString()) }, func(v T) Value { return String(string(v)) }}
}
func BytesCodec[T ~[]byte]() Codec[T] {
	return Codec[T]{reflect.TypeFor[T](), func(v Value) T { return T(v.AsBytes()) }, func(v T) Value { return Bytes([]byte(v)) }}
}
