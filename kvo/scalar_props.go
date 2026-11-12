package kvo

import (
	"github.com/andreyvit/edb/values"
)

// Scalar property references contain only the permanent code. Their concrete
// type selects conversion, so Get does not consult a codec or use reflection.
// The phantom marker prevents conversions between differently typed codes.

type SignedPropRef[T values.Signed] struct {
	_    [0]*T
	code uint64
}

func (p SignedPropRef[T]) Code() uint64   { return p.code }
func (p SignedPropRef[T]) Get(r Record) T { v := r.GetValue(p.code); return T(v.AsInt()) }
func (p SignedPropRef[T]) readEditor(e *Editor) T {
	v := e.GetValue(p.code)
	return T(v.AsInt())
}
func (p SignedPropRef[T]) toValue(v T) values.Value { return values.Int(int64(v)) }
func (p SignedPropRef[T]) Set(e *Editor, v T)       { e.SetValue(p.code, p.toValue(v)) }
func BindSignedProp[T values.Signed](schema *Schema, code uint64) SignedPropRef[T] {
	checkCodec(schema.MustPropByCode(code).AnyType(), values.SignedCodec[T]())
	return SignedPropRef[T]{code: code}
}
func NewSignedProp[T values.Signed](schema *Schema, code uint64, name string, typ AnyType) SignedPropRef[T] {
	checkCodec(typ, values.SignedCodec[T]())
	NewProp(schema, code, name, typ, nil)
	return SignedPropRef[T]{code: code}
}

type UnsignedPropRef[T values.Unsigned] struct {
	_    [0]*T
	code uint64
}

func (p UnsignedPropRef[T]) Code() uint64   { return p.code }
func (p UnsignedPropRef[T]) Get(r Record) T { v := r.GetValue(p.code); return T(v.AsUint()) }
func (p UnsignedPropRef[T]) readEditor(e *Editor) T {
	v := e.GetValue(p.code)
	return T(v.AsUint())
}
func (p UnsignedPropRef[T]) toValue(v T) values.Value { return values.Uint(uint64(v)) }
func (p UnsignedPropRef[T]) Set(e *Editor, v T)       { e.SetValue(p.code, p.toValue(v)) }
func BindUnsignedProp[T values.Unsigned](schema *Schema, code uint64) UnsignedPropRef[T] {
	checkCodec(schema.MustPropByCode(code).AnyType(), values.UnsignedCodec[T]())
	return UnsignedPropRef[T]{code: code}
}
func NewUnsignedProp[T values.Unsigned](schema *Schema, code uint64, name string, typ AnyType) UnsignedPropRef[T] {
	checkCodec(typ, values.UnsignedCodec[T]())
	NewProp(schema, code, name, typ, nil)
	return UnsignedPropRef[T]{code: code}
}

type StringPropRef[T ~string] struct {
	_    [0]*T
	code uint64
}

func (p StringPropRef[T]) Code() uint64   { return p.code }
func (p StringPropRef[T]) Get(r Record) T { v := r.GetValue(p.code); return T(v.AsString()) }
func (p StringPropRef[T]) readEditor(e *Editor) T {
	v := e.GetValue(p.code)
	return T(v.AsString())
}
func (p StringPropRef[T]) toValue(v T) values.Value { return values.String(string(v)) }
func (p StringPropRef[T]) Set(e *Editor, v T)       { e.SetValue(p.code, p.toValue(v)) }
func BindStringProp[T ~string](schema *Schema, code uint64) StringPropRef[T] {
	checkCodec(schema.MustPropByCode(code).AnyType(), values.StringCodec[T]())
	return StringPropRef[T]{code: code}
}
func NewStringProp[T ~string](schema *Schema, code uint64, name string, typ AnyType) StringPropRef[T] {
	checkCodec(typ, values.StringCodec[T]())
	NewProp(schema, code, name, typ, nil)
	return StringPropRef[T]{code: code}
}

type BoolPropRef[T ~bool] struct {
	_    [0]*T
	code uint64
}

func (p BoolPropRef[T]) Code() uint64   { return p.code }
func (p BoolPropRef[T]) Get(r Record) T { v := r.GetValue(p.code); return T(v.AsBool()) }
func (p BoolPropRef[T]) readEditor(e *Editor) T {
	v := e.GetValue(p.code)
	return T(v.AsBool())
}
func (p BoolPropRef[T]) toValue(v T) values.Value { return values.Bool(bool(v)) }
func (p BoolPropRef[T]) Set(e *Editor, v T)       { e.SetValue(p.code, p.toValue(v)) }
func BindBoolProp[T ~bool](schema *Schema, code uint64) BoolPropRef[T] {
	checkCodec(schema.MustPropByCode(code).AnyType(), values.BoolCodec[T]())
	return BoolPropRef[T]{code: code}
}
func NewBoolProp[T ~bool](schema *Schema, code uint64, name string, typ AnyType) BoolPropRef[T] {
	checkCodec(typ, values.BoolCodec[T]())
	NewProp(schema, code, name, typ, nil)
	return BoolPropRef[T]{code: code}
}

type Float32PropRef[T ~float32] struct {
	_    [0]*T
	code uint64
}

func (p Float32PropRef[T]) Code() uint64   { return p.code }
func (p Float32PropRef[T]) Get(r Record) T { v := r.GetValue(p.code); return T(v.AsFloat32()) }
func (p Float32PropRef[T]) readEditor(e *Editor) T {
	v := e.GetValue(p.code)
	return T(v.AsFloat32())
}
func (p Float32PropRef[T]) toValue(v T) values.Value { return values.Float32(float32(v)) }
func (p Float32PropRef[T]) Set(e *Editor, v T)       { e.SetValue(p.code, p.toValue(v)) }
func BindFloat32Prop[T ~float32](schema *Schema, code uint64) Float32PropRef[T] {
	checkCodec(schema.MustPropByCode(code).AnyType(), values.Float32Codec[T]())
	return Float32PropRef[T]{code: code}
}
func NewFloat32Prop[T ~float32](schema *Schema, code uint64, name string, typ AnyType) Float32PropRef[T] {
	checkCodec(typ, values.Float32Codec[T]())
	NewProp(schema, code, name, typ, nil)
	return Float32PropRef[T]{code: code}
}

type Float64PropRef[T ~float64] struct {
	_    [0]*T
	code uint64
}

func (p Float64PropRef[T]) Code() uint64   { return p.code }
func (p Float64PropRef[T]) Get(r Record) T { v := r.GetValue(p.code); return T(v.AsFloat()) }
func (p Float64PropRef[T]) readEditor(e *Editor) T {
	v := e.GetValue(p.code)
	return T(v.AsFloat())
}
func (p Float64PropRef[T]) toValue(v T) values.Value { return values.Float(float64(v)) }
func (p Float64PropRef[T]) Set(e *Editor, v T)       { e.SetValue(p.code, p.toValue(v)) }
func BindFloat64Prop[T ~float64](schema *Schema, code uint64) Float64PropRef[T] {
	checkCodec(schema.MustPropByCode(code).AnyType(), values.Float64Codec[T]())
	return Float64PropRef[T]{code: code}
}
func NewFloat64Prop[T ~float64](schema *Schema, code uint64, name string, typ AnyType) Float64PropRef[T] {
	checkCodec(typ, values.Float64Codec[T]())
	NewProp(schema, code, name, typ, nil)
	return Float64PropRef[T]{code: code}
}

type BytesPropRef[T ~[]byte] struct {
	_    [0]*T
	code uint64
}

func (p BytesPropRef[T]) Code() uint64   { return p.code }
func (p BytesPropRef[T]) Get(r Record) T { v := r.GetValue(p.code); return T(v.AsBytes()) }
func (p BytesPropRef[T]) readEditor(e *Editor) T {
	v := e.GetValue(p.code)
	return T(v.AsBytes())
}
func (p BytesPropRef[T]) toValue(v T) values.Value { return values.Bytes([]byte(v)) }
func (p BytesPropRef[T]) Set(e *Editor, v T)       { e.SetValue(p.code, p.toValue(v)) }
func BindBytesProp[T ~[]byte](schema *Schema, code uint64) BytesPropRef[T] {
	checkCodec(schema.MustPropByCode(code).AnyType(), values.BytesCodec[T]())
	return BytesPropRef[T]{code: code}
}
func NewBytesProp[T ~[]byte](schema *Schema, code uint64, name string, typ AnyType) BytesPropRef[T] {
	checkCodec(typ, values.BytesCodec[T]())
	NewProp(schema, code, name, typ, nil)
	return BytesPropRef[T]{code: code}
}
