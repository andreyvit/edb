package kvo

import (
	"reflect"
)

type (
	IntegerValue interface {
		~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
	}
	FloatValue interface {
		float32 | ~float64
	}
)

type AnyType interface {
	Name() string
	reflectType() reflect.Type
}

type Type[T any] struct {
	name  string
	rtype reflect.Type
	conv  ScalarConverter[T]
}

func (typ *Type[T]) Name() string {
	return typ.name
}

func (typ *Type[T]) ScalarConverter() ScalarConverter[T] {
	return typ.conv
}

func (typ *Type[T]) reflectType() reflect.Type {
	return typ.rtype
}

func NewScalarType[T any](name string, conv ScalarConverter[T]) *Type[T] {
	return &Type[T]{
		name:  name,
		rtype: reflect.TypeFor[T](),
		conv:  conv,
	}
}

func NewIntType[T IntegerValue](name string) *Type[T] {
	return NewScalarType(name, intScalarConverter[T]{})
}

func NewFloatType[T FloatValue](name string) *Type[T] {
	return NewScalarType(name, floatScalarConverter[T]{})
}

func NewScalarSubtype[T any](name string, base *Type[T]) *Type[T] {
	return &Type[T]{
		name:  name,
		rtype: reflect.TypeFor[T](),
		conv:  base.conv,
	}
}

func NewEntityType(schema *Schema, name string) *Type[any] {
	return &Type[any]{
		name:  name,
		rtype: nil,
		conv:  nil,
	}
}

var (
	Int64  = NewIntType[int64]("int64")
	Uint64 = NewIntType[uint64]("uint64")
)
