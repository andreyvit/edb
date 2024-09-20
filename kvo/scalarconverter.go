package kvo

import (
	"math"
)

type ScalarConverter[T any] interface {
	ValueToScalar(value T) uint64
	ScalarToValue(scalar uint64) T
}

type intScalarConverter[T IntegerValue] struct{}

func (intScalarConverter[T]) ValueToScalar(value T) uint64 {
	return uint64(value)
}
func (intScalarConverter[T]) ScalarToValue(scalar uint64) T {
	return T(scalar)
}

type floatScalarConverter[T FloatValue] struct{}

func (floatScalarConverter[T]) ValueToScalar(value T) uint64 {
	return math.Float64bits(float64(value))
}
func (floatScalarConverter[T]) ScalarToValue(scalar uint64) T {
	return T(math.Float64frombits(scalar))
}
