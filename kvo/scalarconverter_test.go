package kvo

import (
	"testing"
)

func TestScalarConverter(t *testing.T) {
	type Foo int
	var TFoo = NewIntType[Foo]("foo")
	eq(t, TFoo.ScalarConverter().ValueToScalar(Foo(42)), 42)
	eq(t, TFoo.ScalarConverter().ScalarToValue(42), Foo(42))
}

func eq[T comparable](t testing.TB, a, e T) {
	if a != e {
		t.Helper()
		t.Fatalf("** got %v, wanted %v", a, e)
	}
}
