package kvo

import (
	"math"
	"testing"
	"time"
)

func TestScalarConverters(t *testing.T) {
	var ic intScalarConverter[uint64]
	eq(t, ic.ValueToScalar(42), uint64(42))
	eq(t, ic.ScalarToValue(42), uint64(42))

	var fc floatScalarConverter[float64]
	v := 123.25
	sc := fc.ValueToScalar(v)
	eq(t, sc, math.Float64bits(v))
	eq(t, fc.ScalarToValue(sc), v)

	var tc timeWordConverter
	tm := time.Date(2025, 12, 31, 23, 59, 58, 123456789, time.UTC)
	word := tc.ValueToScalar(tm)
	eq(t, tc.ScalarToValue(word), Uint64ToTime(word))
}

