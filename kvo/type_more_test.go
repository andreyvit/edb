package kvo

import (
	"testing"
)

type myInt uint64

func (m myInt) String() string { return "myInt" }

func TestWordTypeAndConstructors(t *testing.T) {
	eq(t, sign(-1), -1)
	eq(t, sign(0), 0)
	eq(t, sign(1), 1)
	eq(t, signf(-1.0), -1)
	eq(t, signf(0.0), 0)
	eq(t, signf(1.0), 1)

	typ := NewIntType[uint64]("u64", func(fc *FmtContext, v uint64) string { return Hex(v) })
	eq(t, typ.Name(), "u64")
	eq(t, typ.String(), "u64")
	_ = typ.Schema()
	_ = typ.ValueKind()
	_ = typ.ItemType()
	_ = typ.Model()
	_ = typ.MapKeyType()
	_ = typ.MapProp(0)
	_ = typ.MapValueType(0)
	if got := typ.FormatValue(nil, 0x42); got != "42h" {
		t.Fatalf("FormatValue = %q, wanted 42h", got)
	}
	eq(t, typ.Add(1, 2), uint64(3))
	eq(t, typ.Sub(10, 3), uint64(7))
	eq(t, typ.Sign(1), 1)
	_ = typ.MulInt(2, 3)
	_ = typ.Neg(1)

	st := NewIntStringerType[myInt]("myInt")
	eq(t, st.FormatValue(nil, 1), "myInt")

	ft := NewFloatType[float64]("f64", func(fc *FmtContext, v float64) string { return "x" })
	eq(t, ft.FormatValue(nil, 0), "x")

	sub := NewScalarSubtype[uint64]("sub", typ)
	eq(t, sub.Name(), "sub")
	eq(t, sub.FormatValue(nil, 0x42), "42h")

	unk := NewUnknownTypeWithErrorCode("E")
	if got := unk.FormatValue(nil, 0x42); got == "" {
		t.Fatalf("NewUnknownTypeWithErrorCode.FormatValue returned empty string")
	}

	// Exercise EntityType methods via the debug schema types.
	_ = TDoodad.Name()
	_ = TDoodad.String()
	_ = TDoodad.Schema()
	_ = TDoodad.ValueKind()
	_ = TDoodad.ItemType()
	_ = TDoodad.MapKeyType()
	_ = TDoodad.MapProp(KFoo)
	_ = TDoodad.MapValueType(KFoo)
	_ = TDoodad.typeCodeSet()
	_ = TDoodad.Model()

	// Exercise MapType methods.
	mt := Map(TUint64, TUint64)
	_ = mt.Name()
	_ = mt.String()
	_ = mt.Schema()
	_ = mt.ValueKind()
	_ = mt.ItemType()
	_ = mt.Model()
	_ = mt.MapKeyType()
	_ = mt.MapProp(1)
	_ = mt.MapValueType(1)
	_ = mt.typeCodeSet()

	assertPanics(t, "EntityType.FormatValue", func() { _ = TDoodad.FormatValue(nil, 0) })
	assertPanics(t, "EntityType.Sub", func() { _ = TDoodad.Sub(1, 2) })
	assertPanics(t, "EntityType.Add", func() { _ = TDoodad.Add(1, 2) })
	assertPanics(t, "EntityType.Sign", func() { _ = TDoodad.Sign(1) })

	assertPanics(t, "MapType.FormatValue", func() { _ = mt.FormatValue(nil, 0) })
	assertPanics(t, "MapType.Sub", func() { _ = mt.Sub(1, 2) })
	assertPanics(t, "MapType.Add", func() { _ = mt.Add(1, 2) })
	assertPanics(t, "MapType.Sign", func() { _ = mt.Sign(1) })

	_ = TMachineString.Name()
	_ = TMachineString.String()
	_ = TMachineString.Schema()
	_ = TMachineString.ValueKind()
	_ = TMachineString.ItemType()
	_ = TMachineString.Model()
	_ = TMachineString.MapKeyType()
	_ = TMachineString.MapProp(0)
	_ = TMachineString.MapValueType(0)
	_ = TMachineString.typeCodeSet()
	assertPanics(t, "reportCannotAccessKey", func() { reportCannotAccessKey(TMachineString, 1) })
}

func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic")
			}
		}()
		fn()
	})
}
