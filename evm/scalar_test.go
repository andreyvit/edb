package evm

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/values"
)

func TestConversionValidationAndSnapshot(t *testing.T) {
	makeModule := func() *Module {
		return &Module{
			Conversions: []Conversion{{From: NumericType{NumericUnsigned, 64}, To: NumericType{NumericSigned, 8}}},
			Functions:   []Function{{Name: "convert", RCount: 1, Signature: Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{values.Int8}}, Code: []Instruction{ABC(OpConvert, 0, 0, 0), ABC(OpReturn, 0, 0, 0)}}},
		}
	}
	for _, mutate := range []func(*Module){
		func(m *Module) { m.Conversions[0].From.Bits = 7 },
		func(m *Module) { m.Conversions[0].To = NumericType{NumericFloat, 8} },
		func(m *Module) { m.Functions[0].Code[0] = ABC(OpConvert, 0, 0, 1) },
		func(m *Module) { m.Functions[0].Code[0] = ABC(OpConvert, 1, 0, 0) },
		func(m *Module) { m.Functions[0].Code[0] = ABC(OpCheckShift, 0, 1, 0) },
	} {
		m := makeModule()
		mutate(m)
		if _, err := Link(m); err == nil {
			t.Fatal("accepted invalid scalar operation")
		}
	}
	m := makeModule()
	p, err := Link(m)
	if err != nil {
		t.Fatal(err)
	}
	m.Conversions[0].To = NumericType{NumericUnsigned, 8}
	out := make([]values.Value, 1)
	if err := New(p).Run(context.Background(), "convert", []values.Value{{R: 255}}, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != -1 {
		t.Fatalf("mutated linked conversion: %v", out)
	}
}
