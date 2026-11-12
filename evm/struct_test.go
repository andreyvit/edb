package evm

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/values"
)

func TestStructInstructionsValidationAndSnapshot(t *testing.T) {
	str := &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}
	typ := values.NewStructShape("Item", values.Field{Name: "Text", Shape: str})
	ptr := values.NewPointerShape(typ)
	makeModule := func() *Module {
		return &Module{Shapes: []*values.Shape{typ, str}, Strings: []string{"retained"}, Functions: []Function{{Name: "make", Signature: Signature{Results: []*values.Type{ptr.Type}}, RCount: 2, PCount: 2, Code: []Instruction{ABC(OpAlloc, 0, 0, 0), WithBC(OpString, 1, 0), ABC(OpStore, 0, 1, 1), ABC(OpReturn, 0, 0, 0)}}}}
	}
	for _, mutate := range []func(*Module){
		func(m *Module) { m.Shapes[0] = nil },
		func(m *Module) { m.Shapes = make([]*values.Shape, 65537) },
		func(m *Module) { m.Functions[0].Code[0] = ABC(OpAlloc, 0, 2, 0) },
		func(m *Module) { m.Functions[0].Code[2] = ABC(OpStore, 0, 2, 1) },
		func(m *Module) { m.Functions[0].Code[2] = ABC(OpStore, 0, 1, 2) },
		func(m *Module) { m.Functions[0].Positions = []SourcePosition{{Line: 1}} },
	} {
		m := makeModule()
		mutate(m)
		if _, err := Link(m); err == nil {
			t.Fatal("accepted invalid struct bytecode")
		}
	}
	// Retired struct opcodes remain reserved and must never execute.
	for op := Opcode(36); op <= 39; op++ {
		m := makeModule()
		m.Functions[0].Code[0] = ABC(op, 0, 0, 0)
		if _, err := Link(m); err == nil {
			t.Fatalf("accepted retired opcode %d", op)
		}
	}
	m := makeModule()
	p, err := Link(m)
	if err != nil {
		t.Fatal(err)
	}
	m.Shapes[0] = nil
	m.Strings[0] = "changed"
	str.Type, str.Kind = values.Uint64, values.UintKind
	vm := New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "make", nil, out); err != nil {
		t.Fatal(err)
	}
	str.Type, str.Kind = values.StringType, values.StringKind
	if typ.GetField(out[0].P, 0).AsString() != "retained" {
		t.Fatal("changed linked storage")
	}
	for _, ptr := range vm.p {
		if ptr != nil {
			t.Fatal("retained struct in VM")
		}
	}
}
