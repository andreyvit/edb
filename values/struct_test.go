package values_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/andreyvit/edb/values"
)

func TestStructShapeStorage(t *testing.T) {
	str := &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}
	integer := &values.Shape{Type: values.Int64, Kind: values.IntKind, Bits: 64, Cells: 1}
	child := values.NewStructShape("example.Child", values.Field{Name: "N", Shape: integer})
	childPtr := values.NewPointerShape(child)
	empty := values.NewStructShape("example.Empty")
	fields := []values.Field{{Name: "Title", Shape: str}, {Name: "Count", Shape: integer}, {Name: "Child", Shape: childPtr}, {Name: "Nested", Shape: child}, {Name: "Empty", Shape: empty}}
	typ := values.NewStructShape("example.Entry", fields...)
	fields[0].Name = "changed"
	if typ.Fields[0].Name != "Title" || typ.Cells != 4 || typ.Fields[4].Offset != 4 || values.NewPointerShape(typ).Type.Repr() != values.Pointer {
		t.Fatal("mutable fields or incorrect layout")
	}
	p := typ.New()
	if typ.GetField(p, 0) != (values.Value{}) {
		t.Fatal("nonzero field")
	}
	title := strings.Repeat("live string ", 100)
	typ.SetField(p, 0, values.String(title))
	cp := child.New()
	child.SetField(cp, 0, values.Int(123))
	// Scalar writes must clear the unused bank.
	typ.SetField(p, 1, values.Value{R: 42, P: cp})
	typ.SetField(p, 2, values.Value{R: 99, P: cp})
	typ.SetField(p, 3, values.Value{P: cp})
	child.SetField(cp, 0, values.Int(456))
	alias := p
	runtime.GC()
	if typ.GetField(alias, 0).AsString() != title || typ.GetField(alias, 1) != values.Int(42) || typ.GetField(alias, 2).R != 0 {
		t.Fatal("lost scalar data or retained unused bank")
	}
	if child.GetField(typ.GetField(p, 2).P, 0).R != 456 || child.GetField(typ.GetField(p, 3).P, 0).R != 123 {
		t.Fatal("pointer must alias; inline aggregate must copy")
	}
	typ.SetField(alias, 1, values.Int(99))
	if typ.GetField(p, 1).R != 99 {
		t.Fatal("pointer copy must alias")
	}
	if empty.New() == nil || typ.GetField(p, 4).P == nil {
		t.Fatal("empty struct and trailing empty field require an address")
	}
	if values.NewStructShape("example.Entry").Type == typ.Type {
		t.Fatal("distinct declarations must retain nominal identity")
	}
}

func TestInvalidStructShapes(t *testing.T) {
	word := &values.Shape{Type: values.Uint64, Kind: values.UintKind, Bits: 64, Cells: 1}
	for _, makeShape := range []func(){
		func() { values.NewStructShape("") },
		func() { values.NewStructShape("T", values.Field{Name: "X"}) },
		func() { values.NewStructShape("T", values.Field{Shape: word}) },
		func() {
			values.NewStructShape("T", values.Field{Name: "X", Shape: word}, values.Field{Name: "X", Shape: word})
		},
		func() {
			values.NewStructShape("T", values.Field{Name: "X", Shape: &values.Shape{Type: values.Uint64, Cells: 1<<32 + 1}})
		},
		func() { values.NewPointerShape(nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("accepted invalid shape")
				}
			}()
			makeShape()
		}()
	}
}
