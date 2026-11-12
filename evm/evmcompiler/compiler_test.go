package evmcompiler_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestCompileGenericEnvironment(t *testing.T) {
	mode := values.NewType("demo.Mode", values.Raw)
	modeShape := &values.Shape{Type: mode, Kind: values.IntKind, Bits: 64, Cells: 1}
	child := values.NewStructShape("demo.Child", values.Field{Name: "Text", Shape: &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}})
	childPtr := values.NewPointerShape(child)
	entry := values.NewStructShape("demo.Entry", values.Field{Name: "Title", Shape: &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}}, values.Field{Name: "Count", Shape: &values.Shape{Type: values.Int64, Kind: values.IntKind, Bits: 64, Cells: 1}}, values.Field{Name: "Mode", Shape: modeShape}, values.Field{Name: "Child", Shape: childPtr})
	entryPtr := values.NewPointerShape(entry)
	sig := evm.Signature{Params: []*values.Type{values.Int64, values.StringType, entryPtr.Type, values.Float64, values.Boolean}}
	layout, _ := evm.LayoutOf(sig.Params)
	var seen []values.Value
	pkg := evm.Package{Path: "example.test/demo", Types: []evm.TypeDecl{{Name: "Mode", Shape: modeShape}, {Name: "Entry", Shape: entry}, {Shape: entryPtr}, {Name: "Child", Shape: child}, {Shape: childPtr}}, Constants: []evm.ExternalConstant{{Name: "On", Type: mode, Value: values.Uint(2)}}, Functions: []evm.ExternalFunction{{Name: "Publish", Signature: sig}}}
	source := "package example\nimport d \"example.test/demo\"\nfunc Start() {\n" + `d.Publish(-9223372036854775808, "argument", &d.Entry{Title:"hello", Count:7, Mode:d.On, Child:&d.Child{Text:"nested"}}, 1.25, true)` + "\n" + `d.Publish(9, "argument", &d.Entry{Title:"second"}, 1.25, true)` + "\n}"
	m, err := evmcompiler.Compile("source.go", source, pkg)
	if err != nil {
		t.Fatal(err)
	}
	pkg.Functions[0].Call = func(c *evm.Call) error {
		runtime.GC()
		n := int64(c.R[layout.Slots[0]])
		if (len(seen) == 0 && n != -9223372036854775808) || (len(seen) == 1 && n != 9) {
			t.Fatalf("bad integer %d", n)
		}
		if c.Value(int(layout.Slots[1])).AsString() != "argument" || (values.Value{R: c.R[layout.Slots[3]]}).AsFloat() != 1.25 || c.R[layout.Slots[4]] != 1 {
			t.Fatal("bad primitive arguments")
		}
		if c.Position.File != "source.go" || c.Position.Line != 4+len(seen) {
			t.Fatalf("bad call location: %+v", c.Position)
		}
		seen = append(seen, values.Value{P: c.P[layout.Slots[2]]})
		return nil
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	if err := vm.Run(context.Background(), "Start", nil, nil); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	if len(seen) != 2 || entry.GetField(seen[0].P, 0).AsString() != "hello" || entry.GetField(seen[1].P, 0).AsString() != "second" {
		t.Fatal("lost or aliased entries")
	}
	if entry.GetField(seen[0].P, 1).R != 7 || entry.GetField(seen[0].P, 2).R != 2 || child.GetField(entry.GetField(seen[0].P, 3).P, 0).AsString() != "nested" || entry.GetField(seen[1].P, 3).P != nil {
		t.Fatal("bad fields or omitted defaults")
	}
	if _, err := evm.Link(m, evm.Package{Path: pkg.Path, Functions: []evm.ExternalFunction{{Name: "Publish", Signature: evm.Signature{}, Call: pkg.Functions[0].Call}}}); err == nil {
		t.Fatal("accepted changed signature")
	}
}

func BenchmarkCompileAndInitialize(b *testing.B) {
	typ := values.NewStructShape("h.Item", values.Field{Name: "Text", Shape: &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}})
	typPtr := values.NewPointerShape(typ)
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Item", Shape: typ}, {Shape: typPtr}}, Functions: []evm.ExternalFunction{{Name: "Add", Signature: evm.Signature{Params: []*values.Type{typPtr.Type}}, Call: func(c *evm.Call) error { _ = typ.GetField(c.P[0], 0).AsString(); return nil }}}}
	source := `import "h"
func Init(){h.Add(&h.Item{Text:"one"});h.Add(&h.Item{Text:"two"})}`
	b.Run("Compile", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := evmcompiler.Compile("main.go", source, pkg); err != nil {
				b.Fatal(err)
			}
		}
	})
	m, err := evmcompiler.Compile("main.go", source, pkg)
	if err != nil {
		b.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		b.Fatal(err)
	}
	vm := evm.New(p)
	b.Run("Run", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			vm.Reset()
			if err := vm.Run(context.Background(), "Init", nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestCompileNumericAndWindowBounds(t *testing.T) {
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Numbers", Signature: evm.Signature{Params: []*values.Type{values.Int64, values.Uint64, values.Float64}}, Call: func(c *evm.Call) error {
		if c.R[0] != uint64(1)<<63 || c.R[1] != ^uint64(0) || (values.Value{R: c.R[2]}).AsFloat() != -1.5 {
			t.Fatal("lost full-width constant")
		}
		return nil
	}}}}
	m, err := evmcompiler.Compile("main.go", `import "h"
func Init(){h.Numbers(-9223372036854775808,18446744073709551615,-1.5)}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := evm.New(p).Run(context.Background(), "Init", nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, args := range []string{"9223372036854775808,0,1", "0,18446744073709551616,1", "0,-1,1", "0,0,1e10000", "1.5,0,1"} {
		if _, err := evmcompiler.Compile("main.go", "import \"h\"\nfunc Init(){h.Numbers("+args+")}", pkg); err == nil {
			t.Fatalf("accepted %s", args)
		}
	}
	results := make([]*values.Type, 65535)
	for i := range results {
		results[i] = values.Int64
	}
	pkg.Functions[0].Signature.Results = results
	if _, err := evmcompiler.Compile("main.go", "import \"h\"\nfunc Init(){h.Numbers(1,2,3)}", pkg); err == nil {
		t.Fatal("oversized outgoing result window accepted")
	}
}

func TestCompilerEnvironmentValidationAndSourceLimit(t *testing.T) {
	typ := values.NewStructShape("h.Empty")
	typPtr := values.NewPointerShape(typ)
	fn := evm.ExternalFunction{Name: "Add", Signature: evm.Signature{Params: []*values.Type{typPtr.Type}}}
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Empty", Shape: typ}, {Shape: typPtr}}, Functions: []evm.ExternalFunction{fn}}
	for _, pkgs := range [][]evm.Package{
		{pkg, pkg}, {{Path: ""}},
		{{Path: "h", Types: []evm.TypeDecl{{Name: "Empty"}}}},
		{{Path: "h", Types: []evm.TypeDecl{{Name: "Empty", Shape: typ}, {Shape: typPtr}, {Name: "Empty", Shape: typ}, {Shape: typPtr}}}},
		{{Path: "h", Constants: []evm.ExternalConstant{{Name: "X"}}}},
		{{Path: "h", Functions: []evm.ExternalFunction{fn, fn}, Types: pkg.Types}},
	} {
		if m, err := evmcompiler.Compile("main.go", "import _ \"h\"\nfunc Init(){}", pkgs...); err == nil || m != nil {
			t.Fatalf("invalid environment accepted: %+v", pkgs)
		}
	}
	const source = "import \"h\"\nfunc Init(){h.Add(&h.Empty{})}"
	maxSource := source + strings.Repeat(" ", evmcompiler.MaxSourceSize-len(source))
	if _, err := evmcompiler.Compile("main.go", maxSource, pkg); err != nil {
		t.Fatal(err)
	}
	if m, err := evmcompiler.Compile("main.go", maxSource+" ", pkg); err == nil || m != nil {
		t.Fatal("accepted oversized source")
	}
}
