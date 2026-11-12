package evmcompiler_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func ownershipFixture() (evm.Package, []byte, *string) {
	data := []byte("live")
	kept := new(string)
	callback := &values.Shape{Type: values.NewType("func()", values.Pair), Kind: values.FunctionKind, Cells: 1}
	end := func() { copy(data, "dead") }
	pkg := evm.Package{Path: "borrow", Types: []evm.TypeDecl{{Shape: callback}, {Name: "Text", ScalarBase: values.StringType, Shape: &values.Shape{Type: values.NewType("borrow.Text", values.Pair), Kind: values.StringKind, Cells: 1}}}, Functions: []evm.ExternalFunction{
		{Name: "Read", Signature: evm.Signature{Results: []*values.Type{values.StringType}}, Ownership: &evm.OwnershipEffects{NoRetain: true, NoInvalidate: true, Borrowed: []evm.BorrowSource{{Result: 0, Domain: "test.buffer"}}}, Call: func(c *evm.Call) error {
			c.SetResult(0, values.String(unsafe.String(unsafe.SliceData(data), len(data))))
			return nil
		}},
		{Name: "End", Ownership: &evm.OwnershipEffects{NoRetain: true, Ends: []evm.ScopeEnd{{Domain: "test.buffer"}}}, Call: func(*evm.Call) error { end(); return nil }},
		{Name: "RawEnd", Signature: evm.Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{values.Uint64}}, Direct: evm.DirectUnary(func(uint64) uint64 { end(); return 0 })},
		{Name: "UnknownEnd", Call: func(*evm.Call) error { end(); return nil }},
		{Name: "Keep", Signature: evm.Signature{Params: []*values.Type{values.StringType}}, Call: func(c *evm.Call) error { *kept = c.Argument(0).AsString(); return nil }},
		{Name: "Write", Signature: evm.Signature{Params: []*values.Type{callback.Type}}, Ownership: &evm.OwnershipEffects{NoRetain: true, Callbacks: []evm.ScopedCallback{{Argument: 0, Domain: "test.buffer"}}}, Call: func(c *evm.Call) error { defer end(); return c.RunCallback(c.Argument(0)) }},
	}}
	return pkg, data, kept
}

const ownershipSource = `import "borrow"
func Count(n int) int { sum:=0;for i:=0;i<n;i++ { s:=borrow.Read();sum+=len(s) };return sum }
func Local(n int) int { var a [1]string;sum:=0;for i:=0;i<n;i++{a[0]=borrow.Read();sum+=len(a[0])};return sum }
func LocalReturn(n int) string { var a [1]string;for i:=0;i<n;i++{a[0]=borrow.Read()};return a[0] }
func LocalMap(n int) string { m:=map[int]string{};for i:=0;i<n;i++{m[0]=borrow.Read()};return m[0] }
func LocalRange() string {m:=map[string]string{borrow.Read():borrow.Read()};for k,v:=range m{if k==v{return v}};return ""}
func Search(n int) string { for i:=0;i<n;i++ { s:=borrow.Read();if i==n-1{return s} };return "" }
func Live() string { s:=borrow.Read();borrow.End();return s }
func Raw() string { s:=borrow.Read();borrow.RawEnd(0);return s }
func SharedDirect() string {x:=borrow.Text(borrow.Read());borrow.End();return string(x)}
func SharedText() string { var x any=borrow.Text(borrow.Read());borrow.End();return string(x.(borrow.Text)) }
func Unknown() string { s:=borrow.Read();borrow.UnknownEnd();return s }
func Map() string { m:=map[string]string{};s:=borrow.Read();m[s]=s;borrow.End();return m["live"] }
func Slice() string { s:=[]string{borrow.Read()};alias:=s;borrow.End();return alias[0] }
func Nested() string { s:=[]map[string]string{{"x":borrow.Read()}};borrow.End();return s[0]["x"] }
func Callback() string { var s string;borrow.Write(func(){s=borrow.Read()});return s }
func Retaining() { s:=borrow.Read();borrow.Keep(s);borrow.End() }
func Any() string { var x any=borrow.Read();borrow.End();return x.(string) }
func Mixed(flag bool) any { var x any=17;if flag{x=borrow.Read()};borrow.End();return x }
func Deferred() (s string) { s=borrow.Read();defer borrow.End();return }
func Panic() (s string) { defer func(){recover()}();s=borrow.Read();defer borrow.End();panic("stop") }
`

func TestOwnershipBoundaries(t *testing.T) {
	for _, debug := range []bool{false, true} {
		pkg, data, kept := ownershipFixture()
		mod, err := (evmcompiler.Options{NoOptimization: debug}).Compile("borrow.go", ownershipSource, pkg)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(mod.Disassemble(), "Own") {
			t.Fatal("ownership not lowered")
		}
		program, err := evm.Link(mod, pkg)
		if err != nil {
			t.Fatal(err)
		}
		vm := evm.New(program)
		var out [1]values.Value
		for _, flag := range []uint64{0, 1} {
			copy(data, "live")
			if err := vm.Run(context.Background(), "Mixed", []values.Value{{R: flag}}, out[:]); err != nil {
				t.Fatal("mixed ownership join", err)
			}
		}
		for _, name := range []string{"Live", "SharedText", "SharedDirect", "Raw", "Unknown", "Map", "Slice", "Nested", "Callback", "Any", "Deferred", "Panic", "LocalRange"} {
			copy(data, "live")
			if err := vm.Run(context.Background(), name, nil, out[:]); err != nil || out[0].AsString() != "live" {
				t.Fatalf("debug=%v %s: %q %v", debug, name, out[0].AsString(), err)
			}
		}
		copy(data, "live")
		if err := vm.Run(context.Background(), "Retaining", nil, nil); err != nil || *kept != "live" {
			t.Fatal("host retained borrowed storage", *kept, err)
		}
		copy(data, "live")
		budget := evm.Budget{Steps: 1 << 30, Allocations: 1, AllocationBytes: 4}
		if err := vm.RunWithBudget(context.Background(), "Search", []values.Value{values.Int(1000000)}, out[:], budget); err != nil {
			t.Fatal("search must clone only its returned match", err)
		}
		for _, name := range []string{"LocalReturn", "LocalMap"} {
			if err := vm.RunWithBudget(context.Background(), name, []values.Value{values.Int(10000)}, out[:], evm.Budget{Steps: 1 << 30, Allocations: 20, AllocationBytes: 4096}); err != nil {
				t.Fatal("local container copied every insertion", name, err)
			}
		}
		copy(data, "dead")
		if out[0].AsString() != "live" {
			t.Fatal("returned search result borrows storage")
		}
		copy(data, "live")
		if err := vm.RunWithBudget(context.Background(), "Live", nil, out[:], budget); err != nil {
			t.Fatal("live-across-end result should need one clone", err)
		}
	}
}

func TestBorrowReadAllocationsAndBudget(t *testing.T) {
	pkg, _, _ := ownershipFixture()
	mod, err := evmcompiler.Compile("borrow.go", ownershipSource, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	vm.Limits.Steps = 1 << 30
	var out [1]values.Value
	args := []values.Value{values.Int(10000)}
	if err := vm.Run(context.Background(), "Count", args, out[:]); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(10, func() {
		if err := vm.Run(context.Background(), "Count", args, out[:]); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 || out[0].R != 40000 {
		t.Fatal("borrowed scan allocated", allocs, out)
	}
	if err := vm.RunWithBudget(context.Background(), "Live", nil, out[:], evm.Budget{AllocationBytes: 1}); !errors.Is(err, evm.ErrAllocation) {
		t.Fatal("clone bypassed allocation budget", err)
	}
	changed := pkg
	changed.Functions = append([]evm.ExternalFunction(nil), pkg.Functions...)
	changed.Functions[0].Ownership = nil
	if _, err := evm.Link(mod, changed); err == nil {
		t.Fatal("link accepted changed ownership contract")
	}
}

func TestRecordCellOwnership(t *testing.T) {
	s := kvo.NewSchema()
	entity := kvo.NewEntityType(s, "row")
	kvo.NewProp(s, 1, "count", kvo.TUint64, nil)
	kvo.NewProp(s, 2, "name", kvo.TMachineString, nil)
	kvo.NewProp(s, 3, "child", entity, nil)
	model := kvo.NewModel(s, entity, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(1))
		b.Prop(s.MustPropByCode(2))
		b.Prop(s.MustPropByCode(3))
	})
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		for _, debug := range []bool{true, false} {
			edit := row2.Edit(row2.Empty(layout))
			edit.Set([]row2.Step{{Key: 1}}, values.Uint(7), row2.Word)
			edit.Set([]row2.Step{{Key: 2}}, values.String("live"), row2.Data)
			edit.Set([]row2.Step{{Key: 3}, {Key: 1}}, values.Uint(9), row2.Word)
			original := edit.Snapshot().Raw()
			data := append([]byte(nil), original...)
			row := kvo.Encoded(row2.Trusted(data)).WithModel(model)
			pkg, err := evm.ModelPackage("db", s)
			if err != nil {
				t.Fatal(err)
			}
			rt := pkg.Record(model, false).Pointer
			pkg.Functions = append(pkg.Functions,
				evm.ExternalFunction{Name: "Read", Signature: evm.Signature{Results: []*values.Type{rt}}, Ownership: &evm.OwnershipEffects{NoRetain: true, NoInvalidate: true}, Direct: evm.DirectBinding{Op: evm.OpCallHostS, Call: func(c *evm.Call, _, _ values.Value) (values.Value, error) { return c.RecordValue(model, row) }}},
				evm.ExternalFunction{Name: "Tuple", Signature: evm.Signature{Results: []*values.Type{rt, values.Boolean}}, Call: func(c *evm.Call) error { c.SetResult(0, row.Value()); c.SetResult(1, values.Bool(true)); return nil }},
				evm.ExternalFunction{Name: "End", Call: func(*evm.Call) error { clear(data); return nil }},
			)
			mod, err := (evmcompiler.Options{NoOptimization: debug}).Compile("cells.go", `import("db";"fmt")
func Scan(n int) uint64 { var sum uint64; for i:=0;i<n;i++ {r:=db.Read();sum+=r.count+uint64(len(r.name))};return sum }
func Search(n int)*db.Row {for i:=0;i<n;i++ {r:=db.Read();if i==n-1{return r}};return nil}
func Live()uint64{r:=db.Read();db.End();return r.count}
func Name()string{s:=db.Read().name;db.End();return s}
func Child()*db.Row{return db.Read().child}
func Any()any{return db.Read()}
func InterfaceScan(n int) uint64 {var sum uint64;for i:=0;i<n;i++{var x any=db.Read();sum+=x.(*db.Row).count};return sum}
func Tuple()*db.Row{r,ok:=db.Tuple();if !ok{return nil};db.End();return r}
func Copy()uint64 {r:=db.Read();v:=*r;p:=&v;var x any=p;db.End();v=*r;return x.(*db.Row).count}
func Format()string {r:=db.Read();return fmt.Sprintf("%T %v %p",r,r,r)}
func Missing(r *db.Row) bool {var z *db.Row;var x any=r;return r==nil && r==z && x==any(z)}
func TypedNil()bool {var r *db.Row;var x any=r;return x!=nil && x.(*db.Row)==nil}
`, pkg)
			if err != nil {
				t.Fatal(err)
			}
			p, err := evm.Link(mod, pkg)
			if err != nil {
				t.Fatal(err)
			}
			vm := evm.New(p)
			var out [1]values.Value
			run := func(name string, args ...values.Value) {
				t.Helper()
				copy(data, original)
				if err := vm.RunWithBudget(context.Background(), name, args, out[:], evm.Budget{Steps: 1 << 30}); err != nil {
					t.Fatalf("layout=%v debug=%v %s: %v", layout, debug, name, err)
				}
			}
			for _, fn := range []string{"Scan", "InterfaceScan"} {
				run(fn, values.Int(10000))
				if allocs := testing.AllocsPerRun(5, func() { run(fn, values.Int(10000)) }); allocs != 0 {
					t.Fatalf("%s allocated %g", fn, allocs)
				}
			}
			run("Search", values.Int(1000000))
			clear(data)
			if r := kvo.FromValue(out[0]); !r.OwnsData() || r.GetValue(1).R != 7 {
				t.Fatal("search result not owned")
			}
			copy(data, original)
			if err := vm.RunWithBudget(context.Background(), "Search", []values.Value{values.Int(1000000)}, out[:], evm.Budget{Steps: 1 << 30, Allocations: 1, AllocationBytes: uint64(len(data))}); err != nil {
				t.Fatal("search copied more than the match", err)
			}
			for _, fn := range []string{"Live", "Copy"} {
				run(fn)
				if out[0].R != 7 {
					t.Fatal(fn, out)
				}
			}
			run("Name")
			if out[0].AsString() != "live" {
				t.Fatal("borrowed name escaped")
			}
			run("Child")
			clear(data)
			if kvo.FromValue(out[0]).GetValue(1).R != 9 {
				t.Fatal("child escaped")
			}
			run("Any")
			clear(data)
			if kvo.FromValue(out[0]).GetValue(1).R != 7 {
				t.Fatal("interface boxed or borrowed")
			}
			run("Tuple")
			if kvo.FromValue(out[0]).GetValue(1).R != 7 {
				t.Fatal("tuple borrow escaped")
			}
			run("Format")
			if text := out[0].AsString(); !strings.HasPrefix(text, "*db.Row 0x") || strings.Contains(text, "%!") {
				t.Fatal("record formatting", text)
			}
			run("Missing", kvo.Encoded(row2.View{}).WithModel(model).Value())
			if out[0].R != 1 {
				t.Fatal("tagged missing record did not compare equal to nil")
			}
			run("TypedNil")
			if out[0].R != 1 {
				t.Fatal("typed nil")
			}
		}
	}
}
