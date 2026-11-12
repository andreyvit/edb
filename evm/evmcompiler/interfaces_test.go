package evmcompiler_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
	"runtime"
	"strings"
	"testing"
	"unsafe"
)

func TestGoInterfaces(t *testing.T) {
	vm := compileGoVM(t, `type Adder interface{Add(int)int};type N int;func(n N)Add(x int)int{return int(n)+x};type S struct{N};type P struct{N int};func(p *P)Add(x int)int{if p==nil{return 50};p.N+=x;return p.N}
func call(a Adder)int{return a.Add(2)}
func Run()int{var x any=N(4);a:=x.(Adder);n:=call(a);var p *P;var typed Adder=p;if typed==nil{return -1};n+=call(typed);s:=S{8};n+=call(s);p=&P{3};n+=call(p);n+=call(p);switch v:=x.(type){case N:n+=int(v);default:return -2};if _,ok:=x.(int);ok{return -3};var zero any;if _,ok:=zero.(Adder);ok{return -4};return n}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 82 {
		t.Fatalf("interfaces: %v %v", err, out)
	}
}

func TestGoInterfaceSliceCapacity(t *testing.T) {
	vm := compileGoVM(t, `type Empty []struct{}
func Run(length, capacity int) (int, int, bool) {
 var v any = make(Empty, length, capacity)
 s, ok := v.(Empty)
 _, wrong := v.([]struct{})
 return len(s), cap(s), ok && !wrong && s != nil
}`)
	vm.Limits.AllocationBytes, vm.Limits.Allocations = ^uint64(0), ^uint64(0)
	for _, tc := range []struct {
		length, capacity uint32
		allocs           float64
	}{
		{0, 0, 1}, {1, 4096, 1}, {1, 4097, 1},
		{1<<22 - 1, 1<<23 - 2, 1}, {1<<22 - 1, 1<<23 - 1, 2},
		{1 << 22, 1<<22 + 4095, 1}, {1 << 22, 1<<22 + 4096, 2},
		{1<<32 - 1, 1<<32 - 1, 1},
	} {
		t.Run(fmt.Sprintf("%d/%d", tc.length, tc.capacity), func(t *testing.T) {
			args := []values.Value{values.Uint(uint64(tc.length)), values.Uint(uint64(tc.capacity))}
			var out [3]values.Value
			n := testing.AllocsPerRun(10, func() {
				if err := vm.Run(context.Background(), "Run", args, out[:]); err != nil {
					t.Fatal(err)
				}
				if out[0].R != uint64(tc.length) || out[1].R != uint64(tc.capacity) || out[2].R != 1 {
					t.Fatalf("slice header: %v", out)
				}
			})
			if n != tc.allocs {
				t.Fatalf("allocations: %g, want %g", n, tc.allocs)
			}
		})
	}
}

func TestGoInterfaceZeroValuesAndKeys(t *testing.T) {
	vm := compileGoVM(t, `type Count int64; type Text string
func recoverZero(v any) (ok bool) {
 defer func() { x:=recover(); ok=x!=nil && x==v }()
 panic(v)
}
func Run(z float64, nan float64) bool {
 for _, v := range []any{false, int64(0), uint64(0), float64(0), "", Count(0), Text("")} {
  if v==nil || !recoverZero(v) { return false }
 }
 m:=map[any]int{int64(0):1, Count(0):2, "same":3, Text("same"):4, z:5, nan:6}
 if m[int64(0)]!=1 || m[Count(0)]!=2 || m["same"]!=3 || m[Text("same")]!=4 || m[float64(0)]!=5 { return false }
 _, exists:=m[nan]
 return !exists && any(nan)!=any(nan) && any(z)==any(float64(0))
}`)
	var out [1]values.Value
	args := []values.Value{values.Uint(1 << 63), values.Uint(0x7ff8000000000042)}
	if err := vm.Run(context.Background(), "Run", args, out[:]); err != nil || out[0].R != 1 {
		t.Fatalf("zero values and map keys: %v %v", out, err)
	}
}
func TestGoInterfaceCollectionsAndCopies(t *testing.T) {
	vm := compileGoVM(t, `type S struct{N int};type N int;func Run()int{s:=S{2};var a any=s;s.N=9;var x any="key";m:=map[any]int{x:3,N(1):4,1:5};if m["key"]!=3||m[N(1)]!=4||m[1]!=5{return -1};delete(m,"key");if _,ok:=m[x];ok{return -2};p:=&s;var q any=p;if q.(*S)!=p{return -3};v:=[]int{7};var b any=v;b.([]int)[0]=8;if v[0]!=8{return -4};var f any=func()int{return 6};return a.(S).N+v[0]+f.(func()int)()}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 16 {
		t.Fatalf("copies: %v %v", err, out)
	}
}
func TestGoInterfaceComparisons(t *testing.T) {
	vm := compileGoVM(t, `type S struct{A int;B any};func Run()bool{var a any=[]int{1};var b any=map[int]int{};return a!=b && a!=nil && (S{1,a}!=S{2,a}) && any(3)==any(3) && any(3)!=any(int64(3)) && any("x")==any("x")}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("comparison: %v %v", err, out)
	}
	for _, src := range []string{`func Run(){var a any=[]int{1};_ = a==a}`, `func Run(){var m map[any]int;_ = m[[]int{1}]}`, `func Run(){var x any=3;_ = x.(string)}`, `func Run(){var x interface{F()};x.F()}`} {
		vm := compileGoVM(t, src)
		if err := vm.Run(context.Background(), "Run", nil, nil); err == nil {
			t.Fatalf("accepted runtime interface error: %s", src)
		}
	}
}
func TestGoInterfaceGenericMethods(t *testing.T) {
	vm := compileGoVM(t, `type Box[T any]struct{V T};func(b Box[T])Value()T{return b.V};func Run()int{var b interface{Value()int}=Box[int]{7};var s interface{Value()string}=Box[string]{"ok"};if s.Value()!="ok"{return -1};return b.Value()}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 7 {
		t.Fatalf("generic interface methods: %v %v", err, out)
	}
}

func TestGoNativeInterfacesAndMethods(t *testing.T) {
	text := &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}
	number := &values.Shape{Type: values.IntType, Kind: values.IntKind, Bits: 64, Cells: 1}
	named := &values.Shape{Type: values.NewType("host.Number", values.Raw), Kind: values.IntKind, Bits: 64, Cells: 1}
	object := &values.Shape{Type: values.NewType("host.Object", values.Pointer), Kind: values.StructKind, Cells: 1, Fields: []values.Field{{Name: "N", Shape: number}}}
	pointer := &values.Shape{Type: values.NewType("*host.Object", values.Pointer), Kind: values.PointerKind, Cells: 1, Elem: object}
	sig := &values.Shape{Type: values.NewType("describe signature", values.Pair), Kind: values.FunctionKind, Cells: 1, Results: []*values.Shape{text}}
	iface := &values.Shape{Type: values.NewType("host.Describer", values.Pair), Kind: values.InterfaceKind, Cells: 1, Methods: []values.Method{{ID: "Describe", Signature: sig}}}
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Number", Shape: named}, {Name: "Object", Shape: object}, {Shape: pointer}, {Name: "Describer", Shape: iface}}, Methods: []evm.ExternalMethod{
		{Name: "Describe", Receiver: named.Type, Signature: evm.Signature{Results: []*values.Type{values.StringType}}, Call: func(c *evm.Call) error {
			n := c.R[0]
			if n == 7 {
				c.SetValue(0, values.String("seven"))
			} else {
				c.SetValue(0, values.String("other"))
			}
			return nil
		}},
		{Name: "Describe", Receiver: pointer.Type, Signature: evm.Signature{Results: []*values.Type{values.StringType}}, Call: func(c *evm.Call) error {
			if c.P[0] == nil {
				c.SetValue(0, values.String("nil"))
			} else {
				c.SetValue(0, values.String("object"))
			}
			return nil
		}},
	}, Functions: []evm.ExternalFunction{
		{Name: "Make", Signature: evm.Signature{Results: []*values.Type{iface.Type}}, DynamicTypes: []*values.Type{named.Type}, Call: func(c *evm.Call) error { return c.SetInterface(0, named.Type, values.Int(7)) }},
		{Name: "Nil", Signature: evm.Signature{Results: []*values.Type{iface.Type}}, DynamicTypes: []*values.Type{pointer.Type}, Call: func(c *evm.Call) error { return c.SetInterface(0, pointer.Type, values.Value{}) }},
		{Name: "Read", Signature: evm.Signature{Params: []*values.Type{values.AnyType}, Results: []*values.Type{values.IntType}}, Call: func(c *evm.Call) error {
			t, v, err := c.Interface(0)
			if err != nil {
				return err
			}
			if t != named.Type {
				tname := "nil"
				if t != nil {
					tname = t.Name()
				}
				return fmt.Errorf("wrong type: %s", tname)
			}
			c.R[0] = v.R
			return nil
		}},
	}}
	m, err := evmcompiler.Compile("main.go", `import "h";func Run()bool{a:=h.Make();if a.Describe()!="seven"||h.Read(a)!=7{return false};if h.Nil().Describe()!="nil"{return false};v:=h.Number(7);if v.Describe()!="seven"||h.Number.Describe(v)!="seven"{return false};f:=v.Describe;if f()!="seven"{return false};o:=h.Object{N:3};var b h.Describer=&o;return b.Describe()=="object" && a.(h.Number)==7}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("host interface: %v %v", err, out)
	}
}
func TestGoNativeInterfacePairPayload(t *testing.T) {
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Text", Signature: evm.Signature{Results: []*values.Type{values.AnyType}}, DynamicTypes: []*values.Type{values.StringType}, Call: func(c *evm.Call) error { return c.SetInterface(0, values.StringType, values.String("retained text")) }}, {Name: "GC", Call: func(*evm.Call) error { runtime.GC(); return nil }}}}
	m, err := evmcompiler.Compile("main.go", `import "h";func Run()bool{x:=h.Text();h.GC();return x.(string)=="retained text"}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("pair interface: %v %v", err, out)
	}
}

func TestGoInterfaceNominalTypesAndMixedLayouts(t *testing.T) {
	vm := compileGoVM(t, `type A int;type B int;type Wrong struct{};func(Wrong)F(B)B{return 0};type Wanted interface{F(A)A}
type Text string;func(t Text)Join(n int,s string,p *int)(string,int){*p+=n;return string(t)+s,*p}
func Run()bool{var x any=Wrong{};if _,ok:=x.(Wanted);ok{return false};var t interface{Join(int,string,*int)(string,int)}=Text("a");n:=2;s,v:=t.Join(3,"b",&n);f:=t.Join;s2,v2:=f(4,"c",&n);return s=="ab"&&v==5&&s2=="ac"&&v2==9}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("nominal types/layouts: %v %v", err, out)
	}
}
func TestGoInterfaceBlankFieldStillUnhashable(t *testing.T) {
	for _, src := range []string{`func Run(){m:=map[any]int{};m[struct{_ []int}{}]=1}`, `func Run(){var m map[any]int;delete(m,struct{_ []int}{})}`} {
		vm := compileGoVM(t, src)
		if err := vm.Run(context.Background(), "Run", nil, nil); err == nil {
			t.Fatal("accepted unhashable blank field")
		}
	}
}
func TestGoNativeMethodPackageIdentity(t *testing.T) {
	scalar := &values.Shape{Type: values.NewType("a.T", values.Raw), Kind: values.IntKind, Bits: 64, Cells: 1}
	sig := &values.Shape{Type: values.NewType("hidden signature", values.Pair), Kind: values.FunctionKind, Cells: 1}
	iface := &values.Shape{Type: values.NewType("b.I", values.Pair), Kind: values.InterfaceKind, Cells: 1, Methods: []values.Method{{ID: "b.hidden", Signature: sig}}}
	a := evm.Package{Path: "a", Types: []evm.TypeDecl{{Name: "T", Shape: scalar}}, Methods: []evm.ExternalMethod{{Name: "hidden", Receiver: scalar.Type, Call: func(*evm.Call) error { return nil }}}}
	b := evm.Package{Path: "b", Types: []evm.TypeDecl{{Name: "I", Shape: iface}}}
	m, err := evmcompiler.Compile("main.go", `import("a";"b");func Run()bool{var x any=a.T(3);_,ok:=x.(b.I);return !ok}`, a, b)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, a, b)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("package method identity: %v %v", err, out)
	}
}
func TestGoInvalidNativeTypeDeclarations(t *testing.T) {
	for _, shape := range []*values.Shape{
		{Type: values.NewType("bad raw", values.Raw), Kind: values.StringKind, Cells: 1},
		{Type: values.NewType("bad fields", values.Pointer), Kind: values.StructKind, Cells: 1},
		{Type: values.NewType("bad pointer", values.Pointer), Kind: values.PointerKind, Cells: 1},
		{Type: values.NewType("bad numeric", values.Raw), Kind: values.IntKind, Cells: 1, Bits: 17},
	} {
		if _, err := evmcompiler.Compile("main.go", `import "h";func Run(){var _ h.T}`, evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "T", Shape: shape}}}); err == nil {
			t.Fatalf("accepted invalid shape: %#v", shape)
		}
	}
}

func TestGoNativeErrorAndInvalidInterfaceResult(t *testing.T) {
	typ := values.NewType("h.ErrorText", values.Pair)
	shape := &values.Shape{Type: typ, Kind: values.StringKind, Cells: 1}
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "ErrorText", Shape: shape}}, Methods: []evm.ExternalMethod{{Name: "Error", Receiver: typ, Signature: evm.Signature{Results: []*values.Type{values.StringType}}, Call: func(c *evm.Call) error { return nil }}}, Functions: []evm.ExternalFunction{{Name: "Error", Signature: evm.Signature{Results: []*values.Type{values.ErrorType}}, DynamicTypes: []*values.Type{typ, values.IntType}, Call: func(c *evm.Call) error { return c.SetInterface(0, typ, values.String("failed")) }}}}
	source := `import "h";func Run()bool{err:=h.Error();return err!=nil&&err.Error()=="failed"}`
	m, err := evmcompiler.Compile("main.go", source, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("native error: %v %v", err, out)
	}
	pkg.Functions[0].Call = func(c *evm.Call) error { return c.SetInterface(0, values.IntType, values.Int(3)) }
	p, err = evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err == nil {
		t.Fatal("returned non-error as error interface")
	}
}
func TestGoInterfaceLinkValidationAndSnapshot(t *testing.T) {
	source := `type T int;func(t T)M(x string)string{return x};func Run()string{var a interface{M(string)string}=T(3);return a.M("ok")}`
	for _, mutate := range []func(*evm.Module){
		func(m *evm.Module) { m.Types[0] = m.Types[1] },
		func(m *evm.Module) { m.Types[1].Methods[0].Function.Index = 100000 },
		func(m *evm.Module) { m.Types[1].Methods[0].Signature.Params[0] = values.IntType },
		func(m *evm.Module) { m.Invokes[0].Method.ID = "" },
		func(m *evm.Module) {
			for _, f := range m.Functions {
				for pc, ins := range f.Code {
					if ins.Op() == evm.OpCallVirt {
						f.Code[pc] = evm.WithBC(evm.OpCallVirt, 65535, ins.BC())
					}
				}
			}
		},
		func(m *evm.Module) { m.Types = append(m.Types, m.Types[1]) },
		func(m *evm.Module) {
			s := &values.Shape{Type: values.NewType("invalid recursive storage", values.Pointer), Kind: values.StructKind}
			s.Fields = []values.Field{{Name: "Recursive", Shape: s}}
			m.Types[1].Shape = s
		},
		func(m *evm.Module) { m.Types[1].Methods = append(m.Types[1].Methods, m.Types[1].Methods[0]) },
	} {
		m, err := evmcompiler.Compile("main.go", source)
		if err != nil {
			t.Fatal(err)
		}
		mutate(m)
		if _, err := evm.Link(m); err == nil {
			t.Fatal("accepted invalid interface metadata")
		}
	}
	m, err := evmcompiler.Compile("main.go", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	m.Types[1].Methods[0].Function.Index = 100000
	m.Invokes[0].Method.Signature.Params[0] = nil
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].AsString() != "ok" {
		t.Fatalf("linked snapshot: %v %v", err, out)
	}
}

func TestGoNativeStructuralTypeIdentityConflict(t *testing.T) {
	typ := values.NewType("duplicate int64", values.Raw)
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Shape: &values.Shape{Type: typ, Kind: values.IntKind, Bits: 64, Cells: 1}}}, Functions: []evm.ExternalFunction{{Name: "F"}}}
	if _, err := evmcompiler.Compile("main.go", `import "h";func Run(){h.F()}`, pkg); err == nil {
		t.Fatal("replaced builtin int64 identity with unnamed native type")
	}
}

func TestGoPointerInterfaceCallDoesNotBox(t *testing.T) {
	vm := compileGoVM(t, `type T struct{};func(*T)M()int{return 7};func Run(p *T)int{var x interface{M()int}=p;return x.M()}`)
	args := []values.Value{{}}
	out := make([]values.Value, 1)
	ctx := context.Background()
	if err := vm.Run(ctx, "Run", args, out); err != nil || out[0].R != 7 {
		t.Fatalf("typed nil method: %v %v", err, out)
	}
	if n := testing.AllocsPerRun(100, func() {
		if err := vm.Run(ctx, "Run", args, out); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("pointer interface allocates: %g", n)
	}
}

func TestGoInterfaceBudgetCountsInstructions(t *testing.T) {
	var methods, implementations strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&methods, "M%d();", i)
		fmt.Fprintf(&implementations, "func(T)M%d(){};", i)
	}
	source := "type T int;type I interface{" + methods.String() + "};" + implementations.String() + `func Make()any{return T(1)};func Run(v any)bool{_,ok:=v.(I);return ok}`
	vm := compileGoVM(t, source)
	arg := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Make", nil, arg); err != nil {
		t.Fatal(err)
	}
	vm.Limits.Steps = 30
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", arg, out); err != nil || out[0].R != 1 {
		t.Fatalf("100-method assertion: %v, %v", out, err)
	}
	params := make([]string, 100)
	args := make([]string, 100)
	for i := range params {
		params[i] = fmt.Sprintf("p%d int", i)
		args[i] = "x"
	}
	sig := strings.Join(params, ",")
	source = "type T int;type I interface{M(" + sig + ")int};func(T)M(" + sig + ")int{return p0};func Make()I{return T(1)};func Run(v I,x int)int{return v.M(" + strings.Join(args, ",") + ")}"
	vm = compileGoVM(t, source)
	if err := vm.Run(context.Background(), "Make", nil, arg); err != nil {
		t.Fatal(err)
	}
	vm.Limits.Steps = 30
	if err := vm.Run(context.Background(), "Run", []values.Value{arg[0], values.Int(2)}, make([]values.Value, 1)); !errors.Is(err, evm.ErrSteps) {
		t.Fatalf("100 argument moves should exhaust 30 instructions: %v", err)
	}
}
func TestGoConflictingNestedNativeShape(t *testing.T) {
	bad := &values.Shape{Type: values.Int64, Kind: values.FloatKind, Bits: 64, Cells: 1}
	outer := &values.Shape{Type: values.NewType("h.Outer", values.Pointer), Kind: values.StructKind, Cells: 1, Fields: []values.Field{{Name: "X", Shape: bad}}}
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Outer", Shape: outer}}}
	if _, err := evmcompiler.Compile("main.go", `import "h";func Run(){_ = h.Outer{}}`, pkg); err == nil {
		t.Fatal("accepted conflicting nested native field layout")
	}
}
func TestGoLinkRejectsForeignNativeMethodReceiver(t *testing.T) {
	typ := values.NewType("foreign", values.Raw)
	sig := evm.Signature{Params: []*values.Type{typ}}
	m := &evm.Module{Imports: []evm.Import{{Package: "h", Name: "M", Receiver: typ, Signature: sig}}, Functions: []evm.Function{{Name: "Run", Code: []evm.Instruction{evm.ABC(evm.OpReturn, 0, 0, 0)}}}}
	pkg := evm.Package{Path: "h", Methods: []evm.ExternalMethod{{Name: "M", Receiver: typ, Call: func(*evm.Call) error { return nil }}}}
	if _, err := evm.Link(m, pkg); err == nil {
		t.Fatal("linked undeclared method receiver")
	}
}
func TestGoNativeInterfacePayloadMatrix(t *testing.T) {
	number := &values.Shape{Type: values.IntType, Kind: values.IntKind, Bits: 64, Cells: 1}
	slice := &values.Shape{Type: values.NewType("[]int", values.Pair), Kind: values.SliceKind, Cells: 1, Elem: number}
	function := &values.Shape{Type: values.NewType("func()int", values.Pair), Kind: values.FunctionKind, Cells: 1, Results: []*values.Shape{number}}
	pointer := &values.Shape{Type: values.NewType("*int", values.Pointer), Kind: values.PointerKind, Cells: 1, Elem: number}
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Shape: slice}, {Shape: function}, {Shape: pointer}}, Functions: []evm.ExternalFunction{
		{Name: "Slice", Signature: evm.Signature{Results: []*values.Type{values.AnyType}}, DynamicTypes: []*values.Type{slice.Type}, Call: func(c *evm.Call) error {
			cells := []values.Value{values.Int(7), values.Int(8)}
			return c.SetInterface(0, slice.Type, values.SliceValue(unsafe.Pointer(&cells[0]), 2, 2))
		}},
		{Name: "Pointer", Signature: evm.Signature{Results: []*values.Type{values.AnyType}}, DynamicTypes: []*values.Type{pointer.Type}, Call: func(c *evm.Call) error {
			p := new(values.Value)
			*p = values.Int(9)
			return c.SetInterface(0, pointer.Type, values.Value{P: unsafe.Pointer(p)})
		}},
		{Name: "Function", Signature: evm.Signature{Params: []*values.Type{function.Type}, Results: []*values.Type{values.AnyType}}, DynamicTypes: []*values.Type{function.Type}, Call: func(c *evm.Call) error { return c.SetInterface(0, function.Type, c.Value(0)) }},
		{Name: "Nil", Signature: evm.Signature{Results: []*values.Type{values.AnyType}}, Call: func(c *evm.Call) error { return c.SetInterface(0, nil, values.Int(99)) }},
		{Name: "GC", Call: func(*evm.Call) error { runtime.GC(); return nil }},
	}}
	m, err := evmcompiler.Compile("main.go", `import "h";func Run()bool{n:=6;s,p,f:=h.Slice(),h.Pointer(),h.Function(func()int{return n});h.GC();return s.([]int)[1]==8&&*p.(*int)==9&&f.(func()int)()==6&&h.Nil()==nil}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("native payload matrix: %v %v", err, out)
	}
}

func TestGoNativeInterfaceChecksDoNotUseGuestSteps(t *testing.T) {
	methodSig := &values.Shape{Type: values.NewType("func()", values.Pair), Kind: values.FunctionKind, Cells: 1}
	iface := &values.Shape{Type: values.NewType("h.I", values.Pair), Kind: values.InterfaceKind, Cells: 1}
	var methods strings.Builder
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("M%d", i)
		iface.Methods = append(iface.Methods, values.Method{ID: name, Signature: methodSig})
		fmt.Fprintf(&methods, "func(T)%s(){};", name)
	}
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "I", Shape: iface}}, Functions: []evm.ExternalFunction{{Name: "Rewrap", Signature: evm.Signature{Params: []*values.Type{values.AnyType}, Results: []*values.Type{iface.Type}}, Call: func(c *evm.Call) error {
		typ, v, err := c.Interface(0)
		if err != nil {
			return err
		}
		return c.SetInterface(0, typ, v)
	}}}}
	source := `import "h";type T int;` + methods.String() + `func Make()any{return T(1)};func Run(v any)h.I{return h.Rewrap(v)}`
	m, err := evmcompiler.Compile("main.go", source, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	v := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Make", nil, v); err != nil {
		t.Fatal(err)
	}
	vm.Limits.Steps = 30
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", v, out); err != nil || out[0].R != v[0].R {
		t.Fatalf("native interface result: %v, %v", out, err)
	}
}

func TestInvokeSharesMethodAcrossReceivers(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `type Counter interface { Add(string, int) int }
type N int
func (n N) Add(s string, x int) int { return int(n) + len(s) + x }
func both(a, b Counter) int { defer b.Add("deferred", 0); return a.Add("a", 2)*100 + b.Add("bbbb", 3) }
func Run() int { return both(N(10), N(20)) }`)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Invokes) != 1 {
		t.Fatalf("got %d invoke descriptors, want one", len(m.Invokes))
	}
	if listing := m.Disassemble(); strings.Count(listing, "mth0  ; plugin.Counter.Add") != 2 {
		t.Fatalf("expected two virtual calls through mth0:\n%s", listing)
	}
	prog, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	var out [1]values.Value
	if err := evm.New(prog).Run(context.Background(), "Run", nil, out[:]); err != nil || out[0].R != 1327 {
		t.Fatalf("Run: %v, %v", out, err)
	}
}
