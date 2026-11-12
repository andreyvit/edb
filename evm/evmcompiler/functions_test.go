package evmcompiler_test

import (
	"context"
	"errors"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"runtime"
	"testing"

	"github.com/andreyvit/edb/values"
)

func TestGoClosuresAndFunctionValues(t *testing.T) {
	vm := compileGoVM(t, `func counter(n int)func(int)int{return func(delta int)int{n+=delta;return n}}
func apply(f func(int)int,n int)int{return f(n)}
func sum(ns ...int)int{n:=0;for _,x:=range ns{n+=x};return n}
func Run()int{
 a,b:=counter(10),counter(20);n:=apply(a,2)+b(3)+a(4)
 x:=7;f:=func()int{return x};x=9;n+=f()
 fs:=[]func()int{};for i:=0;i<3;i++{fs=append(fs,func()int{return i})};for _,f:=range fs{n+=f()}
 direct:=func(a int)int{return a+x}(2)
 var rec func(int)int;rec=func(n int)int{if n<2{return 1};return n*rec(n-1)}
 args:=[]int{2,3};n+=sum(1,2,3)+sum(args...)+direct+rec(4)
 return n
}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 109 {
		t.Fatalf("closures: %v", out)
	}
}
func TestGoMethodsAndGenericInstances(t *testing.T) {
	vm := compileGoVM(t, `type Number struct{N int};func(n Number)Add(x int)int{return n.N+x};func(n *Number)Bump(x int)int{n.N+=x;return n.N}
type Outer struct{Number}
func fold[T ~int|~string](a,b T)T{return a+b}
func makeGetter[T any](v T)func()T{return func()T{return v}}
type Box[T any]struct{V T};func(b Box[T])Get()T{return b.V}
func Run()int{
 n:=Number{5};value:=n.Add;pointer:=n.Bump;n.N=10
 out:=value(2)+pointer(3)+Number.Add(n,4)+(*Number).Bump(&n,2)
 o:=Outer{Number{9}};out+=o.Add(1)
 if fold("a","b")!="ab"{return -1};out+=fold(2,3)
 getter:=makeGetter(Box[int]{7});out+=getter().Get();if (Box[string]{"ok"}).Get()!="ok"{return -2}
 return out
}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 74 {
		t.Fatalf("methods/generics: %v", out)
	}
}

func TestGoNilFunctionsAndMethodReceivers(t *testing.T) {
	for _, source := range []string{
		`func Run(){var f func();f()}`,
		`type T int;func(T)Value()int{return 1};func Run(){var p *T;_ = (*T).Value(p)}`,
	} {
		vm := compileGoVM(t, source)
		err := vm.Run(context.Background(), "Run", nil, nil)
		if !errors.Is(err, evm.ErrNilFunction) && !errors.Is(err, evm.ErrNil) {
			t.Fatalf("nil call: %v", err)
		}
	}
	vm := compileGoVM(t, `type T struct{};func(p *T)Valid()bool{return p==nil};func F(){};func Run()bool{var p *T;var nilf func();f:=F;return p.Valid()&&f!=nil&&nilf==nil}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("nil receiver/comparison: %v %v", err, out)
	}
}
func TestGoNativeFunctionValueAndClosureGC(t *testing.T) {
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{
		{Name: "Double", Signature: evm.Signature{Params: []*values.Type{values.Int64}, Results: []*values.Type{values.Int64}}, Call: func(c *evm.Call) error { c.R[0] *= 2; return nil }},
		{Name: "GC", Call: func(*evm.Call) error { runtime.GC(); return nil }},
	}}
	m, err := evmcompiler.Compile("main.go", `import "h";func apply(f func(int64)int64,x int64)int64{return f(x)}
func makeFunc(text string)func()string{return func()string{h.GC();return text}}
func Run()int64{f:=makeFunc("retained"+" text");h.GC();if f()!="retained text"{return 0};return apply(h.Double,21)}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 42 {
		t.Fatalf("function value: %v %v", err, out)
	}
}
func TestGoGenericRecursionAndClosureGlobals(t *testing.T) {
	vm := compileGoVM(t, `func fact[T ~int|~int64](n T)T{if n<2{return 1};return n*fact(n-1)}
func counter()func()int{x:=0;return func()int{x++;return x}}
var f=counter()
func Run()int{return f()+fact(5)+int(fact(int64(4)))}`)
	out := make([]values.Value, 1)
	for _, want := range []uint64{145, 146} {
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != want {
			t.Fatalf("generic/global closure: %v %v", err, out)
		}
	}
}

func TestGoFunctionLinkValidationAndSnapshot(t *testing.T) {
	source := `func factory()func(int)int{x,y:=2,3;return func(n int)int{x+=n;return x+y}};func Run()int{f:=factory();return f(4)}`
	for _, mutate := range []func(*evm.Module){
		func(m *evm.Module) { m.FunctionRefs[0].Index = 100000 },
		func(m *evm.Module) { m.Closures[0].Function = 100000 },
		func(m *evm.Module) { m.Closures[0].Captures[0].Slot = 65535 },
		func(m *evm.Module) { m.Closures[0].Captures = nil },
		func(m *evm.Module) {
			for i := range m.Functions {
				f := &m.Functions[i]
				if len(f.Captures) > 0 {
					f.Captures[0].Slot = 65535
					return
				}
			}
		},
		func(m *evm.Module) {
			for i := range m.Functions {
				f := &m.Functions[i]
				if len(f.Captures) > 0 {
					f.Internal = false
					return
				}
			}
		},
		func(m *evm.Module) { m.Signatures[0].Params[0] = nil },
		func(m *evm.Module) {
			for i := range m.Functions {
				for pc, ins := range m.Functions[i].Code {
					if ins.Op() == evm.OpClosure {
						m.Functions[i].Code[pc] = evm.WithBC(evm.OpFunction, ins.A(), m.Closures[ins.BC()].Function)
						return
					}
				}
			}
		},
	} {
		m, err := evmcompiler.Compile("main.go", source)
		if err != nil {
			t.Fatal(err)
		}
		mutate(m)
		if _, err := evm.Link(m); err == nil {
			t.Fatal("accepted invalid function metadata")
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
	m.FunctionRefs[0].Index = 100000
	m.Closures[0].Captures[0].Slot = 65535
	m.Signatures[0].Params[0] = nil
	for i := range m.Functions {
		if len(m.Functions[i].Captures) > 0 {
			m.Functions[i].Captures[0].Slot = 65535
		}
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 9 {
		t.Fatalf("snapshot: %v %v", err, out)
	}
}

func TestGoNestedClosureChain(t *testing.T) {
	vm := compileGoVM(t, `func outer(n int)func(int)func(int)int{return func(a int)func(int)int{return func(b int)int{n+=a+b;return n}}};func Run()int{outer:=outer(10);a,b:=outer(2),outer(3);return a(4)+b(5)+a(1)}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 67 {
		t.Fatalf("nested closures: %v %v", err, out)
	}
}
func TestGoUnusedMethodIsCompiled(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `type T int;func(t T)Unused()int{return int(t)+1};func Run(){}`)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for i := range m.Functions {
		f := &m.Functions[i]
		if len(f.Signature.Params) == 1 && len(f.Signature.Results) == 1 {
			found = true
			if !f.Internal || len(f.Code) == 0 {
				t.Fatalf("method was not compiled: %v", f)
			}
		}
	}
	if !found {
		t.Fatal("unused method omitted")
	}
	if len(m.FunctionRefs) != 0 {
		t.Fatalf("structural callees created function values: %v", m.FunctionRefs)
	}
}
func TestGoIndirectSignatureMismatch(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `func wrong(x string)string{return x};func right(x int)int{return x};func apply(f func(int)int)int{return f(2)};func Run()int{return apply(right)}`)
	if err != nil {
		t.Fatal(err)
	}
	for i := range m.Functions {
		if m.Functions[i].Name == "wrong" {
			m.FunctionRefs[0].Index = uint32(i)
		}
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := evm.New(p).Run(context.Background(), "Run", nil, make([]values.Value, 1)); !errors.Is(err, evm.ErrFunction) {
		t.Fatalf("signature mismatch: %v", err)
	}
}
func TestGoStructuralCalleeDoesNotCreateFunctionValue(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `func direct(x int)int{return x+1};func Run()int{x:=2;return direct(func()int{return x}())}`)
	if err != nil {
		t.Fatal(err)
	}
	for i := range m.Functions {
		for _, ins := range m.Functions[i].Code {
			if ins.Op() == evm.OpFunction {
				t.Fatal("unused function value for structural callee")
			}
		}
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := evm.New(p).Run(context.Background(), "Run", nil, make([]values.Value, 1)); err != nil {
		t.Fatal(err)
	}
}
