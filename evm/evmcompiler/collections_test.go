package evmcompiler_test

import (
	"context"
	"errors"
	"math"
	"runtime"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func compileGoVM(t testing.TB, source string) *evm.VM {
	t.Helper()
	m, err := evmcompiler.Compile("main.go", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	return evm.New(p)
}
func TestGoAggregateCopiesAndPointers(t *testing.T) {
	vm := compileGoVM(t, `type Inner struct{N int; Text string}
type Record struct{Inner Inner; Items [2]int; P *int}
func change(r Record)Record {r.Inner.N=20;r.Items[0]=30;*r.P=40;return r}
func Run()int {
 n:=1;a:=Record{Inner:Inner{N:2,Text:"hello"},Items:[2]int{3,4},P:&n}
 b:=change(a);p:=&a.Inner.N;*p=5
 x:=a.Items;y:=x;y[1]=99
 if a.Inner.Text!="hello" || b.Inner.Text!="hello" {return -1}
 return a.Inner.N+b.Inner.N+a.Items[0]+b.Items[0]+x[1]+y[1]+n
}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 201 {
		t.Fatalf("copy/alias result %v", out)
	}
}
func TestGoSlicesAndStrings(t *testing.T) {
	vm := compileGoVM(t, `func Run()int {
 a:=make([]int,2,5);a[0]=1;a[1]=2;b:=a[:1];b=append(b,7,8)
 if a[1]!=7 || len(b)!=3 || cap(b)!=5 {return -1}
 c:=b[:2:2];c=append(c,9);c[0]=10;if a[0]!=1 {return -2}
 copy(b[1:],b[:2]);clear(b[2:]);if b[1]!=1 || b[2]!=0{return -3}
 text:="aé\xff";total:=0;for i,r:=range text {total+=i+int(r)}
 bytes:=[]byte(text);bytes[0]='z';if text[0]!='a' {return -4}
 runes:=[]rune("世界");runes[1]='!';if string(runes)!="世!" {return -5}
 if string(bytes[:1])!="z" || string(0x1f600)!="😀" {return -6}
 extra:=append([]byte{'x'},"é"...);if string(extra)!="xé"{return -8};if copy(extra,"ab")!=2||string(extra[:2])!="ab"{return -9}
 var empty []int;if empty!=nil || make([]int,0)==nil {return -7}
 return total
}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 97+1+233+3+65533 {
		t.Fatalf("slice/string result %v", out)
	}
}
func TestGoMapsAndMutation(t *testing.T) {
	vm := compileGoVM(t, `type Key struct{N int; S string}
func Run()int {
 hint:=-1;empty:=make(map[Key]int,hint);if len(empty)!=0{return -4}
 m:=map[Key]int{{1,"a"}:3,{2,"b"}:4};k:=Key{1,"a"};m[k]++;k.N=9
 if v,ok:=m[Key{1,"a"}];!ok || v!=4{return -1}
 delete(m,Key{2,"b"});m[Key{3,"c"}]=6
 total:=0;for _,v:=range m{total+=v};clear(m);if len(m)!=0{return -2}
 var nilmap map[string]int;if v,ok:=nilmap["x"];ok || v!=0{return -3}
 return total
}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 10 {
		t.Fatalf("map result %v", out)
	}
}

func TestGoGlobalsAndInitialization(t *testing.T) {
	vm := compileGoVM(t, `var a=b+1;var b=4;var items=[]int{a};func init(){a+=2};func init(){a*=3};func Run()int{a++;items[0]++;return a*100+items[0]}`)
	out := make([]values.Value, 1)
	for _, want := range []int64{2206, 2307} {
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil {
			t.Fatal(err)
		}
		if out[0].AsInt() != want {
			t.Fatalf("global result %d want %d", out[0].AsInt(), want)
		}
	}
}

func TestGoCollectionEdges(t *testing.T) {
	vm := compileGoVM(t, `type Node struct{Next *Node; N int};type Empty struct{};type Tail struct{N int; E Empty}
func Run(z float64)int{
 root:=&Node{N:4};root.Next=root;if root.Next.N!=4{return -1}
 tail:=&Tail{N:3};e:=&tail.E;*e=Empty{}
 a:=[3]int{1,2,3};b:=a;if a!=b{return -2};b[0]=9;if a==b{return -3}
 s:=a[:];p:=(*[3]int)(s);p[1]=8;if a[1]!=8{return -4}
 m:=make(map[float64]int);m[0]=1;m[-z]=2;if len(m)!=1||m[0]!=2{return -5}
 nan:=z/z;m[nan]=3;m[nan]=4;if len(m)!=3{return -6}
 n:=0;total:=0;for k,v:=range m{if k!=k{n++;total+=v}};if n!=2||total!=7{return -7}
 mutable:=map[int]int{1:1,2:2,3:3};for k:=range mutable{delete(mutable,k)};if len(mutable)!=0{return -8}
 items:=[]Empty{{},{}};items=append(items,Empty{});if len(items)!=3{return -9}
 var nils []int;if (*[0]int)(nils)!=nil{return -10}
 return tail.N+a[0]+a[1]+a[2]
}
func Complex()complex128{x:=complex(2.0,3.0);y:=complex128(complex64(x));return -x*y+complex(real(x),imag(y))}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", []values.Value{values.Float(0)}, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 15 {
		t.Fatalf("collection edges: %v", out)
	}
	// Use a scalar result to keep the test independent of aggregate storage ABI.
	cvm := compileGoVM(t, `func Run()float64{x:=complex(2.0,3.0);y:=complex128(complex64(x));z:=-x*y+complex(real(x),imag(y));return real(z)+imag(z)}`)
	if err := cvm.Run(context.Background(), "Run", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsFloat() != -2 {
		t.Fatalf("complex result: %v", out)
	}
}

func TestGoCollectionBoundsAndBudgets(t *testing.T) {
	for _, tc := range []struct {
		source string
		want   error
	}{
		{`func Run(){var p *int;_ = *p}`, evm.ErrNil},
		{`func Run(){a:=[]int{1};_ = a[-int(a[0])]}`, evm.ErrBounds},
		{`func Run(){a:=[]int{1};_ = a[:2]}`, evm.ErrBounds},
		{`func Run(){a:=[]int{1};_ = (*[2]int)(a)}`, evm.ErrBounds},
		{`func Run(){var m map[string]int;m["x"]=1}`, evm.ErrNilMap},
		{`func Run(){n:=int64(1)<<32;_ = make([]byte,n)}`, evm.ErrBounds},
		{`func Run(){n:=10000000;_ = make([]int,n)}`, evm.ErrAllocation},
	} {
		vm := compileGoVM(t, tc.source)
		if err := vm.Run(context.Background(), "Run", nil, nil); !errors.Is(err, tc.want) {
			t.Fatalf("%s: %v want %v", tc.source, err, tc.want)
		}
		if err := vm.Run(context.Background(), "Run", nil, nil); !errors.Is(err, evm.ErrInvalidState) {
			t.Fatalf("failed session still usable: %v", err)
		}
	}
	vm := compileGoVM(t, `type A [1<<50]struct{};func Run()bool{var a,b A;return a==b}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
		t.Fatalf("zero-sized giant array: %v %v", err, out)
	}
}
func TestGoSessionResetAndIsolation(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `var n=1;func init(){n+=2};func Run()int{n++;return n}`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	a, b := evm.New(p), evm.New(p)
	for _, name := range []string{"init", "init#1"} {
		if err := a.Run(context.Background(), name, nil, nil); err == nil {
			t.Fatalf("public init %s", name)
		}
	}
	out := make([]values.Value, 1)
	for _, vm := range []*evm.VM{a, b} {
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 4 {
			t.Fatalf("session: %v %v", err, out)
		}
	}
	if err := a.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 5 {
		t.Fatalf("second call: %v %v", err, out)
	}
	if err := a.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := a.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 4 {
		t.Fatalf("reset: %v %v", err, out)
	}
	bad := compileGoVM(t, `var a=1;var b=a/(a-1);func Run()int{return b}`)
	if err := bad.Run(context.Background(), "Run", nil, out); !errors.Is(err, evm.ErrDivideByZero) {
		t.Fatalf("init failure: %v", err)
	}
	if err := bad.Run(context.Background(), "Run", nil, out); !errors.Is(err, evm.ErrInvalidState) {
		t.Fatalf("init state: %v", err)
	}
}
func TestGoRuntimeComplexAndMinMax(t *testing.T) {
	vm := compileGoVM(t, `func Run(a,b float64)float64{x:=complex(a,b);y:=complex128(complex64(x));z:=-x*y+complex(real(x),imag(y));return real(z)+imag(z)}
func Min(a,b float64)float64{return min(a,b)}
func Max(a,b float64)float64{return max(a,b)}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", []values.Value{values.Float(2), values.Float(3)}, out); err != nil || out[0].AsFloat() != -2 {
		t.Fatalf("complex: %v %v", err, out)
	}
	for _, name := range []string{"Min", "Max"} {
		if err := vm.Run(context.Background(), name, []values.Value{values.Float(math.NaN()), values.Float(3)}, out); err != nil || !math.IsNaN(out[0].AsFloat()) {
			t.Fatalf("NaN %s: %v %v", name, err, out)
		}
	}
	if err := vm.Run(context.Background(), "Min", []values.Value{values.Float(0), values.Float(math.Copysign(0, -1))}, out); err != nil || !math.Signbit(out[0].AsFloat()) {
		t.Fatalf("negative zero: %v %v", err, out)
	}
}

func TestGoExtendedDescriptorsValidateAndSnapshot(t *testing.T) {
	// Exercise the cell-backed ABI descriptors, including their operands and shape table.
	source := `func Data()[]int{return []int{3}};func Run()int{return Data()[0]}`
	for _, mutate := range []func(*evm.Module){
		func(m *evm.Module) { m.ExtendedOps[0].In[0].Slot = 65535 },
		func(m *evm.Module) { m.ExtendedOps[0].Kind = 255 },
		func(m *evm.Module) { m.ExtendedOps[0].Shape.Cells++ },
		func(m *evm.Module) { m.ExtendedOps[0].In = nil },
		func(m *evm.Module) { m.Shapes = nil },
	} {
		m, err := evmcompiler.Compile("main.go", source)
		if err != nil {
			t.Fatal(err)
		}
		mutate(m)
		if _, err := evm.Link(m); err == nil {
			t.Fatal("accepted invalid extended descriptor")
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
	m.ExtendedOps[0].In[0].Slot = 65535
	m.ExtendedOps[0].Shape.Cells = 0
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 3 {
		t.Fatalf("snapshot: %v %v", err, out)
	}
}

func TestGoCollectionPointersSurviveGC(t *testing.T) {
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "GC", Call: func(*evm.Call) error { runtime.GC(); return nil }}}}
	m, err := evmcompiler.Compile("main.go", `import "h";type Node struct{N int;Next *Node};func Run()int{head:=&Node{N:9};for i:=0;i<20;i++{head=&Node{N:i,Next:head};h.GC()};sum:=0;for p:=head;p!=nil;p=p.Next{sum+=p.N};return sum}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 199 {
		t.Fatalf("GC: %v %v", err, out)
	}
}

func TestGoAggregateIndexBudgetIsIndependentOfSize(t *testing.T) {
	vm := compileGoVM(t, `func Run(a [1][1024]int)[1024]int{return a[0]}`)
	vm.Limits.Steps = 32
	if err := vm.Run(context.Background(), "Run", []values.Value{{}}, make([]values.Value, 1)); err != nil {
		t.Fatalf("aggregate indexing: %v", err)
	}
}
