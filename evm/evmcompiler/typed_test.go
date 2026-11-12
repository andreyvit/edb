package evmcompiler_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestGoControlFlowAndFunctions(t *testing.T) {
	source := `package plugin
func fib(n int) int { if n<2 {return n}; return fib(n-1)+fib(n-2) }
func pair(a,b int)(int,int) {return b,a}
func Run(n int)(int,int) {
 total:=0
 for i:=0;i<n;i++ { if i%2!=0 {continue};total+=i;if total>100 {break} }
 a,b:=pair(total,fib(8))
 switch {case n<0:a=-1;case n==10:b+=3;fallthrough;default:b++}
 x,y:=1,2
 for i:=0;i<3;i++ {x,y=y,x}
 if false && fib(1000)>0 {a=0}
 if true || fib(1000)>0 {b+=x*10+y}
 goto done
 a=999
 done:
 return a,b
}`
	m, err := evmcompiler.Compile("main.go", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 2)
	if err := evm.New(p).Run(context.Background(), "Run", []values.Value{values.Int(10)}, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 21 || out[1].AsInt() != 45 {
		t.Fatalf("bad results: %+v", out)
	}
}

func TestGoScalarsAndConversions(t *testing.T) {
	for _, tc := range []struct {
		body string
		want values.Value
	}{
		{`var x int8=127; x++; return int64(x)`, values.Int(-128)},
		{`x:=float32(1.00000005960464477539062500000000001);if x>1{return 1};return 0`, values.Int(1)},
		{`var x uint8=255; x++; return int64(x)`, values.Int(0)},
		{`x:=int16(-2); return int64(x >> 1)`, values.Int(-1)},
		{`x:=uint32(0xffffffff); return int64(x >> 16)`, values.Int(65535)},
		{`x,y:=int32(7),int32(3); return int64(x &^ y)`, values.Int(4)},
		{`x:=float32(16777216); x++; return int64(x)`, values.Int(16777216)},
		{`x:=1.5; y:=-x; if y<=x && x>=y && y!=x {return int64(x*4)};return 0`, values.Int(6)},
		{`s:="hello"+" world";if s=="hello world" && "a"<s {return int64(len(s))};return 0`, values.Int(11)},
	} {
		t.Run(tc.body, func(t *testing.T) {
			m, err := evmcompiler.Compile("main.go", "func Run()int64{"+tc.body+"}")
			if err != nil {
				t.Fatal(err)
			}
			p, err := evm.Link(m)
			if err != nil {
				t.Fatal(err)
			}
			out := make([]values.Value, 1)
			if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil {
				t.Fatal(err)
			}
			if out[0] != tc.want {
				t.Fatalf("got %+v want %+v", out[0], tc.want)
			}
		})
	}
}

func TestGoRequiresImportsAndTypes(t *testing.T) {
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Ping", Call: func(*evm.Call) error { return nil }}}}
	for _, source := range []string{`func Run(){h.Ping()}`, `package p;import "h";func Run(){}`, `package p;import "unknown";func Run(){unknown.Ping()}`, `func Run(){x:=1;x="wrong"}`, `func Run(){x:=true;if x+1>0{}}`, `func Run(){var x int8=128;_ =x}`, `func Run(){ch:=make(chan int);go func(){ch<-1}();select{case <-ch:default:}}`, `func Run(){if false{go Run()}}`} {
		if m, err := evmcompiler.Compile("main.go", source, pkg); err == nil || m != nil {
			t.Fatalf("accepted %s", source)
		}
	}
	m, err := evmcompiler.Compile("main.go", `package p;import alias "h";func Run(){alias.Ping()}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := evm.New(p).Run(context.Background(), "Run", nil, nil); err != nil {
		t.Fatal(err)
	}
	_, err = evmcompiler.Compile("main.go", "//line fake.go:999\nfunc Run(){\n x:=unknown\n _=x\n}")
	if err == nil || !strings.Contains(err.Error(), "main.go:3:") {
		t.Fatalf("bad physical location: %v", err)
	}
}

func TestGoMixedBankCallsAndRuntimeLimits(t *testing.T) {
	source := `func swap(a,b string,n int)(string,int,string){return b,n+1,a}
func Run(s string,n int)(string,int,string){
 a,x,b:=swap(s,"second",n)
 for i:=0;i<2;i++ {a,x,b=swap(a,b,x)}
 return a,x,b
}

func Grow(){s:="x";for {s+=s}}
func Spin(){for {}}
func Shift(n int)int{return 1<<n}
func Compare(x float64)bool{return x<=x}
`
	m, err := evmcompiler.Compile("main.go", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 3)
	if err := vm.Run(context.Background(), "Run", []values.Value{values.String("first"), values.Int(7)}, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsString() != "second" || out[1].R != 10 || out[2].AsString() != "first" {
		t.Fatalf("bad mixed results: %+v", out)
	}
	vm.Limits = evm.Limits{AllocationBytes: 128, Steps: 1000}
	if err := vm.Run(context.Background(), "Grow", nil, nil); !errors.Is(err, evm.ErrAllocation) {
		t.Fatalf("allocation limit: %v", err)
	}
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := vm.Run(context.Background(), "Spin", nil, nil); !errors.Is(err, evm.ErrSteps) {
		t.Fatalf("fuel limit: %v", err)
	}
	out = make([]values.Value, 1)
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := vm.Run(context.Background(), "Shift", []values.Value{values.Int(-1)}, out); !errors.Is(err, evm.ErrNegativeShift) {
		t.Fatalf("negative shift: %v", err)
	}
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := vm.Run(context.Background(), "Compare", []values.Value{values.Float(math.NaN())}, out); err != nil || out[0].R != 0 {
		t.Fatalf("NaN comparison: %v %+v", err, out)
	}
}

func TestGoHostSignaturesAreSnapshots(t *testing.T) {
	params := []*values.Type{values.IntType}
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Take", Signature: evm.Signature{Params: params}, Call: func(c *evm.Call) error {
		if c.R[0] != 42 {
			t.Fatalf("argument: %d", c.R[0])
		}
		return nil
	}}}}
	m, err := evmcompiler.Compile("main.go", `import "h"; func Run(){h.Take(42)}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	params[0] = values.StringType
	if m.Imports[0].Signature.Params[0] != values.IntType {
		t.Fatal("retained caller signature")
	}
	params[0] = values.IntType
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := evm.New(p).Run(context.Background(), "Run", nil, nil); err != nil {
		t.Fatal(err)
	}
}
func FuzzTypedCompile(f *testing.F) {
	for _, s := range []string{`func Run()(n int){defer func(){if recover()!=nil{n=7}}();panic(1)}`, `func seq(y func(int)bool){y(1)};func Run(){for n:=range seq{defer func(){_ =n}()}}`, `type T int;func(t T)M()int{return int(t)};func Run(){var x any=T(2);_ = x.(interface{M()int}).M()}`, `func Run(){m:=map[any]int{1:2,"a":3};for k:=range m{delete(m,k)}}`, "func Run(){}", "func Run(n int)int{for i:=0;i<5;i++{n+=i};return n}", "func Run()string{s:=\"x\";for{ s+=s }}", "func Run(){go Run()}", `func Run(){n:=1;f:=func()int{n++;return n};_ =f()}`, `type T int;func(t T)F()int{return int(t)};func Run(){f:=T(2).F;_ =f()}`, `func id[T any](v T)T{return v};func Run(){_ =id(1)}`, `func Run(){s:=[]int{1,2,3};s=append(s,4);for i,v:=range s{s[i]=v+1}}`, `func Run(){m:=map[int]int{1:2};for k,v:=range m{m[k]=v+1}}`, `var x=1;func Run(){x++}`, `func Run(){type T struct{X int};a:=T{1};b:=a;_ =a==b}`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, source string) {
		m, err := evmcompiler.Compile("main.go", source)
		if err != nil {
			return
		}
		p, err := evm.Link(m)
		if err != nil {
			t.Fatalf("invalid compiled bytecode: %v", err)
		}
		for _, fn := range m.Functions {
			vm := evm.New(p)
			vm.Limits = evm.Limits{Steps: 1000, Depth: 32, AllocationBytes: 4096}
			args := make([]values.Value, len(fn.Signature.Params))
			out := make([]values.Value, len(fn.Signature.Results))
			_ = vm.Run(context.Background(), fn.Name, args, out)
		}
	})
}

func TestGoRejectsInvalidHostSignatures(t *testing.T) {
	for _, params := range [][]*values.Type{{nil}, make([]*values.Type, 65536)} {
		_, err := evmcompiler.Compile("main.go", `func Run(){}`, evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Bad", Signature: evm.Signature{Params: params}}}})
		if err == nil {
			t.Fatal("accepted invalid host signature")
		}
	}
}

func TestGoFloat32ConversionRoundsOnce(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `func Run(x uint64) float32{return float32(x)}`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	// Just above a float32 midpoint which float64 rounds back to the midpoint.
	x := uint64(1)<<63 | uint64(1)<<39 | 1
	if err := vm.Run(context.Background(), "Run", []values.Value{{R: x}}, out); err != nil {
		t.Fatal(err)
	}
	if out[0].R != uint64(math.Float32bits(float32(x))) {
		t.Fatalf("double rounding: %#x", out[0].R)
	}
}

func BenchmarkGoScalarRun(b *testing.B) {
	m, err := evmcompiler.Compile("main.go", `func Run(n int)int{sum:=0;for i:=0;i<n;i++{sum+=i};return sum}`)
	if err != nil {
		b.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		b.Fatal(err)
	}
	vm := evm.New(p)
	args := []values.Value{values.Int(10)}
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", args, out); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := vm.Run(context.Background(), "Run", args, out); err != nil {
			b.Fatal(err)
		}
	}
}

func TestGoIntegerArithmeticMatchesNative(t *testing.T) {
	m, err := evmcompiler.Compile("main.go", `func Signed(x,y int64) (int64,int64,int64,int64,int64){return x+y,x-y,x*y,x/y,x%y}
func Unsigned(x,y uint64) (uint64,uint64,uint64,uint64,uint64){return x+y,x-y,x*y,x/y,x%y}`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 5)
	bits := []uint64{0, 1, 2, 127, 128, 255, 256, 1 << 31, 1 << 32, 1<<63 - 1, 1 << 63, 1<<63 + 1, ^uint64(0)}
	for _, x := range bits {
		for _, y := range bits {
			if y == 0 {
				continue
			}
			for _, name := range []string{"Signed", "Unsigned"} {
				want := []uint64{x + y, x - y, x * y, x / y, x % y}
				if name == "Signed" {
					want[3] = uint64(int64(x) / int64(y))
					want[4] = uint64(int64(x) % int64(y))
				}
				if err := vm.Run(context.Background(), name, []values.Value{{R: x}, {R: y}}, out); err != nil {
					t.Fatal(err)
				}
				for i, v := range out {
					if v.R != want[i] {
						t.Fatalf("%s(%x,%x) result %d=%x want %x", name, x, y, i, v.R, want[i])
					}
				}
			}
		}
	}
}

func TestGoRejectsOversizedSourceSignature(t *testing.T) {
	// Caller sorts before callee; lowering must not index a failed call layout.
	source := "func A(){Z(" + strings.Repeat("0,", 65535) + "0)}\nfunc Z(" + strings.Repeat("_ int,", 65535) + "_ int){}"
	if m, err := evmcompiler.Compile("main.go", source); err == nil || m != nil {
		t.Fatal("accepted oversized source signature")
	}
}

func TestLoopSlotAndParallelCopyAliases(t *testing.T) {
	vm := compileGoVM(t, `func Run(n int) (int, string, int, string) {
		x, y := 1, 2
		a, b := "a", "b"
		total := 0
		for i := 0; i < n; i++ {
			fresh := x + 3
			total += x // old x is still live after fresh
			x, y = fresh, x // another phi also reads old x
			a, b = b, a
		}
		return total, b, y, a
	}`)
	for n := 0; n < 9; n++ {
		x, y, total, a, b := 1, 2, 0, "a", "b"
		for range n {
			total += x
			x, y = x+3, x
			a, b = b, a
		}
		out := make([]values.Value, 4)
		if err := vm.Run(context.Background(), "Run", []values.Value{values.Int(int64(n))}, out); err != nil {
			t.Fatal(err)
		}
		if out[0].AsInt() != int64(total) || out[1].AsString() != b || out[2].AsInt() != int64(y) || out[3].AsString() != a {
			t.Fatalf("n=%d: %v", n, out)
		}
	}
}
