package evmcompiler_test

import (
	"context"
	"errors"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
	"testing"
	"unsafe"
)

func TestGoDeferAndRecover(t *testing.T) {
	tests := []struct {
		name, source string
		want         int64
	}{
		{"arguments and named results", `func Run()(r int){x:=1;defer func(n int){r=r*10+n}(x);x=2;defer func(){r=r*10+x}();return 3}`, 321},
		{"direct recovery", `func rescue(r *int){if recover()!=nil{*r=7}};func Run()(r int){defer rescue(&r);panic(3)}`, 7},
		{"helper cannot recover", `func helper()any{return recover()};func Run()(r int){defer func(){if recover()!=nil{r+=10}}();defer func(){if helper()==nil{r++}}();panic(1)}`, 11},
		{"deferred builtin recover", `func Run()(r int){defer func(){if recover()!=nil{r=9}}();defer recover();panic(1)}`, 9},
		{"nil deferred function", `func Run()(r int){defer func(){if recover()!=nil{r++}}();var f func();defer f();r=9;return}`, 10},
		{"nested recovered panic", `func Run()(r int){defer func(){if recover()==1{r+=100}}();defer func(){defer func(){if recover()==2{r+=10}}();panic(2)}();panic(1)}`, 110},
		{"replacement panic", `func Run()(r int){defer func(){if recover()==2{r=20}}();defer func(){panic(2)}();panic(1)}`, 20},
		{"older defers after recovery", `func Run()(r int){defer func(){r*=2}();defer func(){if recover()!=nil{r=7}}();panic(3)}`, 14},
		{"panic nil", `func Run()(r int){defer func(){p:=recover();if p==nil{return};if e,ok:=p.(error);ok&&e.Error()!=""{r=1}}();panic(nil)}`, 1},
		{"typed nil panic", `type T struct{};func Run()(r int){defer func(){p:=recover();v,ok:=p.(*T);if ok&&v==nil{r=2}}();var p *T;panic(p)}`, 2},
		{"bound method recovery", `type T struct{R *int};func(t T)Rescue(){if recover()!=nil{*t.R=4}};func Run()(r int){t:=T{&r};f:=t.Rescue;defer f();panic(1)}`, 4},
		{"method expression recovery", `type T struct{R *int};func(t T)Rescue(){if recover()!=nil{*t.R=5}};func Run()(r int){t:=T{&r};defer T.Rescue(t);panic(1)}`, 5},
		{"normal recover nil", `func Run()int{if recover()==nil{return 1};return 0}`, 1},
		{"nested normal defer cannot recover", `func Run()(r int){defer func(){if recover()!=nil{r+=10}}();defer func(){defer func(){if recover()==nil{r++}}()}();panic(1)}`, 11},
		{"builtins", `func Run()(r int){a:=[]int{1,2};b:=[]int{8,9};m:=map[int]int{1:2};defer func(){r=a[0]*10+len(m)}();defer delete(m,1);defer copy(a,b);return}`, 80},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vm := compileGoVM(t, tt.source)
			out := make([]values.Value, 1)
			if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].AsInt() != tt.want {
				t.Fatalf("got %v %v, want %d", err, out, tt.want)
			}
		})
	}
}
func TestGoRecoverRuntimePanics(t *testing.T) {
	for _, body := range []string{`var p *int;_ = *p`, `s:=[]int{1};_ = s[2]`, `var m map[int]int;m[1]=2`, `n:=0;_ = 1/n`, `n:=-1;_ = 1<<n`, `var x any=1;_ = x.(string)`, `var x any=[]int{1};_ = x==x`, `var m map[any]int;_ = m[[]int{1}]`} {
		vm := compileGoVM(t, `func Run()(ok bool){defer func(){p:=recover();e,valid:=p.(error);ok=valid&&e.Error()!=""}();`+body+`;return}`)
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
			t.Fatalf("runtime panic %s: %v %v", body, err, out)
		}
	}
	vm := compileGoVM(t, `func Run(){n:=0;_ = 1/n}`)
	if err := vm.Run(context.Background(), "Run", nil, nil); !errors.Is(err, evm.ErrDivideByZero) {
		t.Fatalf("uncaught sentinel: %v", err)
	}
}
func TestGoRangeFunctions(t *testing.T) {
	vm := compileGoVM(t, `func numbers(yield func(int)bool){for i:=1;i<=5;i++{if !yield(i){return}}};func pairs(yield func(int,string)bool){if !yield(2,"x"){return};yield(3,"y")};func Run()(r int){for n:=range numbers{if n==2{continue};defer func(v int){r+=v*10}(n);r+=n;if n==3{break}};for k,v:=range pairs{if v=="y"{r+=k;return};r+=k};return}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 49 {
		t.Fatalf("range function: %v %v", err, out)
	}
}
func TestGoFatalLimitsDoNotRunDefers(t *testing.T) {
	vm := compileGoVM(t, `var ran bool;func Run(){defer func(){ran=true;recover()}();for{}};func State()bool{return ran}`)
	vm.Limits.Steps = 100
	if err := vm.Run(context.Background(), "Run", nil, nil); !errors.Is(err, evm.ErrSteps) {
		t.Fatalf("limit: %v", err)
	}
	if err := vm.Run(context.Background(), "State", nil, make([]values.Value, 1)); !errors.Is(err, evm.ErrInvalidState) {
		t.Fatalf("failed session: %v", err)
	}
}

func TestGoDeferInterfaceMethodsAndInitializers(t *testing.T) {
	vm := compileGoVM(t, `var N int;func init(){defer func(){if recover()!=nil{N=4}}();panic(1)};type T struct{R *int};func(t T)Rescue(){if recover()!=nil{*t.R+=10}};type Outer struct{T};func Run()(r int){r=N;var x interface{Rescue()}=Outer{T{&r}};f:=x.Rescue;defer f();panic(2)}`)
	out := make([]values.Value, 1)
	for i := 0; i < 2; i++ {
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 14 {
			t.Fatalf("init/interface recovery: %v %v", err, out)
		}
	}
}
func TestGoRangeYieldLifetimeAndProtocol(t *testing.T) {
	tests := []string{
		`func bad(y func(int)bool){y(1);y(2)};func Run()(ok bool){defer func(){ok=recover()!=nil}();for range bad{break};return}`,
		`var saved func(int)bool;func save(y func(int)bool){saved=y};func Run()(ok bool){for range save{};defer func(){ok=recover()!=nil}();saved(1);return}`,
		`func bad(y func(int)bool){func(){defer func(){recover()}();y(1)}()};func Run()(ok bool){defer func(){ok=recover()!=nil}();for range bad{panic("body")};return}`,
	}
	for _, source := range tests {
		vm := compileGoVM(t, source)
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
			t.Fatalf("yield protocol: %v %v", err, out)
		}
	}
}
func TestGoDeferredArgumentsKeepCopiesAndAliases(t *testing.T) {
	vm := compileGoVM(t, `type S struct{N int};func Run()(r int){s:=S{1};a:=[]int{2};defer func(v S,a []int){r=v.N*10+a[0]}(s,a);s.N=9;a[0]=8;return}`)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 18 {
		t.Fatalf("deferred arguments: %v %v", err, out)
	}
}

func TestGoFatalFailuresSkipDeferredHostEffects(t *testing.T) {
	for _, mode := range []string{"steps", "allocation", "depth", "native", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			ran := false
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Mark", Call: func(*evm.Call) error { ran = true; return nil }}, {Name: "Bad", Call: func(*evm.Call) error {
				if mode == "cancellation" {
					cancel()
					return nil
				}
				return evm.ErrBounds
			}}}}
			body := `for{}`
			want := evm.ErrSteps
			limits := evm.Limits{Steps: 100}
			switch mode {
			case "allocation":
				limits = evm.Limits{AllocationBytes: 1}
				want = evm.ErrAllocation
			case "depth":
				body = "Run()"
				limits = evm.Limits{Depth: 8}
				want = evm.ErrDepth
			case "native":
				body = "h.Bad()"
				want = evm.ErrBounds
			case "cancellation":
				body = "h.Bad()"
				want = context.Canceled
			}
			m, err := evmcompiler.Compile("main.go", `import "h";func Run(){defer h.Mark();defer func(){recover()}();`+body+`}`, pkg)
			if err != nil {
				t.Fatal(err)
			}
			p, err := evm.Link(m, pkg)
			if err != nil {
				t.Fatal(err)
			}
			vm := evm.New(p)
			vm.Limits = limits
			if err := vm.Run(ctx, "Run", nil, nil); !errors.Is(err, want) {
				t.Fatalf("failure: %v", err)
			}
			if ran {
				t.Fatal("fatal failure ran deferred host effects")
			}
		})
	}
}
func TestGoDeferredNativeSourcePosition(t *testing.T) {
	line := 0
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Mark", Call: func(c *evm.Call) error { line = c.Position.Line; return nil }}}}
	m, err := evmcompiler.Compile("plugin.go", "import \"h\"\nfunc Run(){\n defer h.Mark()\n}\n", pkg)
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
	if line != 3 {
		t.Fatalf("defer call position: %d", line)
	}
}

func TestGoDeferLinkValidationAndSnapshot(t *testing.T) {
	source := `func Run()(r int){defer func(n int){r=n}(7);return}`
	for _, mutate := range []func(*evm.Module){
		func(m *evm.Module) { m.Defers[0].Kind = 99 },
		func(m *evm.Module) { m.Defers[0].Args[0].Slot = 65535 },
		func(m *evm.Module) { m.Defers[0].Signature = 100000 },
		func(m *evm.Module) { m.Functions[0].RecoverPC = 100000 },
		func(m *evm.Module) { m.Defers[0].Function.Repr = values.Raw },
	} {
		m, err := evmcompiler.Compile("main.go", source)
		if err != nil {
			t.Fatal(err)
		}
		mutate(m)
		if _, err := evm.Link(m); err == nil {
			t.Fatal("accepted invalid defer metadata")
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
	m.Defers[0].Args[0].Slot = 65535
	m.Functions[0].RecoverPC = 100000
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 7 {
		t.Fatalf("defer metadata snapshot: %v %v", err, out)
	}
}

func TestGoRangeDeferredReturnValues(t *testing.T) {
	tests := []struct {
		body string
		want uint64
	}{
		{`func Run()int{n:=7;for range seq{defer func(){n=9}()};return n}`, 7},
		{`func Run()int{n:=7;for range seq{defer func(){n=9}();return n};return 0}`, 7},
		{`func Run()(n int){for range seq{defer func(){n=9}();return 7};return 0}`, 9},
		{`func Run()(n int){for range seq{for range seq{defer func(){n++}()}};return 7}`, 11},
	}
	for _, tt := range tests {
		vm := compileGoVM(t, `func seq(y func()bool){if !y(){return};y()};`+tt.body)
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != tt.want {
			t.Fatalf("range return: %v %v, want %d", err, out, tt.want)
		}
	}
}

func TestGoDeferredSourceFailurePosition(t *testing.T) {
	vm := compileGoVM(t, "func Mark(){}\nfunc Run(){\n defer Mark()\n}\n")
	vm.Limits.Depth = 1
	err := vm.Run(context.Background(), "Run", nil, nil)
	var execution *evm.ExecutionError
	if !errors.Is(err, evm.ErrDepth) || !errors.As(err, &execution) || execution.Position.Line != 3 {
		t.Fatalf("deferred source failure position: %v", err)
	}
}

func TestGoDeferredBuiltinFailurePosition(t *testing.T) {
	vm := compileGoVM(t, "func Run(){\n defer println(\"too long\")\n}\n")
	vm.Limits.OutputBytes = 1
	err := vm.Run(context.Background(), "Run", nil, nil)
	var execution *evm.ExecutionError
	if !errors.Is(err, evm.ErrOutput) || !errors.As(err, &execution) || execution.Position.Line != 2 {
		t.Fatalf("deferred builtin failure position: %v", err)
	}
}

func TestGoDeferredBulkCopy(t *testing.T) {
	vm := compileGoVM(t, `func Run(dst, src []int) {
		defer copy(dst, src)
	}`)
	vm.Limits.Steps = 32
	dst, src := make([]values.Value, 512), make([]values.Value, 512)
	src[511] = values.Int(42)
	args := []values.Value{values.SliceValue(unsafe.Pointer(&dst[0]), 512, 512), values.SliceValue(unsafe.Pointer(&src[0]), 512, 512)}
	if err := vm.Run(context.Background(), "Run", args, nil); err != nil || dst[511] != src[511] {
		t.Fatalf("deferred copy: %v, %v", dst[511], err)
	}
}

func TestGoRecursiveAndMixedDefers(t *testing.T) {
	tests := []struct {
		source string
		want   uint64
	}{
		{`var seq int;func rec(n int)(sum int){defer func(){seq=seq*10+n;sum+=n}();if n>0{sum=rec(n-1)};return};func Run()int{sum:=rec(3);if seq!=123{return -1};return sum}`, 6},
		{`func seq(y func(int)bool){if !y(2){return};y(3)};func Run()(r int){defer func(){r=r*10+1}();for n:=range seq{defer func(n int){r=r*10+n}(n)};defer func(){r=r*10+4}();return}`, 4321},
	}
	for _, tt := range tests {
		vm := compileGoVM(t, tt.source)
		out := make([]values.Value, 1)
		if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != tt.want {
			t.Fatalf("recursive/mixed defers: %v %v", err, out)
		}
	}
}
func TestGoYieldAndDeferredCallsShareFuel(t *testing.T) {
	for _, source := range []string{
		`import "h";func seq(y func()bool){for{if !y(){return}}};func Run(){for range seq{defer h.Mark()}}`,
		`import "h";func Run(){defer h.Mark();defer func(){for{}}()}`,
	} {
		ran := false
		pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Mark", Call: func(*evm.Call) error { ran = true; return nil }}}}
		m, err := evmcompiler.Compile("main.go", source, pkg)
		if err != nil {
			t.Fatal(err)
		}
		p, err := evm.Link(m, pkg)
		if err != nil {
			t.Fatal(err)
		}
		vm := evm.New(p)
		vm.Limits.Steps = 500
		if err := vm.Run(context.Background(), "Run", nil, nil); !errors.Is(err, evm.ErrSteps) {
			t.Fatalf("shared fuel: %v", err)
		}
		if ran {
			t.Fatal("exhausted fuel ran pending defers")
		}
	}
}
