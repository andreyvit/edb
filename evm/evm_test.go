package evm

import (
	"context"
	"errors"
	"math"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

func linked(t testing.TB, m *Module, packages ...Package) *VM {
	t.Helper()
	p, err := Link(m, packages...)
	if err != nil {
		t.Fatal(err)
	}
	return New(p)
}

func TestRecursionLimitsAndReuse(t *testing.T) {
	sig := Signature{Params: []*values.Type{values.Int64}, Results: []*values.Type{values.Int64}}
	// factorial keeps n below the outgoing argument window at r2.
	m := &Module{Words: []uint64{1}, Functions: []Function{{Name: "factorial", Signature: sig, RCount: 2, Code: []Instruction{
		WithBC(OpConst, 1, 0), ABC(OpLtI64, 1, 0, 1), WithBC(OpJnz, 1, 5),
		WithBC(OpConst, 1, 0), ABC(OpSubI64, 2, 0, 1), WithBC(OpCallFunc, 0, 0),
		ABC(OpMulI64, 0, 0, 2), ABC(OpReturn, 0, 0, 0),
		WithBC(OpConst, 0, 0), ABC(OpReturn, 0, 0, 0),
	}}}}
	vm := linked(t, m)
	out := []values.Value{values.Int(99)}
	args := []values.Value{values.Int(8)}
	if err := vm.Run(context.Background(), "factorial", args, out); err != nil {
		t.Fatal(err)
	}
	if out[0].R != 40320 {
		t.Fatalf("factorial: %d", out[0].R)
	}
	for _, tc := range []struct {
		limits Limits
		err    error
	}{
		{Limits{Depth: 3}, ErrDepth}, {Limits{RSlots: 4}, ErrRegisters}, {Limits{Steps: 5}, ErrSteps},
	} {
		if err := vm.Reset(); err != nil {
			t.Fatal(err)
		}
		vm.Limits = tc.limits
		if err := vm.Run(context.Background(), "factorial", args, out); !errors.Is(err, tc.err) {
			t.Fatalf("got %v, want %v", err, tc.err)
		}
		if out[0].R != 40320 {
			t.Fatal("failure overwrote result")
		}
	}
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	vm.Limits = Limits{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := vm.Run(ctx, "factorial", args, out); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := vm.Run(context.Background(), "factorial", args, out); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("session survived cancellation: %v", err)
	}
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := vm.Run(context.Background(), "factorial", args, out); err != nil {
		t.Fatal(err)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		if err := vm.Run(context.Background(), "factorial", args, out); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("warmed recursive calls allocate: %v", allocations)
	}
}

func TestNativeCancellationPanicAndPointerCleanup(t *testing.T) {
	sig := Signature{Params: []*values.Type{values.StringType}, Results: []*values.Type{values.StringType}}
	m := &Module{Imports: []Import{{Package: "bh", Name: "Check", Signature: sig}}, Functions: []Function{{Name: "main", Signature: sig, RCount: 1, PCount: 1, Code: []Instruction{ABC(OpMoveV, 1, 0, 0), WithBC(OpCallHost, 0, 0), ABC(OpMoveV, 0, 1, 0), ABC(OpReturn, 0, 0, 0)}}}}
	var cancel context.CancelFunc
	mode := "success"
	vm := linked(t, m, Package{Path: "bh", Functions: []ExternalFunction{{Name: "Check", Signature: sig, Call: func(c *Call) error {
		runtime.GC()
		if c.Value(0).AsString() != strings.Repeat("pointer", 20) {
			t.Fatal("lost pointer data")
		}
		switch mode {
		case "cancel":
			cancel()
		case "panic":
			panic("host panic")
		}
		return nil
	}}}})
	out := make([]values.Value, 1)
	args := []values.Value{values.String(strings.Repeat("pointer", 20))}
	checkClean := func() {
		t.Helper()
		for _, p := range vm.p {
			if p != nil {
				t.Fatal("retained pointer after execution")
			}
		}
		if vm.call.Context != nil || vm.call.R != nil || vm.call.P != nil || vm.running || len(vm.stack) != 0 {
			t.Fatal("retained call state")
		}
	}
	if err := vm.Run(context.Background(), "main", args, out); err != nil {
		t.Fatal(err)
	}
	checkClean()
	if out[0].AsString() != args[0].AsString() {
		t.Fatal("returned pointer cleared")
	}
	mode = "cancel"
	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if err := vm.Run(ctx, "main", args, out); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	checkClean()
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	mode = "panic"
	func() {
		defer func() {
			if v := recover(); v != "host panic" {
				t.Fatalf("panic: %v", v)
			}
		}()
		_ = vm.Run(context.Background(), "main", args, out)
	}()
	checkClean()
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	mode = "success"
	if err := vm.Run(context.Background(), "main", args, out); err != nil {
		t.Fatal(err)
	}
	checkClean()
}

func TestArithmeticBoundaries(t *testing.T) {
	for _, tc := range []struct {
		op         Opcode
		x, y, want uint64
		err        error
	}{
		{OpAddI64, math.MaxUint64, 1, 0, nil},
		{OpSubI64, 0, 1, math.MaxUint64, nil},
		{OpDivI64, 1 << 63, math.MaxUint64, 1 << 63, nil},
		{OpRemI64, 1 << 63, math.MaxUint64, 0, nil},
		{OpDivU64, math.MaxUint64, 2, math.MaxInt64, nil},
		{OpRemU64, math.MaxUint64, 2, 1, nil},
		{OpDivI64, 1, 0, 0, ErrDivideByZero},
		{OpDivU64, 1, 0, 0, ErrDivideByZero},
		{OpRemI64, 1, 0, 0, ErrDivideByZero},
		{OpRemU64, 1, 0, 0, ErrDivideByZero},
		{OpShl, 1, 64, 0, nil}, {OpShrU64, math.MaxUint64, 64, 0, nil},
		{OpShrI64, math.MaxUint64, 100, math.MaxUint64, nil},
		{OpLtI64, math.MaxUint64, 0, 1, nil}, {OpLtU64, math.MaxUint64, 0, 0, nil},
		{OpEqR, math.MaxUint64, math.MaxUint64, 1, nil},
		{OpAnd, 0x55, 0xf0, 0x50, nil}, {OpOr, 0x55, 0xf0, 0xf5, nil}, {OpXor, 0x55, 0xf0, 0xa5, nil},
		{OpAddF64, math.Float64bits(1.5), math.Float64bits(2.5), math.Float64bits(4), nil},
		{OpSubF64, math.Float64bits(1.5), math.Float64bits(2.5), math.Float64bits(-1), nil},
		{OpMulF64, math.Float64bits(1.5), math.Float64bits(2.5), math.Float64bits(3.75), nil},
		{OpDivF64, math.Float64bits(1), 0, math.Float64bits(math.Inf(1)), nil},
		{OpEqF64, math.Float64bits(math.NaN()), math.Float64bits(math.NaN()), 0, nil},
		{OpLtF64, math.Float64bits(-1), 0, 1, nil},
	} {
		t.Run(tc.op.String(), func(t *testing.T) {
			vm := linked(t, &Module{Functions: []Function{{Name: "main", RCount: 2, Signature: Signature{Params: []*values.Type{values.Uint64, values.Uint64}, Results: []*values.Type{values.Uint64}}, Code: []Instruction{ABC(tc.op, 0, 0, 1), ABC(OpReturn, 0, 0, 0)}}}})
			out := make([]values.Value, 1)
			err := vm.Run(context.Background(), "main", []values.Value{values.Uint(tc.x), values.Uint(tc.y)}, out)
			if !errors.Is(err, tc.err) {
				t.Fatalf("got %v, want %v", err, tc.err)
			}
			if err == nil && out[0].R != tc.want {
				t.Fatalf("got %#x, want %#x", out[0].R, tc.want)
			}
		})
	}
}

func TestLinkValidationAndSnapshot(t *testing.T) {
	sig := Signature{Params: []*values.Type{values.Int64}, Results: []*values.Type{values.Int64}}
	pkg := Package{Path: "bh", Functions: []ExternalFunction{{Name: "Double", Signature: sig, Call: func(c *Call) error { c.R[0] *= 2; return nil }}}}
	makeModule := func() *Module {
		return &Module{Words: []uint64{21}, Imports: []Import{{Package: "bh", Name: "Double", Signature: sig}}, Functions: []Function{{Name: "main", Signature: Signature{Results: []*values.Type{values.Int64}}, RCount: 1, Code: []Instruction{WithBC(OpConst, 1, 0), WithBC(OpCallHost, 0, 0), ABC(OpMoveR, 0, 1, 0), ABC(OpReturn, 0, 0, 0)}}}}
	}
	m := makeModule()
	if _, err := Link(m); err == nil {
		t.Fatal("accepted missing import")
	}
	m.Imports[0].Signature = Signature{Params: []*values.Type{values.Uint64}, Results: sig.Results}
	if _, err := Link(m, pkg); err == nil {
		t.Fatal("accepted incompatible signature with same representation")
	}
	for _, bad := range []Instruction{
		ABC(65535, 0, 0, 0), ABC(OpAddI64, 0, 0, 2), WithBC(OpConst, 0, 1), WithBC(OpJump, 0, math.MaxInt32),
		WithBC(OpCallFunc, 0, 65536), WithBC(OpCallHost, 1, 0), WithBC(OpCallHost, 0, 1), ABC(OpMoveP, 0, 0, 0), ABC(OpReturn, 1, 0, 0),
	} {
		m = makeModule()
		m.Functions[0].Code[0] = bad
		if _, err := Link(m, pkg); err == nil {
			t.Fatalf("accepted invalid instruction %#x", bad)
		}
	}
	m = makeModule()
	m.Functions[0].Code = []Instruction{WithBC(OpJnz, 0, math.MaxUint32)}
	if _, err := Link(m, pkg); err == nil {
		t.Fatal("accepted conditional fallthrough past function end")
	}
	m = makeModule()
	m.Functions[0].RCount = 65536
	if _, err := Link(m, pkg); err == nil {
		t.Fatal("accepted outgoing arguments beyond the register address range")
	}
	m = makeModule()
	vm := linked(t, m, pkg)
	m.Words[0] = 99
	m.Functions[0].Code[0] = ABC(0, 0, 0, 0)
	m.Functions[0].Signature.Results[0] = values.StringType
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "main", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].R != 42 {
		t.Fatalf("linked program changed: %v", out)
	}
}

func TestPairAndPointerResults(t *testing.T) {
	typ := values.NewType("*object", values.Pointer)
	obj := new(int)
	vm := linked(t, &Module{Functions: []Function{{Name: "main", RCount: 3, PCount: 3, Signature: Signature{Params: []*values.Type{typ, values.StringType}, Results: []*values.Type{values.Boolean, typ, values.StringType}}, Code: []Instruction{
		ABC(OpMoveV, 2, 0, 0), ABC(OpEqP, 1, 1, 1), ABC(OpClearP, 0, 0, 0), ABC(OpMoveP, 0, 2, 0), ABC(OpReturn, 0, 0, 0),
	}}}})
	out := make([]values.Value, 3)
	err := vm.Run(context.Background(), "main", []values.Value{{P: unsafe.Pointer(obj)}, values.String("retained")}, out)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].R != 1 || out[0].P != nil || out[1].P != unsafe.Pointer(obj) || out[1].R != 0 || out[2].AsString() != "retained" {
		t.Fatalf("bad results: %v", out)
	}
}

func TestExactStepsAcrossInitAndEntry(t *testing.T) {
	ctx := context.Background()
	for _, total := range []int{19, 255, 256, 257, 1025} {
		// The initializer executes total-2 words. The entry calls Go, then returns.
		init := make([]Instruction, total-2)
		for i := range init {
			init[i] = WithBC(OpConst, 0, 0)
		}
		init[len(init)-1] = ABC(OpReturn, 0, 0, 0)
		module := &Module{
			Words: []uint64{1}, Init: "init",
			Imports: []Import{{Package: "h", Name: "Mark"}},
			Functions: []Function{
				{Name: "init", RCount: 1, Code: init},
				{Name: "Run", Code: []Instruction{WithBC(OpCallHost, 0, 0), ABC(OpReturn, 0, 0, 0)}},
			},
		}
		calls := 0
		host := Package{Path: "h", Functions: []ExternalFunction{{Name: "Mark", Call: func(*Call) error {
			calls++
			return nil
		}}}}
		for _, limit := range []int{total - 2, total - 1, total} {
			calls = 0
			vm := linked(t, module, host)
			err := vm.RunWithBudget(ctx, "Run", nil, nil, Budget{Steps: uint64(limit)})
			wantCalls := 0
			if limit > total-2 {
				wantCalls = 1
			}
			var wantErr error
			if limit < total {
				wantErr = ErrSteps
			}
			if !errors.Is(err, wantErr) || calls != wantCalls {
				t.Fatalf("total %d, limit %d: got (%v, %d calls), want (%v, %d)", total, limit, err, calls, wantErr, wantCalls)
			}
			if err == nil {
				if err := vm.RunWithBudget(ctx, "Run", nil, nil, Budget{Steps: 2}); err != nil || calls != 2 {
					t.Fatalf("warm entry did not get exactly two fresh steps: %d calls, %v", calls, err)
				}
			}
		}
	}
}

func TestRecordContext(t *testing.T) {
	type labelKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), labelKey{}, "outer"))
	defer cancel()
	ctx, outer := WithRecords(parent)
	defer outer.Close()
	child := context.WithValue(ctx, labelKey{}, "child")
	nested, inner := WithRecords(child)
	defer inner.Close()

	if scopeFromContext(parent) != nil || scopeFromContext(ctx) != outer || scopeFromContext(child) != outer || scopeFromContext(nested) != inner {
		t.Fatal("incorrect inherited record scope")
	}
	if ctx.Value(labelKey{}) != "outer" || nested.Value(labelKey{}) != "child" {
		t.Fatal("record scope hid a parent value")
	}
	cancel()
	if nested.Err() != context.Canceled {
		t.Fatal("nested record scope did not inherit cancellation")
	}
}

// Changing instruction widths may give several sites the same initial cache
// index. They must still have distinct, stable handles in the transaction.
func TestCollidingRecordSites(t *testing.T) {
	_, scope := WithRecords(context.Background())
	defer scope.Close()
	pcs := [...]int{3, 4, 11, 5, 19, 27, 35, 43, 51, 59}
	const n = len(pcs)
	limits := Limits{Allocations: uint64(n), AllocationBytes: uint64(n) * recordHandleBytes}
	budget := &executionBudget{limits: limits}
	vm := new(VM)
	var handles [n]values.Value
	for pass := range 2 {
		for i, pc := range pcs {
			site := recordSite{vm: vm, pc: pc}
			v, row, err := scope.resultSlot(site, budget)
			if err != nil {
				t.Fatal(err)
			}
			if pass == 0 {
				row.step.Key = uint64(i)
				handles[i] = v
				if i < len(scope.local) && scope.reads != nil {
					t.Fatal("overflow with free slots")
				}
			} else if v != handles[i] || row.step.Key != uint64(i) {
				t.Fatalf("site %d reused the wrong row", i)
			}
		}
	}
}
