package evmasm_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmasm"
	"github.com/andreyvit/edb/values"
)

func TestLoopAndFunctionCall(t *testing.T) {
	m, err := evmasm.Assemble(`
func main i64 -> i64
  registers 3 0
  const r1 0
  const r2 1
loop:
  jz r0 done
  add.i64 r1 r1 r0
  sub.i64 r0 r0 r2
  jump loop
done:
  move.r r3 r1
  call.func fun1
  move.r r0 r3
  ret
end

func square i64 -> i64
  registers 1 0
  mul.i64 r0 r0 r0
  ret
end
`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	v := evm.New(p)
	out := make([]values.Value, 1)
	if err := v.Run(context.Background(), "main", []values.Value{values.Int(10)}, out); err != nil {
		t.Fatal(err)
	}
	if out[0].R != 3025 || out[0].P != nil {
		t.Fatalf("got %+v, want 3025", out[0])
	}
}

type customer struct{ points int64 }

func (c *customer) add(n int64) int64 { c.points += n; return c.points }

const awardSource = `
import bh.Award string *bh.Customer i64 -> string i64
func main *bh.Customer i64 -> string i64
  registers 3 1
  string v2 "demo #1"
  move.r r3 r2
  move.p p1 p2
  move.r r4 r0
  move.p p2 p0
  call.func award
  move.r r0 r3
  move.p p0 p1
  move.r r1 r4
  ret
end
func award string *bh.Customer i64 -> string i64
  registers 2 2
  move.v v2 v0
  move.r r3 r1
  move.p p3 p1
  call.host bh.Award
  move.v v0 v2
  move.r r1 r3
  ret
end
`

func awardProgram(t testing.TB, invoke evm.Native) *evm.Program {
	t.Helper()
	typ := values.NewType("*bh.Customer", values.Pointer)
	m, err := evmasm.Assemble(awardSource, typ)
	if err != nil {
		t.Fatal(err)
	}
	pkg := evm.Package{Path: "bh", Functions: []evm.ExternalFunction{{
		Name: "Award", Signature: evm.Signature{
			Params:  []*values.Type{values.StringType, typ, values.Int64},
			Results: []*values.Type{values.StringType, values.Int64},
		}, Call: invoke,
	}}}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func award(c *evm.Call) error {
	obj := (*customer)(c.P[1])
	c.R[1] = uint64(obj.add(int64(c.R[1])))
	return nil
}

func TestHostMethodAndMixedWindows(t *testing.T) {
	failure := errors.New("award unavailable")
	var fail bool
	var vm *evm.VM
	p := awardProgram(t, func(c *evm.Call) error {
		if c.Value(0).AsString() != "demo #1" {
			t.Fatal("lost string while shifting windows")
		}
		if err := vm.Run(c.Context, "main", nil, nil); !errors.Is(err, evm.ErrReentry) {
			t.Fatalf("reentry: %v", err)
		}
		if fail {
			return failure
		}
		return award(c)
	})
	vm = evm.New(p)
	alice := &customer{}
	args := []values.Value{{P: unsafe.Pointer(alice)}, values.Int(7)}
	out := make([]values.Value, 2)
	if err := vm.Run(context.Background(), "main", args, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsString() != "demo #1" || out[1].AsInt() != 7 || alice.points != 7 || out[1].P != nil {
		t.Fatalf("unexpected results: %v, customer: %v", out, alice)
	}
	fail = true
	err := vm.Run(context.Background(), "main", args, out)
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "award pc 3 (line") {
		t.Fatalf("lost host error/location: %v", err)
	}
	if alice.points != 7 || out[1].AsInt() != 7 {
		t.Fatal("failed call changed results")
	}
	if err := vm.Reset(); err != nil {
		t.Fatal(err)
	}
	fail = false
	if err := vm.Run(context.Background(), "main", args, out); err != nil {
		t.Fatal(err)
	}
	if alice.points != 14 {
		t.Fatalf("VM reuse: %v", alice)
	}
}

func BenchmarkHostCall(b *testing.B) {
	vm := evm.New(awardProgram(b, award))
	alice := &customer{}
	args := []values.Value{{P: unsafe.Pointer(alice)}, values.Int(1)}
	out := make([]values.Value, 2)
	ctx := context.Background()
	if err := vm.Run(ctx, "main", args, out); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := vm.Run(ctx, "main", args, out); err != nil {
			b.Fatal(err)
		}
	}
}

func TestAssemblerDiagnostics(t *testing.T) {
	for _, source := range []string{
		"", "ret", "func main ->\nregisters 0 0\nret", "func main unknown ->\nend",
		"func main ->\nregisters -1 0\nend", "func main ->\nregisters 1 0\nconst r65536 1\nend",
		"func main ->\nregisters 1 0\nstring v0 \"unterminated\nend",
		"func main ->\nregisters 1 0\nconst r0 -9223372036854775809\nend",
		"func main ->\nregisters 1 0\nconst r0 18446744073709551616\nend",
		"func main ->\nregisters 1 0\njump missing\nend",
		"func main ->\nregisters 1 0\ncall missing\nend",
		"func main ->\nregisters 1 0\nadd.i64 p0 r0 r0\nend",
		"func main ->\nregisters 1 0\ncall.host 0 r0 p0\nend",
		"func main ->\nregisters 1 0\ncall 4294967296\nend",
		"func main ->\nregisters 1 0\nx:\nx:\nret\nend",
		"func main ->\nregisters 1 0\nret\nend\nfunc main ->\nend",
	} {
		if _, err := evmasm.Assemble(source); err == nil {
			t.Fatalf("accepted invalid assembly %q", source)
		}
	}
	m, err := evmasm.Assemble(`
func main - -> f64, u64
 registers 2 0
 float r0 1.5
 const r1 0xffffffffffffffff
 ret # done
end
`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 2)
	if err := evm.New(p).Run(context.Background(), "main", nil, out); err != nil {
		t.Fatal(err)
	}
	if out[0].AsFloat() != 1.5 || out[1].R != ^uint64(0) {
		t.Fatalf("lost constant bits: %v", out)
	}
}

func FuzzAssemble(f *testing.F) {
	f.Add(awardSource)
	f.Add("func main ->\nregisters 1 0\nconst r0 42\nret\nend")
	f.Add("func main ->\nregisters 0 0\nx:\njump x\nend")
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > 8192 {
			t.Skip()
		}
		m, err := evmasm.Assemble(source)
		if err != nil {
			return
		}
		// Linking is also expected to reject malformed operands without panicking.
		_, _ = evm.Link(m)
	})
}

func TestCallsReuseOutgoingWindowAndPreserveLocals(t *testing.T) {
	m, err := evmasm.Assemble(`
import bh.Increment i64 -> i64
import bh.Scale i64 i64 -> i64
func main i64 -> i64
  registers 2 0
  move.r r2 r0
  call.host imp0
  move.r r1 r2
  move.r r2 r0
  const r3 2
  call.host bh.Scale
  add.i64 r0 r1 r2
  ret
end
`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, evm.Package{Path: "bh", Functions: []evm.ExternalFunction{
		{Name: "Scale", Signature: evm.Signature{Params: []*values.Type{values.Int64, values.Int64}, Results: []*values.Type{values.Int64}}, Call: func(c *evm.Call) error { c.R[0] *= c.R[1]; return nil }},
		{Name: "Increment", Signature: evm.Signature{Params: []*values.Type{values.Int64}, Results: []*values.Type{values.Int64}}, Call: func(c *evm.Call) error { c.R[0]++; return nil }},
	}})
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "main", []values.Value{values.Int(30)}, out); err != nil {
		t.Fatal(err)
	}
	if out[0].R != 91 {
		t.Fatalf("caller locals lost: %v", out)
	}
	vm.Limits = evm.Limits{RSlots: 3}
	if err := vm.Run(context.Background(), "main", []values.Value{values.Int(30)}, out); !errors.Is(err, evm.ErrRegisters) {
		t.Fatalf("outgoing slots must count against limits: %v", err)
	}
}

func TestFusedCallsShareNativeABIAndCostOneStep(t *testing.T) {
	src := `import h.Double i64 -> i64
import h.Get i64 -> *Row
func Run i64 -> i64
registers 1 1
call.host.rr r0 r0 h.Double
call.host.rp p0 r0 imp1
jnil p0 done
call.host.rr r0 r0 imp0
done:
ret
end`
	row := values.NewType("*Row", values.Pointer)
	m, err := evmasm.Assemble(src, row)
	if err != nil {
		t.Fatal(err)
	}
	var input int64
	host := evm.Package{Path: "h", Functions: []evm.ExternalFunction{
		{Name: "Double", Signature: m.Imports[0].Signature, Call: func(c *evm.Call) error {
			c.R[0] *= 2
			return nil
		}},
		{Name: "Get", Signature: m.Imports[1].Signature, Call: func(c *evm.Call) error {
			input = int64(c.R[0])
			if input > 0 {
				c.P[0] = unsafe.Pointer(&input)
			}
			return nil
		}},
	}}
	p, err := evm.Link(m, host)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := []values.Value{values.Int(99)}
	for _, tc := range []struct {
		x, want int64
		steps   uint64
	}{{1, 4, 5}, {-1, -2, 4}} {
		if err := vm.RunWithBudget(context.Background(), "Run", []values.Value{values.Int(tc.x)}, out, evm.Budget{Steps: tc.steps}); err != nil || out[0].AsInt() != tc.want || input != 2*tc.x {
			t.Fatalf("x=%d: result %v, callback input %d, err %v", tc.x, out, input, err)
		}
	}
	if err := vm.RunWithBudget(context.Background(), "Run", []values.Value{values.Int(1)}, out, evm.Budget{Steps: 4}); !errors.Is(err, evm.ErrSteps) {
		t.Fatalf("four steps on the five-word path: %v", err)
	}

	// Wrong result bank, wrong input register, and nonexistent import.
	for _, bad := range []evm.Instruction{
		evm.ABC(evm.OpCallHostRR, 0, 0, 1), evm.ABC(evm.OpCallHostRP, 0, 0, 0),
		evm.ABC(evm.OpCallHostRR, 0, 1, 0), evm.ABC(evm.OpCallHostRP, 0, 0, 2),
		evm.WithBC(evm.OpJNil, 0, 1000), evm.ABC(evm.OpLoopLtI64, 0, 0, 32767),
	} {
		m.Functions[0].Code[0] = bad
		if _, err := evm.Link(m, host); err == nil {
			t.Fatalf("linked invalid %s: %#x", bad.Op(), bad)
		}
	}
}

func TestFusedLoopOverflowAndBudget(t *testing.T) {
	m, err := evmasm.Assemble(`func Run i64 i64 -> i64
registers 2 0
loop.lt.i64 r0 r1 wrapped
ret
wrapped:
const r0 123
ret
end`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	for _, tc := range []struct {
		x, limit, want int64
		steps          uint64
	}{{7, 8, 8, 2}, {7, 9, 123, 3}, {math.MaxInt64, 0, 123, 3}} {
		args := []values.Value{values.Int(tc.x), values.Int(tc.limit)}
		if err := vm.RunWithBudget(context.Background(), "Run", args, out, evm.Budget{Steps: tc.steps}); err != nil || out[0].AsInt() != tc.want {
			t.Fatalf("(%d,%d): %v, %v", tc.x, tc.limit, out, err)
		}
	}
}

func TestCompactOperands(t *testing.T) {
	const src = `func f f64 f64 f64 -> f64
registers 4096 0
mul.add.f64 r4095 r0 r1 r2
move.r r0 r4095
ret
end`
	m, err := evmasm.Assemble(src)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.Disassemble(), "r4095 r0 r1 r2") {
		t.Fatal(m.Disassemble())
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "f", []values.Value{values.Float(2), values.Float(3), values.Float(4)}, got); err != nil || got[0].AsFloat() != 10 {
		t.Fatal(got, err)
	}
	if _, err := evmasm.Assemble(strings.ReplaceAll(src, "r4095", "r4096")); err == nil {
		t.Fatal("wide operand truncated")
	}
	m.Functions[0].RCount = 3
	if _, err := evm.Link(m); err == nil {
		t.Fatal("out-of-frame destination linked")
	}
}

func TestFusedBranchCanBeEnteredSeparately(t *testing.T) {
	m, err := evmasm.Assemble(`func f i64 i64 -> i64
registers 3 0
jnz r1 branch
lt.i64 r0 r0 r1
branch:
jz r0 empty
add.imm.i64 r0 r0 10
ret
empty:
ret
end`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	for _, tc := range []struct{ x, y, w int64 }{{-1, 0, 11}, {2, 0, 0}, {7, 1, 17}, {0, 1, 0}} {
		if err := vm.Run(context.Background(), "f", []values.Value{values.Int(tc.x), values.Int(tc.y)}, out); err != nil || out[0].AsInt() != tc.w {
			t.Fatal(tc, out, err)
		}
	}
	// jnz + (lt,jz) + add.imm + ret. Both comparison words share one step.
	args := []values.Value{values.Int(-1), values.Int(0)}
	if err := vm.RunWithBudget(context.Background(), "f", args, out, evm.Budget{Steps: 4}); err != nil || out[0].R != 11 {
		t.Fatal(out, err)
	}
	if err := vm.RunWithBudget(context.Background(), "f", args, out, evm.Budget{Steps: 3}); !errors.Is(err, evm.ErrSteps) {
		t.Fatal(err)
	}
}

func TestWarmScalarCallClearsLocalsAndHonorsNewLimits(t *testing.T) {
	m, err := evmasm.Assemble(`func main i64 -> i64
registers 1 1
move.r r1 r0
call.func fun1
move.r r0 r1
ret
end
func probe i64 -> i64
registers 3 0
move.r r2 r0
move.r r0 r1
move.r r1 r2
ret
end`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		limits evm.Limits
		want   error
	}{{evm.Limits{RSlots: 3}, evm.ErrRegisters}, {evm.Limits{Depth: 1}, evm.ErrDepth}} {
		vm := evm.New(p)
		args, out := []values.Value{values.Int(42)}, make([]values.Value, 1)
		for range 3 {
			if err := vm.Run(context.Background(), "main", args, out); err != nil || out[0].R != 0 {
				t.Fatalf("stale raw local: got %v, %v", out, err)
			}
		}
		vm.Limits = tc.limits
		if err := vm.Run(context.Background(), "main", args, out); !errors.Is(err, tc.want) {
			t.Fatalf("warm limits %v: got %v, want %v", tc.limits, err, tc.want)
		}
	}
}
