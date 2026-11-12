package evmcompiler_test

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestCountedLoopFusion(t *testing.T) {
	source := `func Sum(start, limit int64) int64 {
		var total int64
		for i := start; i < limit; i++ {
			total += i
		}
		return total
	}
	func Skip(start, limit int64) int64 {
		var total int64
		for i := start; i < limit; i += 2 { total += i }
		return total
	}
	func check(n int64, tests *int64) int64 { *tests++; return n }
	func Guard(start, limit int64) int64 {
		var tests int64
		for i := start; i < check(limit, &tests); i++ {}
		return tests
	}
	func Break(start, limit int64) int64 {
		stopped := int64(-1)
		i := start
		for ; i < limit; i++ {
			if i == 4 { stopped = 9; break }
		}
		return stopped+i
	}
	func ChangingBound(start, limit int64) int64 {
		var total int64
		for i := start; i < limit; i++ {
			total += i
			limit--
		}
		return total
	}`
	m, err := evmcompiler.Compile("loop.go", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range m.Functions {
		listing := m.DisassembleFunction(i)
		fused := strings.Contains(listing, "loop.lt.i64")
		if f.Name == "Sum" && !fused || (f.Name == "Skip" || f.Name == "Guard") && fused {
			t.Fatal(listing)
		}
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	for _, tc := range []struct {
		fn               string
		start, end, want int64
	}{
		{"Sum", 3, 2, 0}, {"Sum", -3, 4, 0}, {"Sum", 1, 6, 15},
		{"Sum", math.MaxInt64 - 1, math.MaxInt64, math.MaxInt64 - 1},
		{"Sum", math.MinInt64, math.MinInt64 + 1, math.MinInt64},
		{"Break", 0, 4, 3}, {"Break", 0, 6, 13}, {"Break", 5, 5, 4},
		{"Skip", 1, 6, 9}, {"Guard", -2, 2, 5}, {"ChangingBound", 1, 8, 10},
	} {
		err := vm.Run(context.Background(), tc.fn, []values.Value{values.Int(tc.start), values.Int(tc.end)}, out)
		if err != nil || out[0].AsInt() != tc.want {
			t.Fatalf("%s(%d,%d) = %d, %v; want %d", tc.fn, tc.start, tc.end, out[0].AsInt(), err, tc.want)
		}
	}
}

func TestNilBranchFusion(t *testing.T) {
	m, err := evmcompiler.Compile("nil.go", `func Make() *int { n := 7; return &n }
	func Nil(p *int) int { if nil == p {return 1}; return 2 }
	func NonNil(p *int) int { if p != nil {return 3}; return 4 }
	func Equal(p,q *int) int { if p == q {return 5}; return 6 }
	func Keep(p *int) bool { same:=p==nil; if same {return same}; return same }`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range m.Functions {
		listing := m.DisassembleFunction(i)
		fused := strings.Contains(listing, "jnil") || strings.Contains(listing, "jnotnil")
		if fused != (f.Name == "Nil" || f.Name == "NonNil") {
			t.Fatal(listing)
		}
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Make", nil, out); err != nil {
		t.Fatal(err)
	}
	live := out[0].P
	for _, tc := range []struct {
		fn   string
		args []values.Value
		want int64
	}{
		{"Nil", []values.Value{{}}, 1}, {"Nil", []values.Value{{P: live}}, 2},
		{"NonNil", []values.Value{{}}, 4}, {"NonNil", []values.Value{{P: live}}, 3},
		{"Equal", []values.Value{{P: live}, {P: live}}, 5}, {"Equal", []values.Value{{P: live}, {}}, 6},
		{"Keep", []values.Value{{}}, 1}, {"Keep", []values.Value{{P: live}}, 0},
	} {
		if err := vm.Run(context.Background(), tc.fn, tc.args, out); err != nil || out[0].AsInt() != tc.want {
			t.Fatalf("%s: %v, %v", tc.fn, out, err)
		}
	}
}

func TestCountedLoopWithLongBackEdge(t *testing.T) {
	const calls = 1 << 15
	source := "var seen int; func tick(){seen++}; func Run(n int) int { for i:=0; i<n; i++ {" +
		strings.Repeat("tick();", calls) + "}; return seen }"
	m, err := evmcompiler.Compile("wide.go", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range m.Functions {
		if f.Name != "Run" {
			continue
		}
		for _, ins := range f.Code {
			if ins.Op() == evm.OpLoopLtI64 {
				t.Fatal("16-bit loop displacement cannot reach the start of the body")
			}
		}
	}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", []values.Value{values.Int(2)}, out); err != nil || out[0].AsInt() != 2*calls {
		t.Fatalf("long loop: %v, %v", out, err)
	}
}

func TestSteppedLoopFallsBackForLongBody(t *testing.T) {
	const calls = 2048
	source := "var seen int; func tick(){seen++}; func Run(n, step int) int { for i:=0; i<n; i+=step {" +
		strings.Repeat("tick();", calls) + "}; return seen }"
	m, err := evmcompiler.Compile("stride.go", source)
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range m.Functions {
		for _, ins := range fn.Code {
			if ins.Op() == evm.OpLoopAddLtI64 {
				t.Fatal("12-bit displacement truncated")
			}
		}
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]values.Value, 1)
	args := []values.Value{values.Int(7), values.Int(3)}
	if err := evm.New(p).Run(context.Background(), "Run", args, out); err != nil || out[0].AsInt() != 3*calls {
		t.Fatal(out, err)
	}
}

func TestFusedHostFailureIsStillFatal(t *testing.T) {
	// ErrBounds is recoverable when raised by guest indexing. Returning that
	// same error from a Go callback must abort before the result move.
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{
		Name: "Read", Signature: evm.Signature{Params: []*values.Type{values.IntType}, Results: []*values.Type{values.IntType}},
		Call: func(c *evm.Call) error { c.R[0] = 17; return evm.ErrBounds },
	}}}
	m, err := evmcompiler.Compile("read.go", `import "h"
func Run(i int) (v int) {
	defer func() { if recover() != nil { v = 42 } }()
	return h.Read(i)
}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.Disassemble(), "call.host.rr") {
		t.Fatal(m.Disassemble())
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	out := []values.Value{values.Int(-1)}
	err = evm.New(p).Run(context.Background(), "Run", []values.Value{values.Int(3)}, out)
	var trap *evm.ExecutionError
	if !errors.Is(err, evm.ErrBounds) || !errors.As(err, &trap) || trap.Position.Line != 4 || out[0].AsInt() != -1 {
		t.Fatalf("fused failure: result %v, error %v", out, err)
	}
}

func TestRawSliceAccess(t *testing.T) {
	m, err := evmcompiler.Compile("slice.go", `
type Score int32
func At(xs []Score, i int, n Score) Score {
	xs[i] = n
	return xs[i]
}
func Run() int {
	xs := make([]Score, 4)
	p := &xs[2]
	v := At(xs[1:3], 1, -9)
	*p += 2
	return int(v)*10 + int(xs[2])
}
func Bad(i int) (n int) {
	defer func() { if recover()!=nil { n=42 } }()
	return int(At(make([]Score, 2, 4), i, 17))
}
func Strings(xs []string) string {
	xs[0] = "abc"
	return xs[0]
}`)
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range m.Functions {
		listing := m.DisassembleFunction(i)
		if f.Name == "At" && (!strings.Contains(listing, "store.index.r") || !strings.Contains(listing, "load.index.r")) ||
			f.Name == "Strings" && strings.Contains(listing, ".index.r") {
			t.Fatal(listing)
		}
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].AsInt() != -97 {
		t.Fatal(out, err)
	}
	for _, i := range []int64{-1, 2, 1 << 32} {
		if err := vm.Run(context.Background(), "Bad", []values.Value{values.Int(i)}, out); err != nil || out[0].R != 42 {
			t.Fatal(i, out, err)
		}
	}
}

func TestLoopExitSeesOldPhi(t *testing.T) {
	vm := compileGoVM(t, `func Run(limit int) int {
		a, b := 2,3
		for i:=0; i<limit; i++ {
			next := a+b
			if next>15 { return a*10+b }
			a, b = b, next
		}
		return a*10+b
	}`)
	out := make([]values.Value, 1)
	for n, want := range []int64{23, 35, 58, 93, 93} {
		if err := vm.Run(context.Background(), "Run", []values.Value{values.Int(int64(n))}, out); err != nil || out[0].AsInt() != want {
			t.Fatal(n, out, err)
		}
	}
}

func TestCompactArithmetic(t *testing.T) {
	m, err := evmcompiler.Compile("ops.go", `
func AddProduct(x,y,z float64) float64 { return z + x*y }
func ProductAdd(x,y,z float64) float64 { return x*y + z }
func DifferenceAdd(x,y,z float64) float64 { return x-y + z }
func Stride(n,step int64) int64 {
	var sum int64
	for k:=int64(1);k<=n;k+=step { sum+=k }
	return sum
}
func Offset(x int64) int64 { return (x - 32768) + 32767 }
func Large(x int64) int64 { return x-(-32768) }
func Swap(x,y int) int { return y-x }
func Forward(n int) int { return Swap(n-1,n) }
`)
	if err != nil {
		t.Fatal(err)
	}
	listing := m.Disassemble()
	for _, op := range []string{"mul.add.f64", "add.mul.f64", "sub.add.f64", "loop.add.le.i64", "add.imm.i64"} {
		if !strings.Contains(listing, op) {
			t.Fatal(op, listing)
		}
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	run := func(name string, want values.Value, args ...values.Value) {
		t.Helper()
		if err := vm.Run(context.Background(), name, args, out); err != nil || out[0] != want {
			t.Fatalf("%s%v: %v, %v; want %v", name, args, out, err, want)
		}
	}
	// The exact product is 1-2^-54. A single hardware FMA would return
	// -2^-54 instead of rounding the product to one before adding -1.
	x, y := values.Float(1+math.Ldexp(1, -27)), values.Float(1-math.Ldexp(1, -27))
	for _, name := range []string{"AddProduct", "ProductAdd"} {
		run(name, values.Float(0), x, y, values.Float(-1))
	}
	run("DifferenceAdd", values.Float(3), values.Float(7), values.Float(5), values.Float(1))
	run("Stride", values.Int(12), values.Int(7), values.Int(3))
	run("Stride", values.Int(0), values.Int(-1), values.Int(1))
	run("Offset", values.Int(math.MaxInt64), values.Int(math.MinInt64))
	run("Large", values.Int(32768), values.Int(0))
	run("Forward", values.Int(1), values.Int(31))
	if err := vm.RunWithBudget(context.Background(), "Stride", []values.Value{values.Int(7), values.Int(0)}, out, evm.Budget{Steps: 140}); !errors.Is(err, evm.ErrSteps) {
		t.Fatal(err)
	}
}

func TestDirectUnaryCalls(t *testing.T) {
	sig := evm.Signature{Params: []*values.Type{values.IntType}, Results: []*values.Type{values.IntType}}
	var seen []uint64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Op", Signature: sig, Direct: evm.DirectUnary(func(n uint64) uint64 {
		seen = append(seen, n)
		if n == 19 {
			cancel()
		}
		return n * 2
	})}}}
	m, err := evmcompiler.Compile("raw.go", `import "h"
	func Call(f func(int)int, x int) int { return f(x) }
	func Run(n int) int { defer h.Op(7); h.Op(5); return Call(h.Op,h.Op(n)) }
	func Cancel() int { return h.Op(19) }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(ctx, "Run", []values.Value{values.Int(3)}, out); err != nil || out[0].R != 12 || !slices.Equal(seen, []uint64{5, 3, 6, 7}) {
		t.Fatal(out, seen, err)
	}
	if err := vm.Run(ctx, "Cancel", nil, out); !errors.Is(err, context.Canceled) || out[0].R != 12 {
		t.Fatal(out, err)
	}
	// Like every DirectBinding, an optional Call overrides the ordinary ABI.
	windowCalls := 0
	pkg.Functions[0].Call = func(c *evm.Call) error {
		windowCalls++
		c.R[0] *= 3
		return nil
	}
	p, err = evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if err := evm.New(p).Run(context.Background(), "Run", []values.Value{values.Int(3)}, out); err != nil || out[0].R != 18 || windowCalls != 2 {
		t.Fatal(out, windowCalls, err)
	}
	pkg.Functions[0].Direct.Op = evm.OpCallHostU
	if _, err := evm.Link(m, pkg); err == nil {
		t.Fatal("accepted incompatible direct opcode")
	}
	pkg.Functions[0].Direct.Op = evm.OpCallHostUU
	pkg.Functions[0].Signature.Params = []*values.Type{values.StringType}
	if _, err := evm.Link(m, pkg); err == nil {
		t.Fatal("accepted paired argument")
	}
}

func TestConditionalLoopUpdates(t *testing.T) {
	m, err := evmcompiler.Compile("join.go", `
func Count(n int) int {
	count := 0
	for i := 1; i <= n; i++ {
		if i%3 != 0 { count++ }
	}
	return count
}
func History(n int) int {
	a, b, history := 1, 2, 0
	for i := 1; i <= n; i++ {
		old := a
		if i%3 != 0 { a, b = b, a+1 }
		history = 2*history + old
	}
	return history*100 + 10*a + b
}
func Inclusive(start, end int64) int64 {
	var sum int64
	for i := start; i <= end; i++ { sum += i }
	return sum
}
func Stepped(end, step int64) int64 {
	var sum int64
	for i := int64(1); i < end; i += step { sum += i }
	return sum
}`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm, out := evm.New(p), make([]values.Value, 1)
	for i, fn := range m.Functions {
		if fn.Name == "Count" && !strings.Contains(m.DisassembleFunction(i), "loop.le.i64") ||
			fn.Name == "Stepped" && !strings.Contains(m.DisassembleFunction(i), "loop.add.lt.i64") {
			t.Fatal(m.DisassembleFunction(i))
		}
	}
	check := func(name string, want int64, args ...values.Value) {
		t.Helper()
		if err := vm.Run(context.Background(), name, args, out); err != nil || out[0].AsInt() != want {
			t.Fatalf("%s%v: got %v, %v, want %d", name, args, out, err, want)
		}
	}
	var a, b, history, count int64 = 1, 2, 0, 0
	for n := int64(0); n <= 23; n++ {
		check("Count", count, values.Int(n))
		check("History", history*100+10*a+b, values.Int(n))
		history = 2*history + a
		if (n+1)%3 != 0 {
			count++
			a, b = b, a+1
		}
	}
	check("Stepped", 12, values.Int(10), values.Int(3))
	check("Inclusive", -1, values.Int(-1), values.Int(0))
	check("Inclusive", 0, values.Int(2), values.Int(1))
	// Incrementing MaxInt64 wraps. The comparison remains true and the
	// loop must eventually fail on its budget.
	args := []values.Value{values.Int(math.MaxInt64), values.Int(math.MaxInt64)}
	if err := vm.RunWithBudget(context.Background(), "Inclusive", args, out, evm.Budget{Steps: 40}); !errors.Is(err, evm.ErrSteps) {
		t.Fatal(err)
	}
}
