package evmcompiler_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmasm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

// Execute every suffix through a real script. Retaining the inputs across the
// call catches both destructive copies and clobbering an unrelated register bank.
func TestDirectHostMatrix(t *testing.T) {
	row := values.NewStructShape("h.Row")
	rowPtr := values.NewPointerShape(row)
	ptr := rowPtr.Type
	srcType := map[byte]string{'u': "uint64", 's': "string", 'p': "*h.Row"}
	vmType := map[byte]*values.Type{'u': values.Uint64, 's': values.StringType, 'p': ptr}
	v := func(kind byte, n int) values.Value {
		switch kind {
		case 'u':
			return values.Uint(uint64(31337 + n))
		case 's':
			return values.String(strings.Repeat("guest", n+1))
		default:
			return values.Value{P: row.New()}
		}
	}
	for _, method := range []bool{false, true} {
		groups := []string{"", "u", "s", "p"}
		if !method {
			for _, i := range "usp" {
				for _, j := range "usp" {
					groups = append(groups, string([]rune{i, j}))
				}
			}
		}
		for _, args := range groups {
			for _, result := range []byte("uspv") {
				suffix := args + string(result)
				name := "call.host."
				if method {
					name += "method."
				}
				name += suffix
				t.Run(name, func(t *testing.T) {
					var op evm.Opcode
					for i := evm.OpCallHostU; i <= evm.OpCallHostMethodPV; i++ {
						if i.String() == name {
							op = i
						}
					}
					if op == 0 {
						t.Fatal("missing opcode")
					}
					var params, returns, body []string
					var inputs, keep []values.Value
					var sig evm.Signature
					if result != 'v' {
						sig.Results = []*values.Type{vmType[result]}
						returns = append(returns, srcType[result])
					}
					register := func(label string, k byte) {
						params = append(params, label+" "+srcType[k])
						returns = append(returns, srcType[k])
						inputs = append(inputs, v(k, len(inputs)))
						keep = append(keep, inputs[len(inputs)-1])
						body = append(body, label)
					}
					target := "h.F"
					var names []string
					if method {
						register("recv", 'p')
						target = "recv.F"
					}
					for j, k := range []byte(args) {
						label := fmt.Sprintf("arg%d", j)
						register(label, k)
						names = append(names, label)
						sig.Params = append(sig.Params, vmType[k])
					}
					dst := v(result, 5)
					direct := evm.DirectBinding{Op: op, Call: func(c *evm.Call, x, y values.Value) (values.Value, error) {
						got := [2]values.Value{x, y}
						for j := range got {
							var want values.Value
							if j < len(inputs) {
								want = inputs[j]
							}
							if got[j] != want {
								return values.Value{}, fmt.Errorf("argument %d: %v vs %v", j, got[j], want)
							}
						}
						return dst, nil
					}}
					pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Row", Shape: row}, {Shape: rowPtr}}}
					if method {
						pkg.Methods = []evm.ExternalMethod{{Name: "F", Receiver: ptr, Signature: sig, Direct: direct}}
					} else {
						pkg.Functions = []evm.ExternalFunction{{Name: "F", Signature: sig, Direct: direct}}
					}
					call := target + "(" + strings.Join(names, ",") + ")"
					code := "return " + strings.Join(append([]string{call}, body...), ",")
					if result == 'v' {
						code = call + "; return " + strings.Join(body, ",")
					}
					source := fmt.Sprintf("import \"h\"\nfunc Run(%s)(%s){ %s }", strings.Join(params, ","), strings.Join(returns, ","), code)
					m, err := evmcompiler.Compile("matrix.go", source, pkg)
					if err != nil {
						t.Fatal(err)
					}
					var found bool
					for _, fn := range m.Functions {
						for _, ins := range fn.Code {
							if ins.Op() == op {
								found = true
							}
						}
					}
					if !found {
						t.Fatal(m.Disassemble())
					}
					p, err := evm.Link(m, pkg)
					if err != nil {
						t.Fatal(err)
					}
					want := keep
					if result != 'v' {
						want = append([]values.Value{dst}, keep...)
					}
					got := make([]values.Value, len(want))
					if err := evm.New(p).Run(context.Background(), "Run", inputs, got); err != nil {
						t.Fatal(err)
					}
					for j := range got {
						if got[j] != want[j] {
							t.Fatalf("result %d = %v, want %v\n%s", j, got[j], want[j], m.Disassemble())
						}
					}
					// Independently spell the operands. String dominates a tied A.
					bank := func(k byte) byte { return map[byte]byte{'u': 'r', 's': 'v', 'p': 'p'}[k] }
					a := bank(result)
					b := "0"
					if method {
						b = "p1"
					} else if len(args) > 0 {
						b = fmt.Sprintf("%c1", bank(args[len(args)-1]))
					}
					if method && len(args) == 1 || len(args) == 2 {
						if args[0] == 's' || result == 'v' {
							a = bank(args[0])
						}
					}
					as := "0"
					if a != 0 {
						as = fmt.Sprintf("%c0", a)
					}
					line := fmt.Sprintf("%s %s %s imp0", name, as, b)
					imp := "import h.F "
					if method {
						imp += "method *h.Row "
					}
					if len(args) == 0 {
						imp += "- "
					} else {
						for _, k := range []byte(args) {
							imp += vmType[k].Name() + " "
						}
					}
					resultName := "-"
					if result != 'v' {
						resultName = vmType[result].Name()
					}
					asm, err := evmasm.Assemble(imp+"-> "+resultName+"\nfunc Run - -> -\nregisters 2 2\n"+line+"\nret\nend", ptr)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := evm.Link(asm, pkg); err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(strings.Join(strings.Fields(asm.Disassemble()), " "), line) {
						t.Fatal(asm.Disassemble())
					}
				})
			}
		}
	}
}

func TestDirectCancelAndStringLifetime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	sig := evm.Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{values.StringType}}
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Name", Signature: sig, Direct: evm.DirectBinding{Op: evm.OpCallHostUS, Call: func(c *evm.Call, x, _ values.Value) (values.Value, error) {
		calls++
		if c.Position.Line == 0 || c.Context != ctx {
			t.Fatal("missing call metadata")
		}
		if x.R == 13 {
			cancel()
		}
		return values.String(strings.Repeat("b", int(x.R))), nil
	}}}}}
	m, err := evmcompiler.Compile("test.go", `import "h"
func Direct(x uint64) string{return h.Name(x)}
func apply(f func(uint64)string,x uint64) string {return f(x)}
func Indirect(x uint64)string{return apply(h.Name,x)}
func Deferred(){defer h.Name(6)}
func Discard(){h.Name(6)}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	var out [1]values.Value
	for _, name := range []string{"Direct", "Indirect", "Deferred", "Discard"} {
		var args, results []values.Value
		if name == "Direct" || name == "Indirect" {
			args = []values.Value{values.Uint(6)}
			results = out[:]
		}
		if err := vm.Run(ctx, name, args, results); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		if out[0].AsString() != "bbbbbb" {
			t.Fatal("lost backing storage")
		}
		if err := vm.RunWithBudget(ctx, name, args, results, evm.Budget{AllocationBytes: 5}); !errors.Is(err, evm.ErrAllocation) {
			t.Fatalf("%s: %v", name, err)
		}
		if err := vm.Reset(); err != nil {
			t.Fatal(err)
		}
	}
	out[0] = values.String("saved")
	if err := vm.Run(ctx, "Direct", []values.Value{values.Uint(13)}, out[:]); !errors.Is(err, context.Canceled) || out[0].AsString() != "saved" {
		t.Fatal(out, err)
	}
	n := calls
	vm.Reset()
	if err := vm.Run(ctx, "Direct", []values.Value{values.Uint(7)}, out[:]); !errors.Is(err, context.Canceled) || calls != n {
		t.Fatal(err, calls)
	}
}

func TestDirectOverlappingOperands(t *testing.T) {
	cell := values.NewStructShape("h.Cell")
	cellPtr := values.NewPointerShape(cell)
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Cell", Shape: cell}, {Shape: cellPtr}}}
	// Both inputs occupy V0. The wrapper must see the old string twice,
	// even though the raw result replaces its length and kills its pointer.
	pkg.Functions = []evm.ExternalFunction{{Name: "Compare", Signature: evm.Signature{Params: []*values.Type{values.StringType, values.StringType}, Results: []*values.Type{values.Int64}}, Direct: evm.DirectBinding{Op: evm.OpCallHostSSU, Call: func(c *evm.Call, x, y values.Value) (values.Value, error) {
		if x.AsString() != "payload" || x != y {
			return values.Value{}, fmt.Errorf("broken overlap")
		}
		return values.Value{R: 17, P: cell.New()}, nil // only the declared lane is a result
	}}}}
	m, err := evmasm.Assemble(`import h.Compare string string -> i64
func Run string -> i64
registers 1 1
call.host.ssu v0 v0 imp0
jnil p0 done
const r0 0
done:
ret
end`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	var out [1]values.Value
	if err := evm.New(p).Run(context.Background(), "Run", []values.Value{values.String("payload")}, out[:]); err != nil || out[0].R != 17 {
		t.Fatal(out, err)
	}
	for _, invalid := range []evm.Instruction{evm.ABC(evm.OpCallHostSSU, 0, 0, 1), evm.ABC(evm.OpCallHostSSU, 1, 0, 0), evm.ABC(evm.OpCallHostSSU, 0, 1, 0), evm.ABC(evm.OpCallHostSSP, 0, 0, 0), evm.ABC(evm.OpCallHostMethodSU, 0, 0, 0)} {
		m.Functions[0].Code[0] = invalid
		if _, err := evm.Link(m, pkg); err == nil {
			t.Fatalf("accepted %#016x", invalid)
		}
	}
}

func TestDirectHostShapes(t *testing.T) {
	for _, tc := range []struct {
		sig      evm.Signature
		op       evm.Opcode
		variadic bool
	}{
		{evm.Signature{}, evm.OpCallHostU, false},
		{evm.Signature{Results: []*values.Type{values.ErrorType}}, evm.OpCallHostS, false},
		{evm.Signature{Results: []*values.Type{values.Int64, values.Int64}}, evm.OpCallHostU, false},
		{evm.Signature{Params: []*values.Type{values.AnyType}, Results: []*values.Type{values.Int64}}, evm.OpCallHostSU, false},
		{evm.Signature{Params: []*values.Type{values.Int64}, Results: []*values.Type{values.Int64}}, evm.OpCallHostUU, true},
		{evm.Signature{Results: []*values.Type{values.NewType("snapshot", values.Pointer)}}, evm.OpCallHostP, false},
		{evm.Signature{Results: []*values.Type{values.NewType("slice", values.Pair)}}, evm.OpCallHostS, false},
	} {
		pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Invalid", Signature: tc.sig, Variadic: tc.variadic, Direct: evm.DirectBinding{Op: tc.op}}}}
		if _, err := evmcompiler.Compile("bad.go", `func Run(){}`, pkg); err == nil {
			t.Fatal("accepted", tc)
		}
	}
	for _, code := range []string{"call.host.u r0 1 imp0", "call.host.u r0 0 imp65536", "call.host.method.su r0 p1 imp0"} {
		if _, err := evmasm.Assemble("func Run - -> -\nregisters 2 2\n" + code + "\nret\nend"); err == nil {
			t.Fatal(code)
		}
	}
	m, err := evmasm.Assemble("func Run - -> -\nregisters 1 0\ncall.host.u r0 0 imp65535\nret\nend")
	if err != nil || m.Functions[0].Code[0].C() != 65535 {
		t.Fatal(m, err)
	}
}

func TestTiedParameterLifetime(t *testing.T) {
	sig := evm.Signature{Params: []*values.Type{values.StringType, values.IntType}, Results: []*values.Type{values.IntType}}
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Score", Signature: sig, Direct: evm.DirectBinding{Op: evm.OpCallHostSUU, Call: func(c *evm.Call, s, i values.Value) (values.Value, error) { return values.Uint(s.R + i.R), nil }}}}}
	m, err := evmcompiler.Compile("live.go", `import "h"
func Once(s string,i int)int{return h.Score(s,i)}
func Repeat(s string) int {
	sum:=0
	for i:=0;i<4;i++ {sum+=h.Score(s,i)}
	return sum
}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	for i, f := range m.Functions {
		if f.Name == "Once" && (len(f.Code) != 2 || f.Code[0].Op() != evm.OpCallHostSUU) {
			t.Fatal(m.DisassembleFunction(i))
		}
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	var out [1]values.Value
	if err := evm.New(p).Run(context.Background(), "Repeat", []values.Value{values.String("abc")}, out[:]); err != nil || out[0].R != 18 {
		t.Fatal(out, err)
	}
}

func TestDirectMethodThroughOrdinaryABI(t *testing.T) {
	counter := values.NewStructShape("h.Counter", values.Field{Name: "N", Shape: &values.Shape{Type: values.Uint64, Kind: values.UintKind, Bits: 64, Cells: 1}})
	counterPtr := values.NewPointerShape(counter)
	var seen []uint64
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Counter", Shape: counter}, {Shape: counterPtr}}, Methods: []evm.ExternalMethod{{
		Name: "Add", Receiver: counterPtr.Type, Signature: evm.Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{values.Uint64}},
		Direct: evm.DirectBinding{Op: evm.OpCallHostMethodUU, Call: func(c *evm.Call, recv, delta values.Value) (values.Value, error) {
			seen = append(seen, delta.R)
			sum := values.Uint(counter.GetField(recv.P, 0).R + delta.R)
			counter.SetField(recv.P, 0, sum)
			return sum, nil
		}},
	}}}
	m, err := evmcompiler.Compile("methods.go", `import "h"
type Adder interface{Add(uint64)uint64}
func via(a Adder)uint64{return a.Add(2)}
func Run(c *h.Counter)uint64 {
	defer c.Add(8)
	bound:=c.Add
	return via(c)+bound(3)+c.Add(4)
}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	list := m.Disassemble()
	if !strings.Contains(list, "call.virt") || !strings.Contains(list, "call.host.method.uu") {
		t.Fatal(list)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	arg := values.Value{P: counter.New()}
	out := make([]values.Value, 1)
	if err := evm.New(p).Run(context.Background(), "Run", []values.Value{arg}, out); err != nil || out[0].R != 16 || counter.GetField(arg.P, 0).R != 17 || !slices.Equal(seen, []uint64{2, 3, 4, 8}) {
		t.Fatal(out, seen, err)
	}
}
