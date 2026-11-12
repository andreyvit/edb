package evmcompiler_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestDirectVoidCalls(t *testing.T) {
	for _, window := range []bool{false, true} {
		for _, noOptimization := range []bool{false, true} {
			t.Run(fmt.Sprintf("window=%v/unoptimized=%v", window, noOptimization), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var calls int
				failure := errors.New("host failure")
				pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Write", Signature: evm.Signature{Params: []*values.Type{values.StringType, values.IntType}}, Direct: evm.DirectBinding{Op: evm.OpCallHostSUV, Window: window, Call: func(c *evm.Call, x, y values.Value) (values.Value, error) {
					calls++
					if c.Context != ctx || c.Position.File != "void.go" || c.Position.Line == 0 {
						t.Fatal("missing call metadata")
					}
					if x.AsString() != "kept alive" {
						t.Fatal("corrupt string argument")
					}
					if window && (c.Argument(0) != x || c.Argument(1) != y) {
						t.Fatal("corrupt argument window")
					}
					switch y.R {
					case 1:
						return values.Value{}, failure
					case 2:
						cancel()
					case 3:
						return values.Value{}, c.ChargeAllocation(4096)
					}
					// A void call must never copy this into the caller's registers.
					return values.String("ignored result"), nil
				}}}}}
				source := `import "h"
func forward(s string, n int) {h.Write(s,n)}
func indirect(f func(string,int), s string, n int) {f(s,n)}
func Run(s string,n int)(string,int) {
 for i:=0;i<3;i++ {forward(s,n)}
 indirect(h.Write,s,n)
 defer h.Write(s,n)
 return s,n
}`
				m, err := (evmcompiler.Options{NoOptimization: noOptimization}).Compile("void.go", source, pkg)
				if err != nil {
					t.Fatal(err)
				}
				if !noOptimization {
					for _, f := range m.Functions {
						if f.Name == "Run" {
							found := false
							for _, ins := range f.Code {
								found = found || ins.Op() == evm.OpCallHostSUV
							}
							if !found {
								t.Fatal("forwarding wrapper was not folded", m.Disassemble())
							}
						}
					}
				}
				p, err := evm.Link(m, pkg)
				if err != nil {
					t.Fatal(err)
				}
				vm := evm.New(p)
				in := []values.Value{values.String(strings.Join([]string{"kept", "alive"}, " ")), values.Int(0)}
				var out [2]values.Value
				if err := vm.Run(ctx, "Run", in, out[:]); err != nil || out[0] != in[0] || out[1] != in[1] || calls != 5 {
					t.Fatal(out, calls, err)
				}
				for _, mode := range []int64{1, 3, 2} {
					before := calls
					in[1] = values.Int(mode)
					want := map[int64]error{1: failure, 2: context.Canceled, 3: evm.ErrAllocation}[mode]
					if err := vm.RunWithBudget(ctx, "Run", in, out[:], evm.Budget{AllocationBytes: 2048}); !errors.Is(err, want) {
						t.Fatalf("mode %d: %v, want %v", mode, err, want)
					}
					if calls != before+1 {
						t.Fatalf("failed call continued execution: %d calls", calls-before)
					}
					if err := vm.Reset(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestVoidHostValidation(t *testing.T) {
	for _, sig := range []evm.Signature{
		{Results: []*values.Type{values.IntType}},
		{Params: []*values.Type{values.AnyType}},
		{Params: []*values.Type{values.StringType, values.IntType, values.IntType}},
	} {
		pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "Invalid", Signature: sig, Direct: evm.DirectBinding{Op: evm.OpCallHostSV}}}}
		if err := evm.ValidateDirectBindings([]evm.Package{pkg}); err == nil {
			t.Fatal("invalid void signature accepted", sig)
		}
	}
}
