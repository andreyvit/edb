package evmcompiler_test

import (
	"context"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestNoOptimization(t *testing.T) {
	const src = `import "port"
func Run(n int, x float64) float64 {
 var a float64
 for i:=0; i<n; i++ {
  a += x * float64(i)
  x += 0.5
 }
 if n != 0 { a += float64(port.Mark(n+1)) }
 return a
}`
	pkg := evm.Package{Path: "port", Functions: []evm.ExternalFunction{{Name: "Mark", Signature: evm.Signature{Params: []*values.Type{values.IntType}, Results: []*values.Type{values.IntType}}, Direct: evm.DirectUnary(func(x uint64) uint64 { return x + 1 })}}}
	for _, raw := range []bool{false, true} {
		m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("example.go", src, pkg)
		if err != nil {
			t.Fatal(err)
		}
		p, err := evm.Link(m, pkg)
		if err != nil {
			t.Fatal(err)
		}
		listing := m.Disassemble()
		if raw {
			for _, fused := range []string{"add.imm.i64", "loop.lt.i64", "add.mul.f64", "call.host.uu"} {
				if strings.Contains(listing, fused) {
					t.Fatalf("raw listing contains %s:\n%s", fused, listing)
				}
			}
			if !strings.Contains(listing, "call.host ") || p.DisassembleFloatLoops() != "" {
				t.Fatalf("unexpected debug program:\n%s", listing)
			}
		}
		var out [1]values.Value
		vm := evm.New(p)
		for _, n := range []int{0, 1, 15, 256} {
			x, want := 1.25, 0.0
			for i := 0; i < n; i++ {
				want += x * float64(i)
				x += 0.5
			}
			if n != 0 {
				want += float64(n + 2)
			}
			err = vm.Run(context.Background(), "Run", []values.Value{values.Int(int64(n)), values.Float(1.25)}, out[:])
			if err != nil || out[0].AsFloat() != want {
				t.Fatalf("raw=%v n=%d: got %v (%v), want %v", raw, n, out[0], err, want)
			}
		}
	}
}
