package bench_test

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func BenchmarkUnaryBinding(b *testing.B) {
	for _, specialized := range []bool{false, true} {
		name := "Direct"
		if specialized {
			name = "Word"
		}
		b.Run(name, func(b *testing.B) {
			fn := evm.ExternalFunction{Name: "Next", Signature: evm.Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{values.Uint64}}}
			if specialized {
				fn.Direct = evm.DirectUnary(func(n uint64) uint64 { return n + 1 })
			} else {
				fn.Direct = evm.DirectBinding{Op: evm.OpCallHostUU, Call: func(_ *evm.Call, n, _ values.Value) (values.Value, error) {
					return values.Uint(n.R + 1), nil
				}}
			}
			pkg := evm.Package{Path: "host", Functions: []evm.ExternalFunction{fn}}
			m, err := evmcompiler.Compile("unary.go", `import "host"
func Run(n uint64) uint64 {
 for i:=0;i<256;i++ { n=host.Next(n) }
 return n
}`, pkg)
			if err != nil {
				b.Fatal(err)
			}
			p, err := evm.Link(m, pkg)
			if err != nil {
				b.Fatal(err)
			}
			vm := evm.New(p)
			in, out := []values.Value{values.Uint(7)}, make([]values.Value, 1)
			if err := vm.Run(context.Background(), "Run", in, out); err != nil || out[0].R != 263 {
				b.Fatal(out, err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := vm.Run(context.Background(), "Run", in, out); err != nil || out[0].R != 263 {
					b.Fatal(out, err)
				}
			}
		})
	}
}
