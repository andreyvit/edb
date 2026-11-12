package bench_test

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/bench/internal/optwork"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

var optCases = []struct {
	name string
	fn   func(int, int) int
}{
	{"Digest", optwork.Digest}, {"Tariff", optwork.Tariff}, {"Histogram", optwork.Histogram}, {"Queue", optwork.Queue}, {"Channels", optwork.Channels}, {"Rounds", optwork.Rounds}, {"Stencil", optwork.Stencil},
}

func optimizerProgram(t testing.TB) *evm.Program {
	t.Helper()
	m, err := evmcompiler.Compile("kernels.go", optwork.Source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestOptimizerKernels(t *testing.T) {
	vm := evm.New(optimizerProgram(t))
	for _, k := range optCases {
		for _, n := range []int{0, 1, 3, 16, 37, 512} {
			for seed := range 4 {
				var out [1]values.Value
				err := vm.Run(context.Background(), k.name, []values.Value{values.Int(int64(n)), values.Int(int64(seed))}, out[:])
				if err != nil || int(out[0].R) != k.fn(n, seed) {
					t.Fatalf("%s(%d,%d): %v, %v", k.name, n, seed, out, err)
				}
			}
		}
	}
}

func BenchmarkOptimizer(b *testing.B) {
	p := optimizerProgram(b)
	for _, k := range optCases {
		b.Run(k.name, func(b *testing.B) {
			const n = 512
			vm := evm.New(p)
			ctx := context.Background()
			var out [1]values.Value
			var want [4]int
			for i := range want {
				want[i] = k.fn(n, i)
			}
			args := [2]values.Value{values.Int(n), {}}
			// Warm the callable window and keep validation identical in both binaries.
			if err := vm.Run(ctx, k.name, args[:], out[:]); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for j := range b.N {
				if err := vm.Reset(); err != nil {
					b.Fatal(err)
				}
				seed := j & 3
				args[1] = values.Int(int64(seed))
				if err := vm.Run(ctx, k.name, args[:], out[:]); err != nil || int(out[0].R) != want[seed] {
					b.Fatalf("seed %d: %v, %v", seed, out, err)
				}
			}
		})
	}
}
