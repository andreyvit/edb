package evm_test

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func arithmeticVM(tb testing.TB, compact bool) *evm.VM {
	tb.Helper()
	m, err := (evmcompiler.Options{NoOptimization: !compact}).Compile("example.go", "func Arithmetic(x, y uint8) uint8 { return x + y }")
	if err != nil {
		tb.Fatal(err)
	}
	want := 4
	if compact {
		want = 3
	}
	if f := m.Functions[0]; f.Name != "Arithmetic" || len(f.Code) != want || f.Code[1].Op() != evm.OpConvert {
		tb.Fatal("check the arithmetic schedules in chapter 10")
	}
	p, err := evm.Link(m)
	if err != nil {
		tb.Fatal(err)
	}
	return evm.New(p)
}

func TestArithmeticSchedules(t *testing.T) {
	for _, compact := range []bool{false, true} {
		vm := arithmeticVM(t, compact)
		var args [2]values.Value
		var out [1]values.Value
		for x := range 256 {
			for y := range 256 {
				args[0], args[1] = values.Uint(uint64(x)), values.Uint(uint64(y))
				err := vm.Run(context.Background(), "Arithmetic", args[:], out[:])
				if err != nil || out[0] != values.Uint(uint64(uint8(x+y))) {
					t.Fatalf("compact=%t (%d, %d): %+v, %v", compact, x, y, out[0], err)
				}
			}
		}
	}
}

func BenchmarkArithmeticSchedules(b *testing.B) {
	for _, tc := range []struct {
		name    string
		compact bool
	}{{"unoptimized", false}, {"default", true}} {
		b.Run(tc.name, func(b *testing.B) {
			vm := arithmeticVM(b, tc.compact)
			args := []values.Value{values.Uint(250), values.Uint(10)}
			out := make([]values.Value, 1)
			ctx := context.Background()
			if err := vm.Run(ctx, "Arithmetic", args, out); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := vm.Run(ctx, "Arithmetic", args, out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
