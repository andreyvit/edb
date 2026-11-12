package bench_test

import (
	"context"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func BenchmarkAnyControl(b *testing.B) {
	const source = `
func Pointer(n int, p *int) int {
 var v any = p
 total := 0
 for i:=0; i<n; i++ { if i&1==0 {v=p}; total += *v.(*int) }
 return total
}
func Function(n int, p *int) int {
 f:=func(x int)int{return x+3}
 total:=0
 for i:=0; i<n; i++ {var v any=f; total+=v.(func(int)int)(i)}
 return total
}`
	for _, name := range []string{"Pointer", "Function"} {
		b.Run(name, func(b *testing.B) {
			m, err := evmcompiler.Compile("any.go", source)
			if err != nil {
				b.Fatal(err)
			}
			p, err := evm.Link(m)
			if err != nil {
				b.Fatal(err)
			}
			vm := evm.New(p)
			vm.Limits.AllocationBytes, vm.Limits.Allocations = ^uint64(0), ^uint64(0)
			cell := values.Int(7)
			args := []values.Value{values.Int(1024), {P: unsafe.Pointer(&cell)}}
			var out [1]values.Value
			want := int64(1024 * 7)
			if name == "Function" {
				want = 1024*1023/2 + 1024*3
			}
			run := func() {
				if err := vm.Run(context.Background(), name, args, out[:]); err != nil || out[0].AsInt() != want {
					b.Fatalf("got %v, %v; want %d", out, err, want)
				}
			}
			run()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				run()
			}
		})
	}
}
