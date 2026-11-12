package evmcompiler_test

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestLoopInvariantBound(t *testing.T) {
	const src = `func Work(start,span int)int{
  sum:=0
  for i:=start;i<start+span*2;i++{sum+=i}
  return sum
 }
 func Changing(start,span int)int{
  sum:=0
  for i:=start;i<start+span*2;i++{span--;sum+=i}
  return sum
 }`
	for _, raw := range []bool{false, true} {
		m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("bound.go", src)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range m.Functions {
			fused := false
			for _, i := range f.Code {
				if i.Op() == evm.OpLoopLtI64 {
					fused = true
				}
			}
			if fused != (f.Name == "Work" && !raw) {
				t.Fatalf("%s raw=%v fused=%v", f.Name, raw, fused)
			}
		}
		p, err := evm.Link(m)
		if err != nil {
			t.Fatal(err)
		}
		vm := evm.New(p)
		var out [1]values.Value
		for n := -1; n <= 8; n++ {
			for _, name := range []string{"Work", "Changing"} {
				sum := 0
				span := n
				for i := -3; i < -3+span*2; i++ {
					sum += i
					if name == "Changing" {
						span--
					}
				}
				if err := vm.Run(context.Background(), name, []values.Value{values.Int(-3), values.Int(int64(n))}, out[:]); err != nil || int(out[0].R) != sum {
					t.Fatalf("%s raw=%v n=%d: %v %v", name, raw, n, out, err)
				}
			}
		}
	}
}
