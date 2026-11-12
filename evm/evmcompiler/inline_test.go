package evmcompiler_test

import (
	"context"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestInlineLeafBudget(t *testing.T) {
	const defs = `func bump(x uint8)uint8{return x+7}
 func div(x uint8)uint8{return 19/x}
 func load(p *uint8)uint8{return *p}
 func run(x uint8)uint8{`
	src := defs + strings.Repeat("x=bump(x)\n", 20) + "return div(x)+load(&x)}"
	for _, raw := range []bool{false, true} {
		m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("inline.go", src)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, f := range m.Functions {
			if f.Name != "run" {
				continue
			}
			for _, ins := range f.Code {
				if ins.Op() == evm.OpCallFunc {
					count++
				}
			}
		}
		if raw && count != 22 || !raw && (count <= 2 || count >= 22) {
			t.Fatalf("raw=%v emitted %d calls", raw, count)
		}
		p, err := evm.Link(m)
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range []uint8{0, 3, 113, 250, 255} {
			var out [1]values.Value
			err = evm.New(p).Run(context.Background(), "run", []values.Value{values.Uint(uint64(x))}, out[:])
			y := x + 140
			if err != nil || out[0].R != uint64(19/y+y) {
				t.Fatalf("raw=%v x=%d: %v, %v", raw, x, out, err)
			}
		}
	}
}
