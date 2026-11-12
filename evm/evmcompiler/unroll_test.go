package evmcompiler_test

import (
	"context"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestFixedTrips(t *testing.T) {
	const src = `func Fixed(seed int)(int,int){x,i:=seed,-1;for ;i<=2;i++{x=(x+i)*3};return x,i}
 func Early(n int)int{x:=0;for i:=0;i<4;i++{if i==n{break};x+=i};return x}
 func Variable(n int)int{x:=0;for i:=0;i<n;i++{x+=i};return x}
 func Large(n int)int{x:=n;for i:=0;i<128;i++{x+=i};return x}`
	for _, raw := range []bool{false, true} {
		m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("trip.go", src)
		if err != nil {
			t.Fatal(err)
		}
		p, err := evm.Link(m)
		if err != nil {
			t.Fatal(err)
		}
		vm := evm.New(p)
		for seed := -3; seed <= 6; seed++ {
			args := []values.Value{values.Int(int64(seed))}
			var out [2]values.Value
			err := vm.Run(context.Background(), "Fixed", args, out[:])
			x := seed
			for i := -1; i <= 2; i++ {
				x = (x + i) * 3
			}
			if err != nil || out[0].AsInt() != int64(x) || out[1].R != 3 {
				t.Fatalf("fixed seed %d: %v %v", seed, out, err)
			}
			x = 0
			for i := 0; i < 4; i++ {
				if i == seed {
					break
				}
				x += i
			}
			if err := vm.Run(context.Background(), "Early", args, out[:1]); err != nil || out[0].AsInt() != int64(x) {
				t.Fatalf("early seed %d: %v %v", seed, out, err)
			}
		}
		if !raw {
			for j, f := range m.Functions {
				list := m.DisassembleFunction(j)
				switch f.Name {
				case "Fixed":
					if strings.Contains(list, "loop.le.i64") {
						t.Fatal(list)
					}
				case "Variable", "Large":
					if !strings.Contains(list, "loop.lt.i64") {
						t.Fatal(list)
					}
				}
			}
		}
	}
}
