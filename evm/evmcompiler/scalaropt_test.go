package evmcompiler_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestSliceReadReuse(t *testing.T) {
	const src = `import "port"
func Same(a []int, i int) int {
	return a[i] + a[i]
}
func OtherIndex(a []int, i int) int {
	return a[i] + a[i+1]
}
func Store(a []int, i int) int {
	x := a[i]
	alias := a[:]
	alias[i] = x+4
	return x+a[i]
}
func Copy(a []int, i int) int {
	x := a[i]
	copy(a, a[1:])
	return x+a[i]
}
func Clear(a []int, i int) int {
	x := a[i]
	clear(a)
	return x+a[i]
}
func Call(a []int, i int) int {
	x := a[i]
	port.Mutate()
	return x+a[i]
}
func Branch(a []int, i int) int {
	x := a[i]
	if x > 0 {
		a[i] = x*4
	}
	return x+a[i]
}`
	var cells [2]values.Value
	mutate := func(*evm.Call) error {
		cells[0] = values.Int(11)
		return nil
	}
	pkg := evm.Package{Path: "port", Functions: []evm.ExternalFunction{{Name: "Mutate", Call: mutate}}}
	for _, raw := range []bool{false, true} {
		m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("reads.go", src, pkg)
		if err != nil {
			t.Fatal(err)
		}
		for j, f := range m.Functions {
			if !raw && f.Name == "Same" && strings.Count(m.DisassembleFunction(j), "load.index.r") != 1 {
				t.Fatalf("expected one checked load:\n%s", m.DisassembleFunction(j))
			}
		}
		p, err := evm.Link(m, pkg)
		if err != nil {
			t.Fatal(err)
		}
		vm := evm.New(p)
		slice := values.SliceValue(unsafe.Pointer(&cells[0]), 2, 2)
		var out [1]values.Value
		for _, tc := range []struct {
			name string
			want int64
		}{
			{"Same", 6}, {"OtherIndex", 10}, {"Store", 10}, {"Copy", 10},
			{"Clear", 3}, {"Call", 14}, {"Branch", 15},
		} {
			cells = [2]values.Value{values.Int(3), values.Int(7)}
			if err := vm.Run(context.Background(), tc.name, []values.Value{slice, values.Int(0)}, out[:]); err != nil || out[0].AsInt() != tc.want {
				t.Fatalf("%s raw=%v: %v, %v", tc.name, raw, out[0], err)
			}
		}
		if err := vm.Run(context.Background(), "Same", []values.Value{slice, values.Int(-1)}, out[:]); !errors.Is(err, evm.ErrBounds) {
			t.Fatalf("raw=%v: first read should fail bounds check, got %v", raw, err)
		}
	}
}

func TestScalarSimplification(t *testing.T) {
	const src = `var seen int
 func effect(x int)int {seen++;return x}
 func Run(x,y int)int {
  y+=0
  a:=x*y
  b:=x*y
  return a+b+effect(x)*0+seen
 }
 func Trap(x int)int{return (3/x)*0}
 func Fold(x int8)int8{a:=x^x;b:=a+126;c:=b+4;return c}
 func SignedZero(x float64)float64{return x+0}
 func Next(n int) int {
  total := 0
  for i := 0; i < n; i++ {
   total += (i+1)*(i+2)
  }
  return total
 }
 `
	for _, raw := range []bool{false, true} {
		m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("opt.go", src)
		if err != nil {
			t.Fatal(err)
		}
		for k, f := range m.Functions {
			if f.Name == "Next" && strings.Contains(m.DisassembleFunction(k), "loop.lt.i64") == raw {
				t.Fatalf("raw=%v: unexpected back edge:\n%s", raw, m.DisassembleFunction(k))
			}
			if f.Name == "Run" {
				n := strings.Count(m.DisassembleFunction(k), "mul.i64")
				if !raw && n != 1 || raw && n < 3 {
					t.Fatalf("raw=%v: %d multiplies", raw, n)
				}
			}
		}
		p, err := evm.Link(m)
		if err != nil {
			t.Fatal(err)
		}
		vm := evm.New(p)
		var out [1]values.Value
		if err := vm.Run(context.Background(), "Next", []values.Value{values.Int(4)}, out[:]); err != nil || out[0].R != 40 {
			t.Fatalf("raw=%v Next(4): %v %v", raw, out, err)
		}
		for j := 1; j <= 3; j++ {
			if err := vm.Run(context.Background(), "Run", []values.Value{values.Int(-9), values.Int(5)}, out[:]); err != nil || int(out[0].R) != -90+j {
				t.Fatalf("raw=%v side effects: %v %v", raw, out, err)
			}
		}
		if err := vm.Run(context.Background(), "Fold", []values.Value{values.Int(27)}, out[:]); err != nil || int64(out[0].R) != -126 {
			t.Fatalf("int8 wrap: %v %v", out, err)
		}
		if err := vm.Run(context.Background(), "SignedZero", []values.Value{values.Float(math.Copysign(0, -1))}, out[:]); err != nil || math.Signbit(out[0].AsFloat()) {
			t.Fatalf("signed zero: %v %v", out, err)
		}
		if err := vm.Run(context.Background(), "Trap", []values.Value{values.Int(0)}, out[:]); !errors.Is(err, evm.ErrDivideByZero) {
			t.Fatalf("raw=%v removed trap: %v", raw, err)
		}
	}
}
