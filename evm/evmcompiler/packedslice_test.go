package evmcompiler_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestPackedSlices(t *testing.T) {
	const src = `import "h"
 type node struct{ n int }
 func Words(n int)int {
  a:=make([]int,n,n+2); b:=a[:]; a[1]=-7; a=append(a,3,8)
  copy(a[2:],a[1:]); (*[3]int)(b)[0]=11; clear(a[3:4]); return b[0]+a[2]+a[n+1]
 }
 func Bytes(n int)int {b:=make([]byte,n); b=append(b,"hello"...);copy(b[1:],"abc");return int(b[1])+int(b[n+4])}
 func Ptrs(n int)int {a:=make([]*node,n); for i:=0;i<n;i++ {a[i]=&node{n:i}}; b:=append(a[:1:1],a...);clear(a);h.GC();return b[n].n}
 func Alias(x []int,n int)int{a:=make([]int,n);if n>1 {a=x};a[0]=27;return a[0]}
 func Escape(n int)[]int{a:=make([]int,n);a[n-1]=-17;return a}
 func Address(n int)*int{a:=make([]int,n);a[1]=19;return &a[1]}
 func FromText(n int)int{a:=append([]byte(nil),"text"...);return int(a[n])}
 func Limit(n int)int{a:=make([]int,n);return a[0]}
 func Empty(n int)bool{a:=make([]*node,n);return a[n:]!=nil}
 `
	pkg := evm.Package{Path: "h", Functions: []evm.ExternalFunction{{Name: "GC", Call: func(*evm.Call) error { runtime.GC(); return nil }}}}
	for _, raw := range []bool{false, true} {
		m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("packed.go", src, pkg)
		if err != nil {
			t.Fatal(err)
		}
		if !raw {
			for _, op := range []string{"store.index.word", "load.index.byte", "store.index.ptr", "load.index.ptr"} {
				if !strings.Contains(m.Disassemble(), op) {
					t.Fatalf("missing %s", op)
				}
			}
		}
		p, err := evm.Link(m, pkg)
		if err != nil {
			t.Fatal(err)
		}
		vm := evm.New(p)
		var out [1]values.Value
		for _, c := range []struct {
			name    string
			n, want int
		}{{"Words", 8, 7}, {"Bytes", 8, 208}, {"Ptrs", 17, 16}, {"Empty", 0, 1}, {"FromText", 0, 't'}} {
			if err := vm.Run(context.Background(), c.name, []values.Value{values.Int(int64(c.n))}, out[:]); err != nil || int(out[0].R) != c.want {
				t.Fatalf("raw=%v %s: got %v, %v", raw, c.name, out, err)
			}
		}
		cells := [2]values.Value{}
		if err := vm.Run(context.Background(), "Alias", []values.Value{values.SliceValue(unsafe.Pointer(&cells[0]), 2, 2), values.Int(2)}, out[:]); err != nil || cells[0].R != 27 {
			t.Fatalf("external alias %v %v", cells, err)
		}
		if err := vm.Run(context.Background(), "Escape", []values.Value{values.Int(8)}, out[:]); err != nil || int64(values.CellSlice(out[0].P, 8)[7].R) != -17 {
			t.Fatalf("returned slice %v %v", out, err)
		}
		if err := vm.Run(context.Background(), "Address", []values.Value{values.Int(2)}, out[:]); err != nil || (*values.Value)(out[0].P).R != 19 {
			t.Fatalf("escaped address %v %v", out, err)
		}
		vm = evm.New(p)
		vm.Limits.AllocationBytes = 1024
		err = vm.Run(context.Background(), "Limit", []values.Value{values.Int(100)}, out[:])
		if raw && !errors.Is(err, evm.ErrAllocation) || !raw && err != nil {
			t.Fatalf("raw=%v budget: %v", raw, err)
		}
		if err := evm.New(p).Run(context.Background(), "Limit", []values.Value{values.Int(0)}, out[:]); !errors.Is(err, evm.ErrBounds) {
			t.Fatalf("bounds: %v", err)
		}
	}
}
