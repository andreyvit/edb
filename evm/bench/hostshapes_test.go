package bench_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

// Compare each register form to the same callback through Argument/SetResult.
// Each invocation makes 128 calls. SmallHost measures the five application patterns.
func BenchmarkHostShapes(b *testing.B) {
	for op := evm.OpCallHostU; op <= evm.OpCallHostMethodPV; op++ {
		if _, _, ok := op.HostRegisters(); !ok {
			continue
		}
		b.Run(strings.TrimPrefix(op.String(), "call.host."), func(b *testing.B) {
			for _, mode := range []string{"General", "Direct"} {
				b.Run(mode, func(b *testing.B) { benchmarkHostShape(b, op, mode == "Direct") })
			}
		})
	}
}

func benchmarkHostShape(b *testing.B, op evm.Opcode, direct bool) {
	cell := values.NewStructShape("h.Cell")
	cellPtr := values.NewPointerShape(cell)
	pkg := evm.Package{Path: "h", Types: []evm.TypeDecl{{Name: "Cell", Shape: cell}, {Shape: cellPtr}}}
	suffix := strings.TrimPrefix(op.String(), "call.host.")
	method := strings.HasPrefix(suffix, "method.")
	suffix = strings.TrimPrefix(suffix, "method.")
	types := map[byte]*values.Type{'s': values.StringType, 'u': values.Uint64, 'p': cellPtr.Type}
	names := map[byte]string{'s': "string", 'u': "uint64", 'p': "*h.Cell"}
	arg := map[byte]string{'s': "name", 'u': "word", 'p': "cell"}
	ptr := values.Value{P: cell.New()}
	result := suffix[len(suffix)-1]
	var sig evm.Signature
	if result != 'v' {
		sig.Results = []*values.Type{types[result]}
	}
	var args []string
	for _, k := range []byte(suffix[:len(suffix)-1]) {
		sig.Params = append(sig.Params, types[k])
		args = append(args, arg[k])
	}
	target := "h.F"
	n := len(args)
	if method {
		target = "cell.F"
		n++
	}
	callback := func(_ *evm.Call, x, y values.Value) (values.Value, error) {
		// Reading R observes the scalar or length. A pointer contributes one.
		sum := x.R + y.R
		if x.P != nil {
			sum++
		}
		if y.P != nil {
			sum++
		}
		switch result {
		case 's':
			if sum&1 != 0 {
				return values.String("odd"), nil
			}
			return values.String("even"), nil
		case 'p':
			if sum&1 != 0 {
				return ptr, nil
			}
			return values.Value{}, nil
		default:
			return values.Uint(sum), nil
		}
	}
	general := func(c *evm.Call) error {
		var x, y values.Value
		if n > 0 {
			x = c.Argument(0)
		}
		if n > 1 {
			y = c.Argument(1)
		}
		v, err := callback(c, x, y)
		if err != nil {
			return err
		}
		if result == 's' {
			if err := c.ChargeAllocation(v.R); err != nil {
				return err
			}
		}
		if result != 'v' {
			c.SetResult(0, v)
		}
		return nil
	}
	var d evm.DirectBinding
	if direct {
		d = evm.DirectBinding{Op: op, Call: callback}
	}
	if method {
		pkg.Methods = []evm.ExternalMethod{{Name: "F", Receiver: cellPtr.Type, Signature: sig, Call: general, Direct: d}}
	} else {
		pkg.Functions = []evm.ExternalFunction{{Name: "F", Signature: sig, Call: general, Direct: d}}
	}
	src := fmt.Sprintf(`import "h"
func Run(cell *h.Cell,word uint64,name string) %s {
	var result %s
	for i:=0;i<128;i++ {result=%s(%s)}
	return result
}`, names[result], names[result], target, strings.Join(args, ","))
	if result == 'v' {
		src = fmt.Sprintf(`import "h"
func Run(cell *h.Cell,word uint64,name string) {
	for i:=0;i<128;i++ {%s(%s)}
}`, target, strings.Join(args, ","))
	}
	m, err := evmcompiler.Compile("shapes.go", src, pkg)
	if err != nil {
		b.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		b.Fatal(err)
	}
	vm := evm.New(p)
	vm.Limits.Allocations = ^uint64(0)
	vm.Limits.AllocationBytes = ^uint64(0)
	inputs := []values.Value{ptr, values.Uint(7), values.String("word")}
	byBank := map[byte]values.Value{'p': inputs[0], 'u': inputs[1], 's': inputs[2]}
	var native [2]values.Value
	j := 0
	if method {
		native[0] = ptr
		j++
	}
	for _, k := range []byte(suffix[:len(suffix)-1]) {
		native[j] = byBank[k]
		j++
	}
	want, _ := callback(nil, native[0], native[1])
	var out [1]values.Value
	results := out[:]
	if result == 'v' {
		results, want = nil, values.Value{}
	}
	if err := vm.Run(context.Background(), "Run", inputs, results); err != nil || out[0] != want {
		b.Fatal(out, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := vm.Run(context.Background(), "Run", inputs, results); err != nil || out[0] != want {
			b.Fatal(out, err)
		}
	}
}
