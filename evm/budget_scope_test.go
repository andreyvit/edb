package evm_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestWallAndSectionBudgets(t *testing.T) {
	for _, tc := range []struct {
		name, body     string
		outer, inner   uint64
		whole, section time.Duration
		wait           bool
		want           error
	}{
		{"run deadline", "for {}", 1 << 48, 0, 5 * time.Millisecond, 0, false, context.DeadlineExceeded},
		{"section deadline", "host.Open(); for {}", 1 << 48, 0, 0, 5 * time.Millisecond, false, context.DeadlineExceeded},
		{"section steps", "host.Open(); for {}", 1 << 48, 37, 0, 0, false, evm.ErrSteps},
		{"external crosses deadline", "host.Open(); host.Wait(); host.End()", 50000, 0, 0, 10 * time.Millisecond, true, context.DeadlineExceeded},
		{"return restores parent", "host.Open(); host.End(); host.Wait(); for i:=0;i<1000;i++ { sum+=i }", 100000, 10, 0, 10 * time.Millisecond, true, nil},
		{"sections keep charging parent", "for { host.Open(); host.End() }", 1000, 50, 0, time.Second, false, evm.ErrSteps},
		{"nested parent fuel wins", "host.Open(); host.Open(); for {}", 100000, 37, 0, 0, false, evm.ErrSteps},
		{"caller deadline wins", "host.Open(); for {}", 1 << 48, 0, 8 * time.Millisecond, time.Minute, false, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stack []*evm.BudgetScope
			p := evm.Package{Path: "host", Functions: []evm.ExternalFunction{
				{Name: "Open", Call: func(c *evm.Call) error {
					n := tc.inner
					if len(stack) > 0 {
						n = 1 << 40
					}
					stack = append(stack, c.NarrowBudget(n, tc.section))
					return nil
				}},
				{Name: "End", Call: func(c *evm.Call) error {
					k := len(stack) - 1
					s := stack[k]
					stack = stack[:k]
					return s.End(c)
				}},
				{Name: "Wait", Call: func(c *evm.Call) error {
					if tc.wait {
						time.Sleep(25 * time.Millisecond)
					}
					return nil
				}},
			}}
			source := "var sum int\nfunc Main() int { " + tc.body + "; return sum }"
			if strings.Contains(tc.body, "host.") {
				source = "import \"host\"\n" + source
			}
			m, err := evmcompiler.Compile("script.go", source, p)
			if err != nil {
				t.Fatal(err)
			}
			prog, err := evm.Link(m, p)
			if err != nil {
				t.Fatal(err)
			}
			out := []values.Value{values.Int(-1)}
			err = evm.New(prog).RunWithBudget(context.Background(), "Main", nil, out, evm.Budget{Steps: tc.outer, Duration: tc.whole})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if err != nil && out[0].AsInt() != -1 {
				t.Fatal("published results after failure")
			}
		})
	}
}

func TestAdjustableBudgets(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       error
	}{
		{"extend scope", "host.Open(); evmruntime.IncreaseCPUBudget(50); host.Wait(); host.End()", nil},
		{"soft expiry", "host.Open(); host.Wait(); host.End()", context.DeadlineExceeded},
		{"hard cap", "host.Open(); evmruntime.IncreaseCPUBudget(^uint64(0)); host.WaitLong(); host.End()", context.DeadlineExceeded},
		{"restore parent", "host.Open(); evmruntime.IncreaseCPUBudget(50); host.End(); host.WaitLong()", nil},
		{"allocation extension", "evmruntime.IncreaseMemBudget(4096,100); x:=make([]byte,2048); host.Size(len(x))", nil},
		{"allocation cap", "evmruntime.IncreaseMemBudget(^uint64(0),^uint64(0)); x:=make([]byte,8192); host.Size(len(x))", evm.ErrAllocation},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var scope *evm.BudgetScope
			p := evm.Package{Path: "host", Functions: []evm.ExternalFunction{
				{Name: "Open", Call: func(c *evm.Call) error {
					scope = c.NarrowCPUBudget(10*time.Millisecond, 50*time.Millisecond)
					return nil
				}},
				{Name: "End", Call: func(c *evm.Call) error { return scope.End(c) }},
				{Name: "Wait", Call: func(c *evm.Call) error { time.Sleep(25 * time.Millisecond); return nil }},
				{Name: "WaitLong", Call: func(c *evm.Call) error { time.Sleep(70 * time.Millisecond); return nil }},
				{Name: "Size", Signature: evm.Signature{Params: []*values.Type{values.IntType}}, Call: func(c *evm.Call) error { return nil }},
			}}
			imports := `import "host";`
			if strings.Contains(tc.body, "evmruntime.") {
				imports += `import "evmruntime";`
			}
			m, err := evmcompiler.Compile("main.go", imports+"func Main(){"+tc.body+"}", p)
			if err != nil {
				t.Fatal(err)
			}
			program, err := evm.Link(m, p)
			if err != nil {
				t.Fatal(err)
			}
			vm := evm.New(program)
			err = vm.RunWithBudget(context.Background(), "Main", nil, nil, evm.Budget{UnlimitedSteps: true, Duration: time.Second, AllocationBytes: 1024, HardAllocationBytes: 4096})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}
