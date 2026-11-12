package evmcompiler_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestRunBudgets(t *testing.T) {
	vm := compileGoVM(t, `var keep *int
 func Allocate(n int){for i:=0;i<n;i++{keep=new(int)}}
 func Buffer(n int)int{return cap(make([]byte,n))}
 func Spin()int{defer func(){recover()}();for{}}
 func Sum(n int)int{v:=0;for i:=0;i<n;i++{v+=i};return v}`)
	ctx := context.Background()
	run := func(name string, n int, b evm.Budget, out []values.Value) error {
		return vm.RunWithBudget(ctx, name, []values.Value{values.Int(int64(n))}, out, b)
	}
	// Warm reusable registers and initialize globals before measuring guest objects.
	if err := run("Allocate", 0, evm.Budget{}, nil); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{4, 7, 2} {
		if err := run("Allocate", n, evm.Budget{Allocations: uint64(n)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := run("Allocate", 5, evm.Budget{Allocations: 4}, nil); !errors.Is(err, evm.ErrAllocations) {
		t.Fatalf("small objects: %v", err)
	}
	if err := run("Allocate", 0, evm.Budget{}, nil); !errors.Is(err, evm.ErrInvalidState) {
		t.Fatal(err)
	}
	_ = vm.Reset()
	out := []values.Value{values.Int(-1)}
	if err := run("Buffer", 1<<28, evm.Budget{Steps: math.MaxUint64, Allocations: 1000, AllocationBytes: 1024}, out); !errors.Is(err, evm.ErrAllocation) || out[0].AsInt() != -1 {
		t.Fatalf("oversize buffer: %v %v", err, out)
	}
	_ = vm.Reset()
	if err := vm.RunWithBudget(ctx, "Spin", nil, out, evm.Budget{Steps: 31}); !errors.Is(err, evm.ErrSteps) || out[0].AsInt() != -1 {
		t.Fatalf("spin: %v %v", err, out)
	}
	_ = vm.Reset()
	vm.Limits.Steps = 10
	if err := run("Sum", 100, evm.Budget{Steps: 10000}, out); err != nil || out[0].R != 4950 {
		t.Fatal(err, out)
	}
	if err := run("Sum", 100, evm.Budget{}, out); !errors.Is(err, evm.ErrSteps) {
		t.Fatal("per-call override leaked", err)
	}
}

func TestPerCallBudgetCannotBypassSessionAllocationLimit(t *testing.T) {
	vm := compileGoVM(t, `var keep *int
 func Run(){keep=new(int)}`)
	vm.Limits.Allocations = 20
	var completed int
	for i := 0; i < 25; i++ {
		err := vm.RunWithBudget(context.Background(), "Run", nil, nil, evm.Budget{Allocations: 100})
		if errors.Is(err, evm.ErrAllocations) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		completed++
	}
	if completed < 2 || completed >= 20 {
		t.Fatalf("session should bound cumulative allocations, completed %d calls", completed)
	}
	_ = vm.Reset()
	if err := vm.Run(context.Background(), "Run", nil, nil); err != nil {
		t.Fatal(err)
	}
}

type observedPoll struct {
	context.Context
	polls   int
	running chan struct{}
}

func (c *observedPoll) Err() error {
	c.polls++
	if c.polls == 3 {
		close(c.running)
	}
	return c.Context.Err()
}
func TestCancelGuestLoopInFlight(t *testing.T) {
	vm := compileGoVM(t, `func Run()int{for{}}`)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &observedPoll{Context: parent, running: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- vm.RunWithBudget(ctx, "Run", nil, make([]values.Value, 1), evm.Budget{Steps: 1 << 31}) }()
	select {
	case <-ctx.running:
	case err := <-done:
		t.Fatalf("loop stopped before cancellation: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("loop never polled context")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("loop ignored cancellation")
	}
}

func TestNativeAllocationLimitIsFatal(t *testing.T) {
	pkg := evm.Package{Path: "host", Functions: []evm.ExternalFunction{{Name: "Work", Call: func(c *evm.Call) error {
		for range 16 {
			_, _ = c.AllocCells(1)
		}
		return nil
	}}}}
	m, err := evmcompiler.Compile("main.go", `import "host";func Run(){defer func(){recover()}();host.Work()}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	err = evm.New(p).RunWithBudget(context.Background(), "Run", nil, nil, evm.Budget{Allocations: 8})
	if !errors.Is(err, evm.ErrAllocations) {
		t.Fatalf("got %v, want ErrAllocations", err)
	}
}

// Cancel deterministically from inside the VM, without racing the scheduler.
type cancelOnPoll struct {
	context.Context
	cancel context.CancelFunc
	polls  int
}

func (c *cancelOnPoll) Err() error {
	c.polls++
	if c.polls == 8 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestCancelDuringOneMapOperation(t *testing.T) {
	vm := compileGoVM(t, `func Run(key string) int {
		m := make(map[string]int)
		m[key] = 1
		return len(m)
	}`)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &cancelOnPoll{Context: parent, cancel: cancel}
	out := []values.Value{values.Int(-1)}
	err := vm.RunWithBudget(ctx, "Run", []values.Value{values.String(strings.Repeat("x", 128<<10))}, out, evm.Budget{Steps: 32})
	if !errors.Is(err, context.Canceled) || out[0].AsInt() != -1 {
		t.Fatalf("cancel during key hashing: result %v, error %v", out, err)
	}
}
