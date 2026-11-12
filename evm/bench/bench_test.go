package bench_test

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/bench/internal/workload"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func newEVM(t testing.TB, name string) workload.Runner {
	return sensorVM(t, name, false)
}

func sensorVM(t testing.TB, name string, raw bool) workload.Runner {
	t.Helper()
	sensors := workload.NewSensors()
	sig := evm.Signature{Params: []*values.Type{values.IntType}, Results: []*values.Type{values.IntType}}
	sensor := func(name string, data *[256]int) evm.ExternalFunction {
		if raw {
			return evm.ExternalFunction{Name: name, Signature: sig, Direct: evm.DirectUnary(func(i uint64) uint64 { return uint64(data[i]) })}
		}
		return evm.ExternalFunction{Name: name, Signature: sig, Call: func(c *evm.Call) error {
			c.R[0] = uint64(data[c.R[0]])
			return nil
		}}
	}
	pkg := evm.Package{Path: "sensors", Functions: []evm.ExternalFunction{sensor("Reading", &sensors.Reading), sensor("Limit", &sensors.Limit)}}
	m, err := evmcompiler.Compile("guest.go", workload.GuestGo, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	// Each Sieve call drops its backing array. Keep the per-call budget but
	// allow a long benchmark to outlive the default cumulative session quota.
	var budget evm.Budget
	if name == "Sieve" {
		budget = evm.Budget{AllocationBytes: 64 << 20, Allocations: 1_000_000}
		vm.Limits.Allocations = ^uint64(0)
		vm.Limits.AllocationBytes = ^uint64(0)
	}
	var args, results [1]values.Value
	return func(seed int) (int, error) {
		args[0] = values.Int(int64(seed))
		err := vm.RunWithBudget(context.Background(), name, args[:], results[:], budget)
		return int(results[0].AsInt()), err
	}
}

func TestEVM(t *testing.T)      { workload.Verify(t, newEVM) }
func BenchmarkEVM(b *testing.B) { workload.Benchmark(b, newEVM) }

// Keep the original Call benchmark as the embedding regression control.
func BenchmarkDirectUnary(b *testing.B) {
	workload.BenchmarkEntry(b, "Scan", func(t testing.TB, name string) workload.Runner { return sensorVM(t, name, true) })
}
