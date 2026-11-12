package yaegi_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/evm/bench/internal/workload"
	"github.com/traefik/yaegi/interp"
)

func newYaegi(t testing.TB, name string) workload.Runner {
	t.Helper()
	i := interp.New(interp.Options{})
	s := workload.NewSensors()
	err := i.Use(interp.Exports{"sensors/sensors": {
		"Reading": reflect.ValueOf(func(n int) int { return s.Reading[n] }),
		"Limit":   reflect.ValueOf(func(n int) int { return s.Limit[n] }),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.Eval(workload.GuestGo); err != nil {
		t.Fatal(err)
	}
	entry, err := i.Eval("guest." + name)
	if err != nil {
		t.Fatal(err)
	}
	run := entry.Interface().(func(int) int)
	return func(seed int) (int, error) { return run(seed), nil }
}

func TestYaegi(t *testing.T)      { workload.Verify(t, newYaegi) }
func BenchmarkYaegi(b *testing.B) { workload.Benchmark(b, newYaegi) }
