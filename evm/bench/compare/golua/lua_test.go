package golua_test

import (
	"fmt"
	"testing"

	"github.com/andreyvit/edb/evm/bench/internal/workload"
	lunar "github.com/mmcdole/lunar"
	gl "github.com/yuin/gopher-lua"
)

func newLunar(t testing.TB, name string) workload.Runner {
	s, err := lunar.New(lunar.Options{Libraries: lunar.CoreLibraries()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	sensors := workload.NewSensors()
	bind := func(name string, data *[256]int) {
		f, err := s.NewNativeFunction(func(frame lunar.Frame) lunar.Outcome {
			i, ok := frame.Number(0)
			if !ok {
				frame.ThrowArgTypeError(0, lunar.NumberKind)
			}
			return frame.ReturnNumber(float64(data[int(i)]))
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SetGlobal(name, f.Value()); err != nil {
			t.Fatal(err)
		}
	}
	bind("reading", &sensors.Reading)
	bind("limit", &sensors.Limit)
	if _, err := s.DoString("@guest.lua", workload.Lua); err != nil {
		t.Fatal(err)
	}
	f, err := s.Global(name)
	if err != nil {
		t.Fatal(err)
	}
	return func(seed int) (int, error) {
		r, err := s.CallOne(f, lunar.Number(float64(seed)))
		if err != nil {
			return 0, err
		}
		x, ok := r.AsNumber()
		if !ok {
			return 0, fmt.Errorf("non-number result")
		}
		return int(x), nil
	}
}

func newGopher(t testing.TB, name string) workload.Runner {
	s := gl.NewState()
	t.Cleanup(s.Close)
	sensors := workload.NewSensors()
	bind := func(name string, data *[256]int) {
		s.SetGlobal(name, s.NewFunction(func(s *gl.LState) int {
			i := s.CheckInt(1)
			s.Push(gl.LNumber(data[i]))
			return 1
		}))
	}
	bind("reading", &sensors.Reading)
	bind("limit", &sensors.Limit)
	if err := s.DoString(workload.Lua); err != nil {
		t.Fatal(err)
	}
	f := gl.P{Fn: s.GetGlobal(name), NRet: 1, Protect: true}
	return func(seed int) (int, error) {
		if err := s.CallByParam(f, gl.LNumber(seed)); err != nil {
			return 0, err
		}
		v := s.Get(-1)
		s.Pop(1)
		x, ok := v.(gl.LNumber)
		if !ok {
			return 0, fmt.Errorf("non-number result")
		}
		return int(x), nil
	}
}

func TestLunar(t *testing.T)       { workload.Verify(t, newLunar) }
func TestGopher(t *testing.T)      { workload.Verify(t, newGopher) }
func BenchmarkLunar(b *testing.B)  { workload.Benchmark(b, newLunar) }
func BenchmarkGopher(b *testing.B) { workload.Benchmark(b, newGopher) }
