package bench_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/bench/internal/mathwork"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

var mathKernels = []struct {
	name string
	fn   func(int, float64, []float64, []float64) float64
}{
	{"Filter", mathwork.Filter}, {"Oscillator", mathwork.Oscillator},
	{"Midpoint", mathwork.Midpoint}, {"Chebyshev", mathwork.Chebyshev},
	{"Horner", mathwork.Horner}, {"Dot", mathwork.Dot}, {"Kahan", mathwork.Kahan},
	{"Variance", mathwork.Variance}, {"Calibrate", mathwork.Calibrate},
	{"Autocorrelation", mathwork.Autocorrelation}, {"SoftClip", mathwork.SoftClip}, {"Newton", mathwork.Newton},
}

func mathModule(t testing.TB) *evm.Module {
	t.Helper()
	m, err := evmcompiler.Compile("math.go", mathwork.Source)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func mathProgram(t testing.TB, m *evm.Module) *evm.Program {
	t.Helper()
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func closeFloat(a, b float64) bool {
	return math.Abs(a-b) <= 1e-11*(1+math.Abs(b))
}

type mathCase struct {
	vm   *evm.VM
	name string
	n    int
	args [4]values.Value
	out  [1]values.Value
	want [4]float64
	a, b []values.Value
}

func newMathCase(p *evm.Program, index, n int) *mathCase {
	k := mathKernels[index]
	c := &mathCase{vm: evm.New(p), name: k.name, n: n, a: make([]values.Value, n), b: make([]values.Value, n)}
	a, b := make([]float64, n), make([]float64, n)
	for i := range a {
		a[i] = math.Sin(float64(i)*0.137) + 0.4*math.Cos(float64(i)*0.019)
		b[i] = 0.75 - math.Cos(float64(i)*0.231)
		c.a[i], c.b[i] = values.Float(a[i]), values.Float(b[i])
	}
	c.args[0] = values.Int(int64(n))
	c.args[2] = values.SliceValue(unsafe.Pointer(unsafe.SliceData(c.a)), uint32(n), uint32(n))
	c.args[3] = values.SliceValue(unsafe.Pointer(unsafe.SliceData(c.b)), uint32(n), uint32(n))
	for seed := range c.want {
		c.want[seed] = k.fn(n, 0.25+float64(seed)*0.125, a, b)
	}
	return c
}

func (c *mathCase) check(t testing.TB, seed int) {
	c.args[1] = values.Float(0.25 + float64(seed)*0.125)
	err := c.vm.Run(context.Background(), c.name, c.args[:], c.out[:])
	if err != nil || !closeFloat(c.out[0].AsFloat(), c.want[seed]) {
		t.Fatalf("%s(%d), seed %d: got %v (%v), want %v", c.name, c.n, seed, c.out[0].AsFloat(), err, c.want[seed])
	}
}

func TestMathKernels(t *testing.T) {
	p := mathProgram(t, mathModule(t))
	// The same fixtures run against older checkouts without this diagnostic.
	if d, ok := any(p).(interface{ DisassembleFloatLoops() string }); ok {
		for _, line := range strings.Split(d.DisassembleFloatLoops(), "\n") {
			if strings.Contains(line, " pc ") {
				t.Log("linked", line)
			}
		}
	}
	for i, k := range mathKernels {
		t.Run(k.name, func(t *testing.T) {
			for _, n := range []int{3, 8, 17, 255, 256, 257, 512} {
				c := newMathCase(p, i, n)
				for seed := range 4 {
					c.check(t, seed)
					if k.name == "Calibrate" {
						for j, v := range c.b {
							want := c.a[j].AsFloat()*c.args[1].AsFloat() + 0.125
							if !closeFloat(v.AsFloat(), want) {
								t.Fatalf("dst[%d]=%v, want %v", j, v.AsFloat(), want)
							}
						}
					}
				}
			}
		})
	}
}

func BenchmarkMath(b *testing.B) {
	p := mathProgram(b, mathModule(b))
	for i, k := range mathKernels {
		for _, n := range []int{8, 512} {
			b.Run(fmt.Sprintf("%s/%d", k.name, n), func(b *testing.B) {
				c := newMathCase(p, i, n)
				for seed := range 4 {
					c.check(b, seed)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for j := range b.N {
					c.check(b, j&3)
				}
			})
		}
	}
}

func BenchmarkMathBuild(b *testing.B) {
	b.Run("Compile", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			mathModule(b)
		}
	})
	m := mathModule(b)
	b.Run("Link", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			mathProgram(b, m)
		}
	})
}
