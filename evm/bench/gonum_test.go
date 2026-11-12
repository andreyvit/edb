package bench_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

// All scalar-returning reductions from Gonum’s dot, norm and generic vector files.
// Read their function declarations verbatim from a local module checkout.
// EDB has no build or test dependency on Gonum.
var gonumReductions = []string{"DotUnitary", "DotInc", "L1Norm", "L1NormInc", "L1Dist", "LinfDist", "Sum", "L2NormUnitary", "L2NormInc", "L2DistanceUnitary"}

// Separate holdout: every unit-stride out-of-place vector transform in these files.
var gonumTransforms = []string{"AxpyUnitaryTo", "ScalUnitaryTo", "DivTo", "CumSum", "CumProd"}

func gonumProgram(t testing.TB) *evm.Program {
	t.Helper()
	dir := os.Getenv("EVM_GONUM_DIR")
	if dir == "" {
		t.Skip("set EVM_GONUM_DIR to the Gonum v0.16.0 module directory")
	}
	var source strings.Builder
	source.WriteString("package guest\nimport \"math\"\n")
	count := 0
	for _, name := range []string{"dot.go", "stubs_noasm.go", "l2norm_noasm.go", "axpy.go", "scal.go"} {
		data, err := os.ReadFile(filepath.Join(dir, "internal", "asm", "f64", name))
		if err != nil {
			t.Fatal(err)
		}
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, name, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			if f, ok := decl.(*ast.FuncDecl); ok && (slices.Contains(gonumReductions, f.Name.Name) || slices.Contains(gonumTransforms, f.Name.Name)) {
				source.Write(data[fs.Position(f.Pos()).Offset:fs.Position(f.End()).Offset])
				source.WriteByte('\n')
				count++
			}
		}
	}
	if count != len(gonumReductions)+len(gonumTransforms) {
		t.Fatalf("found %d upstream routines, want %d", count, len(gonumReductions)+len(gonumTransforms))
	}
	m, err := evmcompiler.Compile("gonum.go", source.String())
	if err != nil {
		t.Fatal(err)
	}
	return mathProgram(t, m)
}

func gonumInput(name string, n int) (args []values.Value, want float64) {
	x, y := make([]values.Value, n), make([]values.Value, n)
	for i := range x {
		x[i] = values.Float(math.Sin(float64(i) * 0.29))
		y[i] = values.Float(math.Cos(float64(i) * 0.13))
	}
	sv := func(a []values.Value) values.Value {
		return values.SliceValue(unsafe.Pointer(unsafe.SliceData(a)), uint32(n), uint32(n))
	}
	args = []values.Value{sv(x)}
	switch name {
	case "DotUnitary", "L1Dist", "LinfDist", "L2DistanceUnitary":
		args = append(args, sv(y))
	case "DotInc":
		args = append(args, sv(y), values.Uint(uint64((n+1)/2)), values.Uint(2), values.Uint(2), values.Uint(0), values.Uint(0))
	case "L1NormInc", "L2NormInc":
		args = append(args, values.Uint(uint64((n+1)/2)), values.Uint(2))
	}
	step := 1
	if strings.HasSuffix(name, "Inc") {
		step = 2
	}
	for i := 0; i < n; i += step {
		a, b := x[i].AsFloat(), y[i].AsFloat()
		switch name {
		case "DotUnitary", "DotInc":
			want += a * b
		case "L1Norm", "L1NormInc":
			want += math.Abs(a)
		case "L1Dist":
			want += math.Abs(a - b)
		case "LinfDist":
			want = math.Max(want, math.Abs(a-b))
		case "Sum":
			want += a
		case "L2NormUnitary", "L2NormInc":
			want = math.Hypot(want, a)
		case "L2DistanceUnitary":
			want = math.Hypot(want, a-b)
		}
	}
	return args, want
}

func BenchmarkGonum(b *testing.B) {
	p := gonumProgram(b)
	for _, name := range gonumReductions {
		b.Run(name, func(b *testing.B) {
			vm := evm.New(p)
			args, want := gonumInput(name, 512)
			var out [1]values.Value
			run := func() {
				if err := vm.Run(context.Background(), name, args, out[:]); err != nil || !closeFloat(out[0].AsFloat(), want) {
					b.Fatalf("got %v (%v), want %v", out[0].AsFloat(), err, want)
				}
			}
			run()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				run()
			}
		})
	}
}

func TestGonumKernels(t *testing.T) {
	p := gonumProgram(t)
	if d, ok := any(p).(interface{ DisassembleFloatLoops() string }); ok {
		for _, line := range strings.Split(d.DisassembleFloatLoops(), "\n") {
			if strings.Contains(line, " pc ") {
				t.Log("linked", line)
			}
		}
	}
	for _, name := range gonumReductions {
		t.Run(name, func(t *testing.T) {
			vm := evm.New(p)
			for _, n := range []int{0, 1, 8, 17, 255, 256, 257, 512} {
				args, want := gonumInput(name, n)
				var out [1]values.Value
				if err := vm.Run(context.Background(), name, args, out[:]); err != nil || !closeFloat(out[0].AsFloat(), want) {
					t.Fatalf("%s(%d): got %v (%v), want %v", name, n, out[0].AsFloat(), err, want)
				}
			}
		})
	}
}

// Verify the entire destination before and after timing. No output from one run
// is used as input to the next. The timed loop checks the error and one cell.
func BenchmarkGonumTransform(b *testing.B) {
	p := gonumProgram(b)
	for _, name := range gonumTransforms {
		b.Run(name, func(b *testing.B) {
			const n = 512
			var x, y, z [n]values.Value
			var want [n]float64
			sv := func(p *[n]values.Value) values.Value { return values.SliceValue(unsafe.Pointer(&p[0]), n, n) }
			for i := range x {
				a, c := 1+math.Sin(float64(i)*0.11)/128, 1.25+math.Cos(float64(i)*0.17)/8
				x[i], y[i] = values.Float(a), values.Float(c)
				switch name {
				case "AxpyUnitaryTo":
					want[i] = 0.75*a + c
				case "ScalUnitaryTo":
					want[i] = 0.75 * a
				case "DivTo":
					want[i] = a / c
				case "CumSum":
					want[i] = a
					if i > 0 {
						want[i] += want[i-1]
					}
				case "CumProd":
					want[i] = a
					if i > 0 {
						want[i] *= want[i-1]
					}
				}
			}
			args := []values.Value{sv(&z)}
			if name == "AxpyUnitaryTo" || name == "ScalUnitaryTo" {
				args = append(args, values.Float(0.75))
			}
			args = append(args, sv(&x))
			if name == "AxpyUnitaryTo" || name == "DivTo" {
				args = append(args, sv(&y))
			}
			var out []values.Value
			if name == "DivTo" || name == "CumSum" || name == "CumProd" {
				out = make([]values.Value, 1)
			}
			vm := evm.New(p)
			run := func(i int) {
				if err := vm.Run(context.Background(), name, args, out); err != nil || !closeFloat(z[i].AsFloat(), want[i]) {
					b.Fatalf("cell %d: got %v (%v), want %v", i, z[i].AsFloat(), err, want[i])
				}
			}
			check := func() {
				for i := range z {
					if !closeFloat(z[i].AsFloat(), want[i]) {
						b.Fatalf("cell %d: %v vs %v", i, z[i].AsFloat(), want[i])
					}
				}
			}
			run(0)
			check()
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				run(i & (n - 1))
			}
			b.StopTimer()
			check()
		})
	}
}
