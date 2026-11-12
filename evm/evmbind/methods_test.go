//go:build !evmreflect

package evmbind_test

import (
	"bytes"
	"context"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmbind"
	"github.com/andreyvit/edb/evm/evmbind/internal/methodbindings"
	"github.com/andreyvit/edb/evm/evmbind/internal/methodfixture"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func methodRecords() map[string]kvo.Record {
	row := &methodfixture.Row{Earned: 100, Redeemed: 20, Expired: 7}
	reflected := kvo.Bind(reflect.TypeFor[methodfixture.Row](), methodfixture.Model)
	generated := reflected.WithGenerated(&methodfixture.Static, evmbind.StaticOnly)
	fast := generated.Encoded(row2.Trusted(generated.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)))
	compact := generated.Encoded(row2.Trusted(generated.Encode(nil, reflect.ValueOf(row).Elem(), row2.Compact2)))
	return map[string]kvo.Record{
		"Fast2":           fast,
		"Compact2":        compact,
		"MessagePack":     generated.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row))),
		"NativeGenerated": generated.Record(reflect.ValueOf(row)),
		"NativeReflected": reflected.Record(reflect.ValueOf(row)),
		"Memory":          kvo.Encoded(fast.Snapshot()).WithModel(methodfixture.Model).Fastest(),
	}
}
func methodPackage(t testing.TB, generated bool) evm.Package {
	t.Helper()
	pkg, err := evm.ModelPackage("m", methodfixture.Schema)
	if err != nil {
		t.Fatal(err)
	}
	opt := evmbind.MethodOptions{}
	if generated {
		opt.Mode = evmbind.StaticOnly
		opt.Static = methodbindings.MethodBindings
	}
	if err = evmbind.RegisterMethods(&pkg, methodfixture.Methods(), opt); err != nil {
		t.Fatal(err)
	}
	return pkg
}

const methodSource = `import "m"
 func Read(r *m.Customer,n int)int64 {var sum int64;for i:=0;i<n;i++ {sum+=r.Balance()};return sum}
 func Inline(r *m.Customer,n int)int64 {var sum int64;for i:=0;i<n;i++ {sum+=r.earned-r.redeemed-r.expired};return sum}
 type Account interface{Add(int64)int64}
 func via(a Account)int64{return a.Add(2)}
 func Forms(r *m.Customer)(int64,string,bool,uint16,float32){
  defer r.Observe()
  bound:=r.Add;expr:=(*m.Customer).Balance
  n,f:=r.Summary(-8,1.25)
  ok,u:=r.Check(true)
  return via(r)+bound(3)+expr(r)+n,r.Label("balance="),ok,u,f
 }
 func Fail(r *m.Customer){r.Fail()}
 func Label(r *m.Customer)string{return r.Label("balance=")}
 func BoundLabel(r *m.Customer)string {f:=r.Label;return f("balance=")}
 func Name(r *m.Customer)string{return r.Name()}
 func WithFlag(r *m.Customer)int64{return r.WithFlag(3)}
`

func compileMethods(t testing.TB, pkg evm.Package, src string) (*evm.Module, *evm.VM) {
	t.Helper()
	mod, err := evmcompiler.Compile("methods.go", src, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	return mod, evm.New(p)
}
func TestRecordMethods(t *testing.T) {
	for _, generated := range []bool{false, true} {
		pkg := methodPackage(t, generated)
		mod, vm := compileMethods(t, pkg, methodSource)
		if strings.Contains(mod.Disassemble(), "call.host.su") != generated {
			t.Fatal(mod.Disassemble())
		}
		for name, r := range methodRecords() {
			t.Run(name+map[bool]string{true: "/generated", false: "/reflected"}[generated], func(t *testing.T) {
				if n := methodfixture.Customer_Balance(r); n != 73 {
					t.Fatal(n)
				}
				var out [5]values.Value
				if err := vm.Run(context.Background(), "Forms", []values.Value{r.Value()}, out[:]); err != nil || out[0].AsInt() != 289 || out[1].AsString() != "balance=73" || !out[2].AsBool() || out[3].AsUint() != 65535 || out[4].AsFloat32() != 2.5 {
					t.Fatal(out, err)
				}
				args := []values.Value{r.Value(), values.Int(1000)}
				if err := vm.Run(context.Background(), "WithFlag", args[:1], out[:1]); err != nil || out[0].AsInt() != 76 {
					t.Fatal(out[0], err)
				}
				run := func() {
					if err := vm.Run(context.Background(), "Read", args, out[:1]); err != nil || out[0].AsInt() != 73000 {
						t.Fatal(out, err)
					}
				}
				run()
				if generated {
					if n := testing.AllocsPerRun(20, run); n != 0 {
						t.Fatalf("%g allocations", n)
					}
				}
				if err := vm.Run(context.Background(), "Fail", args[:1], nil); err == nil || !strings.Contains(err.Error(), "helper panic") {
					t.Fatal(err)
				}
				if err := vm.Reset(); err != nil {
					t.Fatal(err)
				}
				for _, fn := range []string{"Label", "BoundLabel"} {
					err := vm.RunWithBudget(context.Background(), fn, args[:1], out[:1], evm.Budget{AllocationBytes: 1})
					if err == nil || !strings.Contains(err.Error(), "budget") {
						t.Fatal("string result bypassed allocation budget", err)
					}
					if err := vm.Reset(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
		row := &methodfixture.Row{Name: "borrowed-name"}
		binding := kvo.Bind(reflect.TypeFor[methodfixture.Row](), methodfixture.Model)
		raw := msgpack.Encode(nil, reflect.ValueOf(row))
		var out [1]values.Value
		if err := vm.Run(context.Background(), "Name", []values.Value{binding.MessagePack(raw).Value()}, out[:]); err != nil {
			t.Fatal(err)
		}
		clear(raw)
		if out[0].AsString() != row.Name {
			t.Fatal("method result retained borrowed record bytes")
		}
	}
}
func TestRecordMethodRegistration(t *testing.T) {
	code, err := evmbind.GenerateMethods("methodbindings", methodfixture.Methods())
	if err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile("internal/methodbindings/bindings_gen.go")
	if err != nil || !bytes.Equal(code, saved) {
		t.Fatal("stale generated methods", err)
	}
	if bytes.Count(code, []byte(`"github.com/andreyvit/edb/kvo"`)) != 1 {
		t.Fatal("record receivers and schema arguments must share the canonical import")
	}
	stale := maps.Clone(methodbindings.MethodBindings)
	for name, adapter := range stale {
		adapter.Signature = "old signature"
		stale[name] = adapter
	}
	for _, tc := range []struct {
		name    string
		methods []evmbind.Method
		opt     evmbind.MethodOptions
	}{
		{"duplicate", []evmbind.Method{methodfixture.Methods()[0], methodfixture.Methods()[0]}, evmbind.MethodOptions{}},
		{"field", []evmbind.Method{evmbind.RecordMethod(methodfixture.Model, "earned", methodfixture.Customer_Balance)}, evmbind.MethodOptions{}},
		{"existing", []evmbind.Method{evmbind.RecordMethod(methodfixture.Model, "ToJSON", methodfixture.Customer_Balance)}, evmbind.MethodOptions{}},
		{"signature", []evmbind.Method{evmbind.RecordMethod(methodfixture.Model, "Bad", func(int64) int64 { return 1 })}, evmbind.MethodOptions{}},
		{"missing static", methodfixture.Methods(), evmbind.MethodOptions{Mode: evmbind.StaticOnly}},
		{"stale static", methodfixture.Methods(), evmbind.MethodOptions{Mode: evmbind.StaticOnly, Static: stale}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pkg, err := evm.ModelPackage("m", methodfixture.Schema)
			if err != nil {
				t.Fatal(err)
			}
			before := slices.Clone(pkg.Methods)
			if err := evmbind.RegisterMethods(&pkg, tc.methods, tc.opt); err == nil || len(pkg.Methods) != len(before) {
				t.Fatal("invalid registration changed package", err)
			}
		})
	}
	pkg := methodPackage(t, true)
	if _, err := evmcompiler.Compile("other.go", `import "m";func Run(r *m.Other)int64{return r.Balance()}`, pkg); err == nil {
		t.Fatal("method leaked to other model")
	}
	// One implementation can be attached to another model under an explicit name.
	if err := evmbind.RegisterMethods(&pkg, []evmbind.Method{evmbind.RecordMethod(methodfixture.OtherModel, "Earned", methodfixture.Customer_Balance, evmbind.BorrowOnly)}, evmbind.MethodOptions{Mode: evmbind.StaticOnly, Static: methodbindings.MethodBindings}); err != nil {
		t.Fatal(err)
	}
	_, vm := compileMethods(t, pkg, `import "m";func Run(r *m.Other)int64{return r.Earned()}`)
	r := kvo.Encoded(methodRecords()["Fast2"].Snapshot()).WithModel(methodfixture.OtherModel)
	var out [1]values.Value
	if err := vm.Run(context.Background(), "Run", []values.Value{r.Value()}, out[:]); err != nil || out[0].AsInt() != 73 {
		t.Fatal(out, err)
	}
}

func TestRecordMethodPropertyAliases(t *testing.T) {
	schema := kvo.NewSchema()
	key := kvo.NewProp(schema, 1, "customer:points", kvo.TInt64, nil)
	model := kvo.NewModel(schema, kvo.NewEntityType(schema, "customer"), func(b *kvo.ModelBuilder) {
		b.Prop(schema.MustPropByCode(key), kvo.PropName("balance"))
	})
	for _, name := range []string{"balance", "points", "customer_points"} {
		t.Run(name, func(t *testing.T) {
			pkg, err := evm.ModelPackage("m", schema)
			if err != nil {
				t.Fatal(err)
			}
			before := slices.Clone(pkg.Methods)
			methods := []evmbind.Method{
				evmbind.RecordMethod(model, "Valid", methodfixture.Customer_Balance),
				evmbind.RecordMethod(model, name, methodfixture.Customer_Balance),
			}
			err = evmbind.RegisterMethods(&pkg, methods, evmbind.MethodOptions{})
			if err == nil || !strings.Contains(err.Error(), "conflicts with a property") {
				t.Fatalf("registration = %v, want property conflict", err)
			}
			if len(pkg.Methods) != len(before) {
				t.Fatal("rejected batch partially registered methods")
			}
		})
	}
}

func TestRecordMethodBorrowing(t *testing.T) {
	for _, flags := range []evmbind.MethodFlags{0, evmbind.BorrowOnly} {
		pkg, err := evm.ModelPackage("m", methodfixture.Schema)
		if err != nil {
			t.Fatal(err)
		}
		if err := evmbind.RegisterMethods(&pkg, []evmbind.Method{evmbind.RecordMethod(methodfixture.Model, "Balance", methodfixture.Customer_Balance, flags)}, evmbind.MethodOptions{Mode: evmbind.StaticOnly, Static: methodbindings.MethodBindings}); err != nil {
			t.Fatal(err)
		}
		r := methodRecords()["Fast2"]
		pkg.Functions = append(pkg.Functions, evm.ExternalFunction{Name: "Read", Signature: evm.Signature{Results: []*values.Type{pkg.Record(methodfixture.Model, false).Pointer}}, Ownership: &evm.OwnershipEffects{NoRetain: true, NoInvalidate: true, Borrowed: []evm.BorrowSource{{Result: 0, Domain: "db"}}}, Call: func(c *evm.Call) error { c.SetResult(0, r.Value()); return nil }})
		mod, vm := compileMethods(t, pkg, `import "m";func Run(n int)int64{var s int64;for i:=0;i<n;i++{r:=m.Read();s+=r.Balance()};return s}`)
		copied := false
		for _, f := range mod.Functions {
			copied = copied || len(f.OwnershipSites) > 0
		}
		if copied != (flags == 0) {
			t.Fatal("unexpected ownership copies", flags, mod.Disassemble())
		}
		var out [1]values.Value
		if err := vm.Run(context.Background(), "Run", []values.Value{values.Int(1000)}, out[:]); err != nil || out[0].AsInt() != 73000 {
			t.Fatal(out, err)
		}
	}
}

var methodSink int64

func BenchmarkRecordMethods(b *testing.B) {
	const n = 256
	for name, r := range methodRecords() {
		b.Run(name+"/Native", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for range n {
					methodSink = methodfixture.Customer_Balance(r)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/call")
		})
		pkg := methodPackage(b, true)
		_, vm := compileMethods(b, pkg, methodSource)
		for _, fn := range []string{"Read", "Inline"} {
			b.Run(name+"/EVM"+fn, func(b *testing.B) {
				args := []values.Value{r.Value(), values.Int(n)}
				var out [1]values.Value
				if err := vm.Run(context.Background(), fn, args, out[:]); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if err := vm.Run(context.Background(), fn, args, out[:]); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/call")
			})
		}
	}
}
