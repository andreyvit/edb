package bench_test

import (
	"context"
	_ "embed"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

//go:embed testdata/write_options.go
var writeOptionsScript string

func BenchmarkRecordWriteOptions(b *testing.B) {
	b.Run("Dense", func(b *testing.B) { benchmarkRecordWriteOptions(b, 1) })
	b.Run("Sparse", func(b *testing.B) { benchmarkRecordWriteOptions(b, 4096) })
}

func benchmarkRecordWriteOptions(b *testing.B, scale uint64) {
	_, source := statrow.Model()
	schema := kvo.NewSchema()
	for _, p := range source.AllProps() {
		kvo.NewProp(schema, p.Code()*scale, p.Name(), p.AnyType(), nil)
	}
	model := kvo.NewModel(schema, kvo.NewEntityType(schema, "customer", reflect.TypeFor[statrow.State]()), func(m *kvo.ModelBuilder) {
		for _, p := range source.AllProps() {
			m.Prop(schema.MustPropByCode(p.Code()*scale), source.PropFlags(p.Code()))
		}
	})
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	native := recordOptionsSchema{}
	for typ, fields := range statrow.Static.Fields {
		native[typ] = nativebind.BindFields(typ, fields, nativebind.StaticOnly)
	}
	generated := binding.WithNativeSchema(native)
	child := model.PropByName("profile").TypeModel()
	pkg := evm.Package{Path: "m", Records: []*evm.RecordType{
		evm.NewRecordType("Customer", model, false), evm.NewRecordType("CustomerEdit", model, true),
		evm.NewRecordType("Profile", child, false), evm.NewRecordType("ProfileEdit", child, true),
	}}
	module, err := evmcompiler.Compile("write_options.go", writeOptionsScript, pkg)
	if err != nil {
		b.Fatal(err)
	}
	program, err := evm.Link(module, pkg)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		open func(*statrow.State) kvo.Record
	}{
		{"Fast2", func(r *statrow.State) kvo.Record {
			return binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(r).Elem(), row2.Fast2)))
		}},
		{"Compact2", func(r *statrow.State) kvo.Record {
			return binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(r).Elem(), row2.Compact2)))
		}},
		{"MessagePack", func(r *statrow.State) kvo.Record {
			return binding.MessagePack(msgpack.Encode(nil, reflect.ValueOf(r)))
		}},
		{"Reflected", func(r *statrow.State) kvo.Record { return binding.Record(reflect.ValueOf(r)) }},
		{"Generated", func(r *statrow.State) kvo.Record { return generated.Record(reflect.ValueOf(r)) }},
		{"Memory", func(r *statrow.State) kvo.Record {
			return kvo.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(r).Elem(), row2.Fast2))).WithModel(model).Fastest()
		}},
	} {
		for _, entry := range []string{"Direct", "Nested"} {
			b.Run(tc.name+"/"+entry, func(b *testing.B) {
				e := tc.open(statrow.Sample(7)).Edit()
				vm := evm.New(program)
				vm.Limits.AllocationBytes, vm.Limits.Allocations = ^uint64(0), ^uint64(0)
				run := func() {
					ctx, scope := evm.WithRecords(context.Background())
					args := [1]values.Value{scope.EditRecord(model, e)}
					var out [1]values.Value
					err := vm.Run(ctx, entry, args[:], out[:])
					scope.Close()
					if err != nil || out[0].R != 128 {
						b.Fatalf("sum = %d, error = %v", out[0].R, err)
					}
				}
				run()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/256, "ns/write")
				b.ReportMetric(256, "writes/op")
			})
		}
	}
}

type recordOptionsSchema map[reflect.Type]*nativebind.Struct

func (s recordOptionsSchema) NativeFields(t reflect.Type) *nativebind.Struct { return s[t] }
func (s recordOptionsSchema) NativeType(t reflect.Type) *values.Type {
	if s[t] == nil {
		return nil
	}
	return values.NewType(t.String(), values.Pointer)
}
