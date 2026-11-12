package kvo_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/internal/valuebench"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

var sharedString string
var sharedData []byte

func BenchmarkSharedValue(b *testing.B) {
	schema, model := valuebench.Model()
	ref := kvo.Bind(reflect.TypeFor[valuebench.Row](), model)
	gen := ref.WithGenerated(&valuebench.Static, nativebind.StaticOnly)
	source := valuebench.Sample()
	mp, err := msgpack.TryEncode(nil, reflect.ValueOf(source))
	if err != nil {
		b.Fatal(err)
	}
	fast := gen.Encoded(row2.Trusted(gen.Encode(nil, reflect.ValueOf(source).Elem(), row2.Fast2)))
	compact := gen.Encoded(row2.Trusted(gen.Encode(nil, reflect.ValueOf(source).Elem(), row2.Compact2)))
	cases := []struct {
		name string
		r    kvo.Record
	}{
		{"Fast2", fast}, {"Compact2", compact}, {"MessagePack", gen.MessagePack(mp)},
		{"Generated", gen.Record(reflect.ValueOf(valuebench.Sample()))},
		{"Reflected", ref.Record(reflect.ValueOf(valuebench.Sample()))},
		{"Memory", kvo.Encoded(fast.Snapshot()).WithModel(model).Fastest()},
	}
	pkg, err := evm.ModelPackage("m", schema)
	if err != nil {
		b.Fatal(err)
	}
	module, err := evmcompiler.Compile("bench.go", `import "m";func Read(r *m.Row) string{return r.name}`, pkg)
	if err != nil {
		b.Fatal(err)
	}
	program, err := evm.Link(module, pkg)
	if err != nil {
		b.Fatal(err)
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.Run("Int", func(b *testing.B) {
				b.ReportAllocs()
				var sum int64
				for i := 0; i < b.N; i++ {
					sum += c.r.GetValue(1).AsInt()
				}
				accessSum = uint64(sum)
			})
			b.Run("String", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sharedString = c.r.GetOwnedString(2).AsString()
				}
			})
			b.Run("BorrowString", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sharedString = c.r.GetValue(2).AsString()
				}
			})
			b.Run("Bytes", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sharedData = c.r.GetValue(3).AsBytes()
				}
			})
			b.Run("List", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					sharedData = c.r.GetValue(4).AsBytes()
				}
			})
			b.Run("EditString", func(b *testing.B) {
				edit := c.r.Edit()
				path := []row2.Step{{Key: 2}}
				v := values.String(source.Name)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := edit.TryFastSetPathValue(path, v); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("EVMString", func(b *testing.B) {
				ctx, scope := evm.WithRecords(context.Background())
				defer scope.Close()
				vm := evm.New(program)
				vm.Limits = evm.Limits{Allocations: ^uint64(0), AllocationBytes: ^uint64(0)}
				args := []values.Value{scope.ReadRecord(model, c.r)}
				out := make([]values.Value, 1)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := vm.Run(ctx, "Read", args, out); err != nil {
						b.Fatal(err)
					}
					sharedString = out[0].AsString()
				}
			})
		})
	}
}
