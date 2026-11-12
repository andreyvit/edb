package kvo_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

func BenchmarkEditorPropertyAccess(b *testing.B) {
	_, model := statrow.Model()
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	for _, tc := range recordContractCases(binding, model) {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("Get", func(b *testing.B) {
				e := tc.open(statrow.Sample(7)).Edit()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					accessSum = uint64(e.GetValue(19).AsInt())
				}
			})
			b.Run("GetPending", func(b *testing.B) {
				e := tc.open(statrow.Sample(7)).Edit()
				e.SetValue(19, values.Int(1000))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					accessSum = uint64(e.GetValue(19).AsInt())
				}
			})
			b.Run("Set", func(b *testing.B) {
				e := tc.open(statrow.Sample(7)).Edit()
				e.SetValue(19, values.Int(1000))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					e.SetValue(19, values.Int(1001+int64(i&1)))
				}
			})
		})
	}
}

func BenchmarkEditorWriteOptions(b *testing.B) {
	_, source := statrow.Model()
	native := optionsBenchmarkSchema{}
	for typ, fields := range statrow.Static.Fields {
		native[typ] = nativebind.BindFields(typ, fields, nativebind.StaticOnly)
	}
	for _, density := range []struct {
		name  string
		scale uint64
	}{{"Dense", 1}, {"Sparse", 4096}} {
		s := kvo.NewSchema()
		for _, p := range source.AllProps() {
			kvo.NewProp(s, p.Code()*density.scale, p.Name(), p.AnyType(), nil)
		}
		model := kvo.NewModel(s, kvo.NewEntityType(s, "options", reflect.TypeFor[statrow.State]()), func(m *kvo.ModelBuilder) {
			for _, p := range source.AllProps() {
				flags := source.PropFlags(p.Code())
				if p.Code() == 1 {
					flags |= kvo.PrimaryKey
				}
				m.Prop(s.MustPropByCode(p.Code()*density.scale), flags)
			}
		})
		binding := kvo.Bind(reflect.TypeFor[statrow.State](), model)
		generated := binding.WithNativeSchema(native)
		cases := []struct {
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
		}
		for _, field := range []struct {
			name    string
			key     uint64
			options row2.KeyAccessOptions
		}{{"OmitZero", 19 * density.scale, 0}, {"StoreZero", density.scale, row2.Present}} {
			for _, tc := range cases {
				b.Run(density.name+"/"+field.name+"/"+tc.name, func(b *testing.B) {
					b.Run("Explicit", func(b *testing.B) {
						e := tc.open(statrow.Sample(7)).Edit()
						kvo.SetExplicitOptionsForTest(e, field.key, field.options, values.Uint(1))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							kvo.SetExplicitOptionsForTest(e, field.key, field.options, values.Uint(uint64(i&1)))
						}
					})
					b.Run("Cached", func(b *testing.B) {
						e := tc.open(statrow.Sample(7)).Edit()
						kvo.SetCachedOptionsForTest(e, field.key, values.Uint(1))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							kvo.SetCachedOptionsForTest(e, field.key, values.Uint(uint64(i&1)))
						}
					})
					b.Run("Schema", func(b *testing.B) {
						e := tc.open(statrow.Sample(7)).Edit()
						kvo.SetSchemaOptionsForTest(e, field.key, values.Uint(1))
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							kvo.SetSchemaOptionsForTest(e, field.key, values.Uint(uint64(i&1)))
						}
					})
				})
			}
		}
	}
}

type optionsBenchmarkSchema map[reflect.Type]*nativebind.Struct

func (s optionsBenchmarkSchema) NativeFields(t reflect.Type) *nativebind.Struct { return s[t] }
func (s optionsBenchmarkSchema) NativeType(t reflect.Type) *values.Type {
	if s[t] == nil {
		return nil
	}
	return values.NewType(t.String(), values.Pointer)
}
