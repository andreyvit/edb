package kvo_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

var accessSum uint64
var accessBytes []byte

// Each iteration opens one record, pays any decode cost, then performs the
// requested passes. Registering the model and encoding fixtures are excluded.
func BenchmarkAccessStrategy(b *testing.B) {
	_, m := statrow.Model()
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), m).WithGenerated(&statrow.Static, nativebind.StaticOnly)
	src := statrow.Sample(13)
	mp, err := msgpack.TryEncode(nil, reflect.ValueOf(src))
	if err != nil {
		b.Fatal(err)
	}
	fast := binding.Encode(nil, reflect.ValueOf(src).Elem(), row2.Fast2)
	compact := binding.Encode(nil, reflect.ValueOf(src).Elem(), row2.Compact2)
	for _, tc := range []struct {
		name string
		open func() kvo.Record
	}{
		{"MessagePack", func() kvo.Record { return binding.MessagePack(mp) }},
		{"Fast2", func() kvo.Record { return binding.Encoded(row2.Trusted(fast)) }},
		{"Compact2", func() kvo.Record { return binding.Encoded(row2.Trusted(compact)) }},
	} {
		for _, fields := range []string{"one", "scalars", "all"} {
			for _, passes := range []int{1, 4, 16} {
				for _, decode := range []bool{false, true} {
					strategy := "direct"
					if decode {
						strategy = "Fastest"
					}
					b.Run(fmt.Sprintf("%s/%s/%d/%s", tc.name, fields, passes, strategy), func(b *testing.B) {
						b.ReportAllocs()
						var total uint64
						for i := 0; i < b.N; i++ {
							r := tc.open()
							if decode {
								r = r.Fastest()
							}
							for range passes {
								total += r.GetValue(1).AsUint()
								if fields != "one" {
									for _, key := range []uint64{3, 11, 12, 19, 20, 21} {
										total += r.GetValue(key).AsUint()
									}
									child := r.Child(row2.Step{Key: 9})
									total += child.GetValue(5).AsUint() + child.GetValue(7).AsUint()
								}
								if fields == "all" {
									total += r.GetValue(13).AsUint() + r.GetValue(14).AsUint()
									for _, key := range []uint64{15, 16, 17, 18} {
										total += uint64(row2.Trusted(r.GetValue(key).AsBytes()).Len())
									}
								}
							}
						}
						accessSum = total
					})
				}
			}
		}
		b.Run(tc.name+"/edit", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e := tc.open().Edit()
				if err := e.TryFastSetPathValue([]row2.Step{{Key: 9}, {Key: 5}}, values.Uint(900)); err != nil {
					b.Fatal(err)
				}
				if raw := e.MessagePack(); raw != nil {
					accessBytes = raw
				} else {
					accessBytes = e.Snapshot().Raw()
				}
			}
		})
	}
	// Previous MessagePack strategy, including re-encoding the complete struct.
	b.Run("MessagePack/decode-edit-encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var dst statrow.State
			if err := msgpack.TryDecode(mp, reflect.ValueOf(&dst)); err != nil {
				b.Fatal(err)
			}
			dst.Profile.Balance = 900
			out, err := msgpack.TryEncode(nil, reflect.ValueOf(&dst))
			if err != nil {
				b.Fatal(err)
			}
			accessBytes = out
		}
	})
}

// A wider scalar-only record makes Compact2's repeated field scans visible.
// This binding uses reflection, since the shape is assembled for the benchmark.
func BenchmarkWideAccessStrategy(b *testing.B) {
	const fields = 64
	decl := make([]reflect.StructField, fields)
	for i := range decl {
		decl[i] = reflect.StructField{Name: fmt.Sprintf("F%d", i), Type: reflect.TypeFor[uint64](), Tag: reflect.StructTag(fmt.Sprintf(`edb:"field%d" msgpack:"f%d"`, i, i))}
	}
	typ := reflect.StructOf(decl)
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "wide", typ)
	for i := range fields {
		kvo.NewProp(s, uint64(i+1), fmt.Sprintf("field%d", i), kvo.TUint64, nil)
	}
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) {
		for i := range fields {
			b.Prop(s.MustPropByCode(uint64(i + 1)))
		}
	})
	binding := kvo.Bind(typ, m)
	src := reflect.New(typ)
	for i := range fields {
		src.Elem().Field(i).SetUint(uint64(i + 1))
	}
	mp, err := msgpack.TryEncode(nil, src)
	if err != nil {
		b.Fatal(err)
	}
	fast := binding.Encode(nil, src.Elem(), row2.Fast2)
	compact := binding.Encode(nil, src.Elem(), row2.Compact2)
	for _, tc := range []struct {
		name string
		open func() kvo.Record
	}{
		{"MessagePack", func() kvo.Record { return binding.MessagePack(mp) }},
		{"Fast2", func() kvo.Record { return binding.Encoded(row2.Trusted(fast)) }},
		{"Compact2", func() kvo.Record { return binding.Encoded(row2.Trusted(compact)) }},
	} {
		for _, passes := range []int{1, 4, 16} {
			for _, decode := range []bool{false, true} {
				strategy := "direct"
				if decode {
					strategy = "Fastest"
				}
				b.Run(fmt.Sprintf("%s/%d/%s", tc.name, passes, strategy), func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					for i := 0; i < b.N; i++ {
						r := tc.open()
						if decode {
							r = r.Fastest()
						}
						for range passes {
							for key := uint64(1); key <= fields; key++ {
								sum += r.GetValue(key).AsUint()
							}
						}
					}
					accessSum = sum
				})
			}
		}
	}
}

// Registration, serialization and decoding are outside the timer. All KVO
// alternatives expose the same Record API over the same logical values.
func BenchmarkDecodedAccess(b *testing.B) {
	_, m := statrow.Model()
	reflected := kvo.Bind(reflect.TypeFor[statrow.State](), m)
	generated := reflected.WithGenerated(&statrow.Static, nativebind.StaticOnly)
	src := statrow.Sample(13)
	fast := kvo.Encoded(row2.Trusted(generated.Encode(nil, reflect.ValueOf(src).Elem(), row2.Fast2))).WithModel(m)
	for _, tc := range []struct {
		name string
		r    kvo.Record
	}{
		{"Fast2", fast},
		{"Decoded", fast.WithModel(m).Fastest()},
		{"GeneratedNative", generated.Record(reflect.ValueOf(statrow.Sample(13)))},
		{"ReflectedNative", reflected.Record(reflect.ValueOf(statrow.Sample(13)))},
	} {
		b.Run(tc.name+"/one", func(b *testing.B) {
			b.ReportAllocs()
			var n uint64
			for i := 0; i < b.N; i++ {
				n += tc.r.GetValue(1).AsUint()
			}
			accessSum = n
		})
		b.Run(tc.name+"/scalars", func(b *testing.B) {
			b.ReportAllocs()
			var n uint64
			for i := 0; i < b.N; i++ {
				for _, key := range []uint64{1, 3, 11, 12, 19, 20, 21} {
					n += tc.r.GetValue(key).AsUint()
				}
				child := tc.r.Child(row2.Step{Key: 9})
				n += child.GetValue(5).AsUint() + child.GetValue(7).AsUint()
			}
			accessSum = n
		})
		b.Run(tc.name+"/write", func(b *testing.B) {
			e := tc.r.Edit()
			path := []row2.Step{{Key: 11}}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := e.TryFastSetPathValue(path, values.Uint(uint64(i))); err != nil {
					b.Fatal(err)
				}
			}
			accessSum = e.GetPathValue(path).AsUint()
		})
	}
	b.Run("DirectGo/one", func(b *testing.B) {
		var n uint64
		for i := 0; i < b.N; i++ {
			n += src.ID
		}
		accessSum = n
	})
}

// Include the one-time materialization cost, then read nine scalar fields per
// pass. This is distinct from BenchmarkDecodedAccess's steady-state timings.
func BenchmarkMaterialization(b *testing.B) {
	_, m := statrow.Model()
	reflected := kvo.Bind(reflect.TypeFor[statrow.State](), m)
	generated := reflected.WithGenerated(&statrow.Static, nativebind.StaticOnly)
	src := statrow.Sample(13)
	mp, err := msgpack.TryEncode(nil, reflect.ValueOf(src))
	if err != nil {
		b.Fatal(err)
	}
	for _, format := range []string{"Fast2", "Compact2", "MessagePack"} {
		layout := row2.Compact2
		if format == "Fast2" {
			layout = row2.Fast2
		}
		raw := generated.Encode(nil, reflect.ValueOf(src).Elem(), layout)
		var gen, ref kvo.Record
		if format == "MessagePack" {
			gen = generated.MessagePack(mp)
			ref = reflected.MessagePack(mp)
		} else {
			gen = generated.Encoded(row2.Trusted(raw))
			ref = reflected.Encoded(row2.Trusted(raw))
		}
		strategies := []struct {
			name   string
			source kvo.Record
			decode bool
		}{
			{"direct", gen, false}, {"generated", gen, true}, {"reflected", ref, true},
		}
		if format != "MessagePack" {
			strategies = append(strategies, struct {
				name   string
				source kvo.Record
				decode bool
			}{"generic", kvo.Encoded(row2.Trusted(raw)).WithModel(m), true})
		}
		for _, tc := range strategies {
			for _, passes := range []int{0, 1, 16, 256} {
				b.Run(fmt.Sprintf("%s/%s/%d", format, tc.name, passes), func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					for i := 0; i < b.N; i++ {
						r := tc.source
						if tc.decode {
							r = r.Fastest()
						}
						for range passes {
							for _, k := range []uint64{1, 3, 11, 12, 19, 20, 21} {
								sum += r.GetValue(k).AsUint()
							}
							child := r.Child(row2.Step{Key: 9})
							sum += child.GetValue(5).AsUint() + child.GetValue(7).AsUint()
						}
						if passes == 0 {
							accessRecord = r
						}
					}
					accessSum = sum
				})
			}
		}
	}
}

var accessRecord kvo.Record
