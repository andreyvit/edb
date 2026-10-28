package kvolab

// Scenarios use nonuniform widths/lengths from the retained anonymous replay.
// Field count and access repetitions are explicit scenarios, not a workload trace.
import (
	"bytes"
	"fmt"
	"github.com/andreyvit/edb/kvo"
	"reflect"
	"testing"
	"unsafe"
)

type nativeMetrics struct {
	N01 uint64 `edb:"n01"`
	N02 uint64 `edb:"n02"`
	N03 uint64 `edb:"n03"`
	N04 uint64 `edb:"n04"`
	N05 uint64 `edb:"n05"`
	N06 uint64 `edb:"n06"`
	N07 uint64 `edb:"n07"`
	N08 uint64 `edb:"n08"`
	N09 uint64 `edb:"n09"`
	N10 uint64 `edb:"n10"`
	N11 uint64 `edb:"n11"`
	N12 uint64 `edb:"n12"`
	N13 uint64 `edb:"n13"`
	N14 uint64 `edb:"n14"`
	N15 uint64 `edb:"n15"`
	N16 uint64 `edb:"n16"`
	N17 uint64 `edb:"n17"`
	N18 uint64 `edb:"n18"`
	N19 uint64 `edb:"n19"`
	N20 uint64 `edb:"n20"`
	S1  string `edb:"s1"`
	S2  string `edb:"s2"`
	S3  string `edb:"s3"`
	S4  string `edb:"s4"`
}
type nativeMetricTail struct {
	Last  uint64 `edb:"n20"`
	Label string `edb:"s4"`
}

var nativeMetricType = func() *kvo.EntityType {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "metrics", reflect.TypeFor[nativeMetrics](), reflect.TypeFor[nativeMetricTail]())
	for i := 1; i <= 24; i++ {
		name := fmt.Sprintf("n%02d", i)
		var scalar kvo.AnyType = kvo.TUint64
		if i > 20 {
			name = fmt.Sprintf("s%d", i-20)
			scalar = kvo.TMachineString
		}
		kvo.NewProp(s, uint64(i), name, scalar, nil)
	}
	kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) {
		for i := uint64(1); i <= 24; i++ {
			b.Prop(i)
		}
	})
	return typ
}()

func nativeMetricSample(tb testing.TB, name string) (nativeMetrics, Value) {
	r := rowFixtures(tb)[name]
	var words []uint64
	var texts []string
	for _, l := range r.leaves {
		if l.kind == Uint {
			words = append(words, l.value.Uint())
		}
		if l.kind == Text {
			texts = append(texts, string(l.value.Bytes()))
		}
	}
	if len(words) == 0 {
		words = []uint64{1, 1 << 63}
	}
	if len(texts) == 0 {
		texts = []string{"sample"}
	}
	out := nativeMetrics{}
	v := reflect.ValueOf(&out).Elem()
	source := Obj()
	for i := 0; i < 24; i++ {
		if i < 20 {
			n := words[i%len(words)]
			v.Field(i).SetUint(n)
			source.Fields = append(source.Fields, Field{uint64(i + 1), U(n)})
		} else {
			text := texts[(i-20)%len(texts)]
			v.Field(i).SetString(text)
			source.Fields = append(source.Fields, Field{uint64(i + 1), S(text)})
		}
	}
	return out, source
}

//go:noinline
func nativeMetricSum(r *nativeMetrics) uint64 {
	return r.N01 + r.N02 + r.N03 + r.N04 + r.N05 + r.N06 + r.N07 + r.N08 + r.N09 + r.N10 + r.N11 + r.N12 + r.N13 + r.N14 + r.N15 + r.N16 + r.N17 + r.N18 + r.N19 + r.N20 + uint64(len(r.S1)) + uint64(len(r.S2)) + uint64(len(r.S3)) + uint64(len(r.S4))
}

//go:noinline
func nativeMetricKeySum(view currentView) uint64 {
	var sum uint64
	for k := uint64(1); k <= 20; k++ {
		sum += view.Get(k).Uint()
	}
	for k := uint64(21); k <= 24; k++ {
		sum += uint64(len(view.Get(k).Bytes()))
	}
	return sum
}

//go:noinline
func nativeMetricScanSum(view currentView) uint64 {
	var sum uint64
	it := view.Iter()
	for {
		key, _, v, ok := it.Next()
		if !ok {
			break
		}
		if key <= 20 {
			sum += v.Uint()
		} else {
			sum += uint64(len(v.Bytes()))
		}
	}
	return sum
}

// Native generated-code control: no reflection, but includes row traversal and
// field writes. Kept runnable to distinguish API convenience from binding cost.
func nativeMetricGenerated(view currentView, out *nativeMetrics) {
	*out = nativeMetrics{}
	it := view.Iter()
	for {
		key, _, v, ok := it.Next()
		if !ok {
			break
		}
		switch key {
		case 1:
			out.N01 = v.Uint()
		case 2:
			out.N02 = v.Uint()
		case 3:
			out.N03 = v.Uint()
		case 4:
			out.N04 = v.Uint()
		case 5:
			out.N05 = v.Uint()
		case 6:
			out.N06 = v.Uint()
		case 7:
			out.N07 = v.Uint()
		case 8:
			out.N08 = v.Uint()
		case 9:
			out.N09 = v.Uint()
		case 10:
			out.N10 = v.Uint()
		case 11:
			out.N11 = v.Uint()
		case 12:
			out.N12 = v.Uint()
		case 13:
			out.N13 = v.Uint()
		case 14:
			out.N14 = v.Uint()
		case 15:
			out.N15 = v.Uint()
		case 16:
			out.N16 = v.Uint()
		case 17:
			out.N17 = v.Uint()
		case 18:
			out.N18 = v.Uint()
		case 19:
			out.N19 = v.Uint()
		case 20:
			out.N20 = v.Uint()
		case 21:
			out.S1 = unsafe.String(unsafe.SliceData(v.Bytes()), len(v.Bytes()))
		case 22:
			out.S2 = unsafe.String(unsafe.SliceData(v.Bytes()), len(v.Bytes()))
		case 23:
			out.S3 = unsafe.String(unsafe.SliceData(v.Bytes()), len(v.Bytes()))
		case 24:
			out.S4 = unsafe.String(unsafe.SliceData(v.Bytes()), len(v.Bytes()))
		}
	}
}

func TestNativeMetricControls(t *testing.T) {
	for _, name := range []string{"row-small", "row-wide"} {
		src, _ := nativeMetricSample(t, name)
		c := bindNative[nativeMetrics](nativeMetricType)
		for _, fast := range []bool{false, true} {
			raw := c.Put(nil, &src, fast)
			view := currentTrusted(raw)
			var a, b nativeMetrics
			c.GetFast(view, &a)
			nativeMetricGenerated(view, &b)
			want := nativeMetricSum(&src)
			if a != src || b != src || nativeMetricScanSum(view) != want || nativeMetricKeySum(view) != want {
				t.Fatal("controls disagree")
			}
		}
	}
}

func BenchmarkNativeAccess(b *testing.B) {
	for _, shape := range []string{"row-small", "row-wide"} {
		src, _ := nativeMetricSample(b, shape)
		for _, fast := range []bool{true, false} {
			b.Run(fmt.Sprintf("%s/fast%v", shape, fast), func(b *testing.B) {
				c := bindNative[nativeMetrics](nativeMetricType)
				raw := c.Put(nil, &src, fast)
				d := newMmapDataset(b, raw, 1024)
				warmMmapPayloads(d)
				for _, passes := range []int{1, 4, 16} {
					for _, mode := range []string{"keys", "scan", "decode-reuse", "decode-generated", "get-owned"} {
						b.Run(fmt.Sprintf("passes%d/%s", passes, mode), func(b *testing.B) {
							var out nativeMetrics
							var sum uint64
							c.GetFast(currentTrusted(d.rows[0]), &out)
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								view := currentTrusted(d.rows[i&1023])
								switch mode {
								case "keys":
									for j := 0; j < passes; j++ {
										sum += nativeMetricKeySum(view)
									}
								case "scan":
									for j := 0; j < passes; j++ {
										sum += nativeMetricScanSum(view)
									}
								case "decode-reuse":
									c.GetFast(view, &out)
									for j := 0; j < passes; j++ {
										sum += nativeMetricSum(&out)
									}
								case "decode-generated":
									nativeMetricGenerated(view, &out)
									for j := 0; j < passes; j++ {
										sum += nativeMetricSum(&out)
									}
								case "get-owned":
									p := c.Get(view)
									for j := 0; j < passes; j++ {
										sum += nativeMetricSum(p)
									}
								}
							}
							sinkU = sum
							b.ReportMetric(float64(len(raw)), "row-B")
						})
					}
				}
				b.Run("native-already-decoded", func(b *testing.B) {
					p := c.Get(currentTrusted(raw))
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						sum += nativeMetricSum(p)
					}
					sinkU = sum
				})
				for _, mode := range []string{"scan", "lookup", "auto"} {
					b.Run("projection-"+mode, func(b *testing.B) {
						pc := bindNative[nativeMetricTail](nativeMetricType)
						var out nativeMetricTail
						var sum uint64
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							view := currentTrusted(d.rows[i&1023])
							switch mode {
							case "scan":
								pc.GetFastScan(view, &out)
							case "lookup":
								pc.GetFastKeys(view, &out)
							default:
								pc.GetFast(view, &out)
							}
							sum += out.Last + uint64(len(out.Label))
						}
						sinkU = sum
					})
				}
				b.Run("projection-keys", func(b *testing.B) {
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						v := currentTrusted(d.rows[i&1023])
						sum += v.Get(20).Uint() + uint64(len(v.Get(24).Bytes()))
					}
					sinkU = sum
				})
			})
		}
	}
}

func nativeCollectionSample(tb testing.TB, shape string) nativeRow {
	src := nativeSample()
	r := rowFixtures(tb)[shape]
	src.Labels = map[string]string{}
	src.Values = nil
	src.Items = nil
	for _, l := range r.leaves {
		if l.kind == Uint && len(src.Values) < 64 {
			src.Values = append(src.Values, int64(l.value.Uint()))
		}
		if l.kind == Text && len(src.Items) < 32 {
			text := string(l.value.Bytes())
			src.Items = append(src.Items, nativeDetail{Enabled: true, Delta: int64(len(text)), Memo: text})
			src.Labels[fmt.Sprintf("label-%07d", len(src.Items))] = text
		}
	}
	src.Extra = nil // aggregate map variants below explicitly measure their cost
	return src
}

func BenchmarkNativeCollections(b *testing.B) {
	for _, shape := range []string{"row-small", "row-wide"} {
		for _, fast := range []bool{true, false} {
			b.Run(fmt.Sprintf("%s/fast%v", shape, fast), func(b *testing.B) {
				src := nativeCollectionSample(b, shape)
				c := bindNative[nativeRow](nativeTypes[0])
				raw := c.Put(nil, &src, fast)
				d := newMmapDataset(b, raw, 1024)
				warmMmapPayloads(d)
				b.Run("get-reuse", func(b *testing.B) {
					var out nativeRow
					c.GetFast(currentTrusted(d.rows[0]), &out)
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						c.GetFast(currentTrusted(d.rows[i&1023]), &out)
						sum += uint64(len(out.Items) + len(out.Values) + len(out.Labels))
					}
					sinkU = sum
				})
				b.Run("get-fresh", func(b *testing.B) {
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var out nativeRow
						c.GetFast(currentTrusted(d.rows[i&1023]), &out)
						sum += uint64(len(out.Items) + len(out.Values) + len(out.Labels))
					}
					sinkU = sum
				})
				b.Run("get-owned", func(b *testing.B) {
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						out := c.Get(currentTrusted(d.rows[i&1023]))
						sum += uint64(len(out.Items) + len(out.Values) + len(out.Labels))
					}
					sinkU = sum
				})
				b.Run("put-reuse", func(b *testing.B) {
					out := bytes.Clone(raw)
					out = c.Put(out[:0], &src, fast)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						out = c.Put(out[:0], &src, fast)
					}
					sinkBytes = out
					b.ReportMetric(float64(len(out)), "row-B")
				})
				b.Run("put-cold", func(b *testing.B) {
					// Plan setup is excluded, scratch and output allocation are included.
					var out []byte
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						cold := nativeCodec[nativeRow]{plan: c.plan}
						out = cold.Put(nil, &src, fast)
					}
					sinkBytes = out
				})
				edit := nativeSummary{Count: 7, Title: "edited"}
				ec := bindNative[nativeSummary](nativeTypes[0])
				b.Run("patch-two", func(b *testing.B) {
					out := ec.Patch(nil, currentTrusted(raw), &edit)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						out = ec.Patch(out[:0], currentTrusted(d.rows[i&1023]), &edit)
					}
					sinkBytes = out
				})
				b.Run("key-edit-two", func(b *testing.B) {
					var w currentWriter
					var scratch [2]classMeta
					var arena [64]byte
					out := make([]byte, 0, len(raw)+64)
					run := func(view currentView) {
						e := newCurrentLengthEditor(view, scratch[:0], arena[:0])
						e.Set(1, currentInt(7))
						e.Set(2, currentBytes(tagStringBytes("edited")))
						out = e.Append(&w, out[:0], fast)
					}
					run(currentTrusted(raw))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run(currentTrusted(d.rows[i&1023]))
					}
					sinkBytes = out
				})
			})
		}
	}
	b.Run("nested-map-values", func(b *testing.B) {
		src := nativeSample()
		src.Extra = map[string]nativeDetail{}
		for i := 0; i < 32; i++ {
			src.Extra[fmt.Sprintf("item-%d", i)] = nativeDetail{Delta: int64(i), Memo: "nested"}
		}
		c := bindNative[nativeRow](nativeTypes[0])
		raw := c.Put(nil, &src, true)
		var out nativeRow
		c.GetFast(currentTrusted(raw), &out)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			c.GetFast(currentTrusted(raw), &out)
		}
		sinkU = uint64(len(out.Extra))
	})
}
