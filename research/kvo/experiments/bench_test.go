package kvolab

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/vmihailenco/msgpack/v5"
)

var sinkU uint64
var sinkBytes []byte
var sinkAny any

func query(i, n int) uint64 { return uint64((uint(i)*2654435761)%uint(n)) + 1 }
func BenchmarkRead(b *testing.B) {
	for _, n := range []int{8, 32, 128, 512} {
		v := numeric(n, 0)
		old := oldKVO(v).Record(nil).Root()
		compact := View(Encode(v, Compact))
		indexed := View(Encode(v, Indexed))
		offsets := View(Encode(v, Offsets))
		mp := MPView(mpEncode(v, false))
		values := make([]uint64, n+1)
		for _, f := range v.Fields {
			values[f.Key] = f.Value.U
		}
		cases := []struct {
			name string
			get  func(uint64) uint64
		}{{"kvo", old.Get}, {"compact", func(k uint64) uint64 { c, _ := compact.Get(k); return c.U }}, {"indexed", func(k uint64) uint64 { c, _ := indexed.Get(k); return c.U }}, {"offsets", func(k uint64) uint64 { c, _ := offsets.Get(k); return c.U }}, {"msgpack-view", func(k uint64) uint64 { c, _ := mp.Get(k); return c.Uint() }}, {"decoded-slots", func(k uint64) uint64 { return values[k] }}}
		for _, c := range cases {
			b.Run(fmt.Sprintf("n%d/%s", n, c.name), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					sum += c.get(query(i, n))
				}
				sinkU = sum
			})
		}
	}
}
func BenchmarkScan(b *testing.B) {
	for _, n := range []int{32, 128, 512} {
		v := numeric(n, 0)
		old := oldKVO(v).Record(nil).Root()
		compact := View(Encode(v, Compact))
		indexed := View(Encode(v, Indexed))
		offsets := View(Encode(v, Offsets))
		mp := MPView(mpEncode(v, false))
		for _, name := range []string{"kvo", "compact", "indexed", "offsets", "msgpack-view", "msgpack-decode-map", "decode-slots"} {
			b.Run(fmt.Sprintf("n%d/%s", n, name), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					switch name {
					case "kvo":
						for _, x := range old.Items() {
							sum += x
						}
					case "compact":
						compact.Walk(func(_ uint64, c Cell) bool { sum += c.U; return true })
					case "indexed":
						indexed.Walk(func(_ uint64, c Cell) bool { sum += c.U; return true })
					case "offsets":
						offsets.Walk(func(_ uint64, c Cell) bool { sum += c.U; return true })
					case "msgpack-view":
						mp.Walk(func(_ uint64, c MPView) bool { sum += c.Uint(); return true })
					case "msgpack-decode-map":
						var out map[uint64]uint64
						if err := msgpack.Unmarshal(mp, &out); err != nil {
							b.Fatal(err)
						}
						for _, x := range out {
							sum += x
						}
					case "decode-slots":
						var out [513]uint64
						compact.Walk(func(k uint64, c Cell) bool { out[k] = c.U; return true })
						for j := 1; j <= n; j++ {
							sum += out[j]
						}
					}
				}
				sinkU = sum
			})
		}
	}
}
func BenchmarkDictionary(b *testing.B) {
	for _, n := range []int{8, 32, 128, 512} {
		for _, long := range []bool{false, true} {
			v := dictionary(n, long)
			cv := View(Encode(v, Compact))
			iv := View(Encode(v, Indexed))
			ov := View(Encode(v, Offsets))
			mp := MPView(mpEncode(v, true))
			keys := make([]string, n)
			m := make(map[string]string, n)
			for i, e := range v.Entries {
				keys[i] = e.Key
				m[e.Key] = e.Value.S
			}
			for _, name := range []string{"compact", "indexed", "offsets", "msgpack-view", "go-map"} {
				b.Run(fmt.Sprintf("n%d/long%t/%s", n, long, name), func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					for i := 0; i < b.N; i++ {
						key := keys[(uint(i)*2654435761)%uint(n)]
						switch name {
						case "compact":
							c, _ := cv.GetString(key)
							sum += uint64(len(c.Bytes()))
						case "indexed":
							c, _ := iv.GetString(key)
							sum += uint64(len(c.Bytes()))
						case "msgpack-view":
							c, _ := mp.GetString(key)
							sum += uint64(len(c.Bytes()))
						case "offsets":
							c, _ := ov.GetString(key)
							sum += uint64(len(c.Bytes()))
						case "go-map":
							sum += uint64(len(m[key]))
						}
					}
					sinkU = sum
				})
			}
		}
	}
}
func BenchmarkNested(b *testing.B) {
	v := fixtures()["state"]
	for _, name := range []string{"compact", "indexed", "mixed", "msgpack-view", "msgpack-decode"} {
		var data []byte
		switch name {
		case "compact":
			data = Encode(v, Compact)
		case "indexed":
			data = Encode(v, Indexed)
		case "mixed":
			data = EncodeMixed(v, Indexed, Compact)
		default:
			data = mpEncode(v, false)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				if name == "msgpack-decode" {
					var m map[uint64]any
					dec := msgpack.NewDecoder(bytes.NewReader(data))
					dec.SetMapDecoder(func(d *msgpack.Decoder) (any, error) { return d.DecodeUntypedMap() })
					if err := dec.Decode(&m); err != nil {
						b.Fatal(err)
					}
					sinkAny = m
				} else if name == "msgpack-view" {
					x, _ := MPView(data).Get(57)
					y, _ := x.Get(5)
					z, _ := y.GetString("key-0003")
					sum += uint64(len(z.Bytes()))
				} else {
					x, _ := View(data).Get(57)
					y, _ := x.Object().Get(5)
					z, _ := y.Object().GetString("key-0003")
					sum += uint64(len(z.Bytes()))
				}
			}
			sinkU = sum
		})
	}
}
func BenchmarkUpdate(b *testing.B) {
	for _, n := range []int{32, 128, 512} {
		v := numeric(n, 0)
		original := oldKVO(v).Record(nil)
		data := Encode(v, Indexed)
		for _, name := range []string{"kvo-overlay-only", "kvo-pack", "kvo-double-pack", "indexed-copy-patch", "indexed-owned-patch", "indexed-reused-copy-patch"} {
			b.Run(fmt.Sprintf("n%d/%s", n, name), func(b *testing.B) {
				b.ReportAllocs()
				buf := bytes.Clone(data)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := query(i, n)
					switch name {
					case "kvo-overlay-only":
						m := kvo.UpdateRecord(original)
						m.Set(key, uint64(i+1))
						sinkU = m.Get(key)
					case "kvo-pack":
						m := kvo.UpdateRecord(original)
						m.Set(key, uint64(i+1))
						sinkBytes = m.Record().Pack().Bytes()
					case "kvo-double-pack":
						m := kvo.UpdateRecord(original)
						m.Set(key, uint64(i+1))
						sinkBytes = m.Record().Pack().Bytes()
						sinkBytes = m.Record().Pack().Bytes()
					case "indexed-copy-patch":
						out := bytes.Clone(data)
						View(out).SetExisting(key, uint64(i+1))
						sinkBytes = out
					case "indexed-owned-patch":
						View(buf).SetExisting(key, uint64(i+1))
					case "indexed-reused-copy-patch":
						copy(buf, data)
						View(buf).SetExisting(key, uint64(i+1))
					}
				}
			})
		}
	}
}

// Bitmap is a candidate for dense local IDs, not arbitrary map keys.
func BenchmarkBitmap(b *testing.B) {
	v := numeric(128, 0)
	var present [8]uint64
	values := make([]uint64, 0, 128)
	for _, f := range v.Fields {
		present[f.Key/64] |= 1 << (f.Key % 64)
		values = append(values, f.Value.U)
	}
	b.ReportAllocs()
	var sum uint64
	for i := 0; i < b.N; i++ {
		k := query(i, 128)
		rank := 0
		for w := uint64(0); w < k/64; w++ {
			rank += bits.OnesCount64(present[w])
		}
		rank += bits.OnesCount64(present[k/64] & ((1 << (k % 64)) - 1))
		sum += values[rank]
	}
	sinkU = sum
}
