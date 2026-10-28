package kvolab

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

func TestWords(t *testing.T) {
	for _, n := range []int{8, 32, 128, 512} {
		v := numeric(n, 0)
		w := EncodeWords(v)
		for _, f := range v.Fields {
			if w.Get(f.Key) != f.Value.U {
				t.Fatal("mismatch")
			}
		}
		if w.Get(9999) != 0 {
			t.Fatal("missing")
		}
	}
}
func TestWordEditorStack(t *testing.T) {
	base := EncodeWords(numeric(128, 0))
	var out [8192]byte
	if n := testing.AllocsPerRun(1000, func() {
		e := WordEditor{Base: base}
		e.Set(7, 99)
		b := e.Append(out[:0])
		if Words(b).Get(7) != 99 {
			panic("bad")
		}
	}); n != 0 {
		t.Fatalf("%g allocations", n)
	}
}
func BenchmarkWords(b *testing.B) {
	for _, n := range []int{8, 32, 128, 512} {
		v := numeric(n, 0)
		data := EncodeWords(v)
		for _, name := range []string{"get", "stack-edit", "stack-edit-pack"} {
			b.Run(fmt.Sprintf("n%d/%s", n, name), func(b *testing.B) {
				b.ReportAllocs()
				buf := make([]byte, 0, len(data))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := query(i, n)
					switch name {
					case "get":
						sinkU = data.Get(key)
					case "stack-edit":
						e := WordEditor{Base: data}
						e.Set(key, uint64(i+1))
						sinkU = e.Get(key)
					case "stack-edit-pack":
						e := WordEditor{Base: data}
						e.Set(key, uint64(i+1))
						buf = e.Append(buf[:0])
						sinkU = uint64(len(buf))
					}
				}
			})
		}
	}
}
func BenchmarkRawScan(b *testing.B) {
	for _, n := range []int{32, 128, 512} {
		data := oldKVO(numeric(n, 0)).RootObject()
		b.Run(fmt.Sprintf("n%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				for _, x := range data[len(data)/2:] {
					sum += x
				}
			}
			sinkU = sum
		})
	}
}
func BenchmarkCacheRead(b *testing.B) {
	const n = 128
	for _, rows := range []int{1, 65536} {
		for _, name := range []string{"kvo", "words", "compact", "indexed", "msgpack"} {
			v := numeric(n, 0)
			var sample []byte
			switch name {
			case "kvo":
				sample = oldKVO(v).Bytes()
			case "words":
				sample = EncodeWords(v)
			case "compact":
				sample = Encode(v, Compact)
			case "indexed":
				sample = Encode(v, Indexed)
			case "msgpack":
				sample = mpEncode(v, false)
			}
			b.Run(fmt.Sprintf("rows%d/%s", rows, name), func(b *testing.B) {
				data := bytes.Repeat(sample, rows)
				size := len(sample)
				b.ReportMetric(float64(len(data)), "dataset-B")
				b.ReportAllocs()
				b.ResetTimer()
				rng := uint64(0xdedabcde12345678)
				var sum uint64
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					row := int(rng>>16) & (rows - 1)
					key := (rng & 127) + 1
					raw := data[row*size : (row+1)*size]
					switch name {
					case "kvo":
						sum += kvo.LoadRecordData(raw).RootObject().MapGet(key)
					case "words":
						sum += Words(raw).Get(key)
					case "compact":
						c, _ := View(raw).Get(key)
						sum += c.U
					case "indexed":
						c, _ := View(raw).Get(key)
						sum += c.U
					case "msgpack":
						c, _ := MPView(raw).Get(key)
						sum += c.Uint()
					}
				}
				sinkU = sum
			})
		}
	}
}
func TestIDSensitivity(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 21))
	for _, mode := range []string{"local", "global", "spaced", "random"} {
		v := numeric(128, 0)
		var key uint64
		for i := range v.Fields {
			switch mode {
			case "local":
				key++
			case "global":
				key = uint64(300 + i)
			case "spaced":
				key += uint64(100000 + 997*i)
			case "random":
				key += rng.Uint64N(1<<55) + 1
			}
			v.Fields[i].Key = key
		}
		t.Logf("keys=%s compact=%d words=%d kvo=%d mp-numeric=%d", mode, len(Encode(v, Compact)), len(EncodeWords(v)), len(oldKVO(v).Bytes()), len(mpEncode(v, false)))
	}
}

func BenchmarkEditDensity(b *testing.B) {
	const n = 128
	fixture := numeric(n, 0)
	original := oldKVO(fixture).Record(nil)
	words := EncodeWords(fixture)
	for _, edits := range []int{1, 8, 32, 128} {
		for _, name := range []string{"kvo-read-overlay", "words-read-owned", "kvo-edit-pack", "words-edit-pack"} {
			b.Run(fmt.Sprintf("edits%d/%s", edits, name), func(b *testing.B) {
				m := kvo.UpdateRecord(original)
				owned := bytes.Clone(words)
				for i := 0; i < edits; i++ {
					key := uint64(i + 1)
					m.Set(key, uint64(10000+i))
					Words(owned).SetExisting(key, uint64(10000+i))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					switch name {
					case "kvo-read-overlay":
						sinkU = m.Get(query(i, n))
					case "words-read-owned":
						sinkU = Words(owned).Get(query(i, n))
					case "kvo-edit-pack":
						e := kvo.UpdateRecord(original)
						for j := 0; j < edits; j++ {
							e.Set(uint64(j+1), uint64(i+j+1))
						}
						sinkBytes = e.Record().Pack().Bytes()
					case "words-edit-pack":
						copy(owned, words)
						for j := 0; j < edits; j++ {
							Words(owned).SetExisting(uint64(j+1), uint64(i+j+1))
						}
						sinkU = uint64(len(owned))
					}
				}
			})
		}
	}
}
