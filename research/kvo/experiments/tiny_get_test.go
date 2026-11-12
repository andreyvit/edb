package kvolab

// Narrow-key byte search can lose when a path repeatedly visits one tiny
// Object. Keep linear cutoffs explicit and measured; no wire special case.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"testing"
)

func keysLastTinyGet(v lengthView, key uint64, equal bool, cutoff int) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		return keysLastPortableGet(v, key)
	}
	raw := v.Raw()
	end := len(raw) - 5
	t := raw[len(raw)-1]
	if t>>1&3 != 0 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return v.Get(key)
	}
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	keys := end - n*width
	codes := keys
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	cells := codes - n*8
	i := -1
	if width == 1 {
		if key <= 255 {
			if n <= cutoff {
				for j, k := range raw[keys:end] {
					if k == byte(key) {
						i = j
						break
					}
				}
			} else {
				i = bytes.IndexByte(raw[keys:end], byte(key))
			}
		}
	} else {
		i = perfFind(raw[keys:end], width, key, equal)
	}
	if i < 0 {
		return lengthValue{}
	}
	word := binary.LittleEndian.Uint64(raw[cells+i*8:])
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return lengthWord(word)
	}
	at, size := int(uint32(word)), int(word>>32)
	return lengthBytes(raw[at : at+size])
}
func keysLastTinyPath(raw []byte, path []perfStep, cutoff int) lengthValue {
	var value lengthValue
	for i, s := range path {
		v := lengthTrusted(raw)
		switch s.kind {
		case 0:
			value = keysLastTinyGet(v, s.key, true, cutoff)
		case 1:
			value = perfGetString(v, s.text, true)
		case 2:
			value = perfAt(v, int(s.key))
		}
		if i+1 < len(path) {
			raw = value.Bytes()
		}
	}
	return value
}
func TestTinyGet(t *testing.T) {
	for _, source := range workloadFixtures(t) {
		var old lengthWriter
		back := lengthEncode(&old, nil, source, func(int, Value) bool { return true })
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		for _, leaf := range perfCollect(source, back) {
			for _, cutoff := range []int{1, 2, 4} {
				if !equalLengthValue(keysLastTinyPath(raw, leaf.path, cutoff), leaf.value) {
					t.Fatal("leaf")
				}
			}
		}
	}
}

func TestTinyGetBoundaries(t *testing.T) {
	for n := 0; n <= 6; n++ {
		var w keysLastWriter
		w.Begin(0, "", 0, true)
		for i := 0; i < n; i++ {
			// Include a present zero and a span near the one-byte key limit.
			value := lengthWord(uint64(i))
			if i == 2 {
				value = lengthBytes([]byte("x"))
			}
			keysLastPut(&w, uint64(250+i), "", value)
		}
		w.End()
		v := keysLastTrusted(w.BytesWritten())
		for _, key := range []uint64{0, 249, 250, 251, 252, 253, 254, 255, 256, 506, ^uint64(0)} {
			want := keysLastPortableGet(v.lengthView, key)
			for _, cutoff := range []int{1, 2, 4} {
				if got := keysLastTinyGet(v.lengthView, key, true, cutoff); !equalLengthValue(got, want) {
					t.Fatalf("count=%d key=%d cutoff=%d", n, key, cutoff)
				}
			}
		}
	}
}
func BenchmarkTinyGet(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy", "local128", "global128", "nested"} {
		var r perfRow
		rows := 1024
		numeric := name == "local128" || name == "global128" || name == "nested"
		if numeric {
			rows = 32768
			r.source = mmapFixture(name)
			var old lengthWriter
			r.raw = lengthEncode(&old, nil, r.source, func(int, Value) bool { return true })
			r.leaves = perfCollect(r.source, r.raw)
		} else {
			r = rowFixtures(b)[name]
		}
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, r.source, func(int, Value) bool { return true })
		for _, mode := range []string{"binary", "byte", "tiny1", "tiny2", "tiny4", "immutable"} {
			if !numeric && mode == "immutable" {
				continue
			}
			b.Run(fmt.Sprintf("%s/%s", name, mode), func(b *testing.B) {
				dataRow := raw
				if mode == "immutable" {
					dataRow = mmapEncode(r.source, "immutable")
				}
				cutoff := 0
				if mode == "tiny1" {
					cutoff = 1
				} else if mode == "tiny2" {
					cutoff = 2
				} else if mode == "tiny4" {
					cutoff = 4
				}
				d := newMmapDataset(b, dataRow, rows)
				warmMmapPayloads(d)
				b.ReportAllocs()
				b.ResetTimer()
				rng := uint64(0x123abcde99887766)
				if numeric {
					rng = 0xdedabcde12345678
				}
				var sum uint64
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					data := d.rows[int(rng>>16)&(rows-1)]
					if numeric {
						if mode == "immutable" {
							sum += mmapRead(data, "immutable", name, rng)
							continue
						}
						key := rng&127 + 1
						if name == "global128" {
							key += 1 << 50
						}
						if name == "nested" {
							key = rng&7 + 1
						}
						var v lengthValue
						if mode == "binary" {
							v = keysLastGet(lengthTrusted(data), key, true)
						} else if mode == "byte" {
							v = keysLastByteGet(lengthTrusted(data), key, true)
						} else {
							v = keysLastTinyGet(lengthTrusted(data), key, true, cutoff)
						}
						if name == "nested" {
							key = rng>>3&15 + 1
							if mode == "binary" {
								v = keysLastGet(v.Object(), key, true)
							} else if mode == "byte" {
								v = keysLastByteGet(v.Object(), key, true)
							} else {
								v = keysLastTinyGet(v.Object(), key, true, cutoff)
							}
						}
						sum += v.Uint()
						continue
					}
					path := r.leaves[int(rng%uint64(len(r.leaves)))].path
					var v lengthValue
					if mode == "binary" {
						v = keysLastPath(data, path)
					} else if mode == "byte" {
						v = keysLastBytePath(data, path)
					} else {
						v = keysLastTinyPath(data, path, cutoff)
					}
					sum += v.num
				}
				b.StopTimer()
				sinkU = sum
				b.ReportMetric(float64(len(dataRow)), "row-B")
			})
		}
	}
}
