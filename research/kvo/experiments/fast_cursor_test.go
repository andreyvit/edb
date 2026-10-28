package kvolab

// Optional prepared Object handle: two GC-visible pointers and a packed count /
// width / mode word. It borrows the same transaction; it never owns mmap memory.
// This is an explicit repeated-access API, not an allocation hidden in Get.
import (
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"
)

type fastCursor struct {
	base, keys *byte
	info       uint64
}

func prepareFast(v lengthView) fastCursor {
	raw := v.Raw()
	if !v.Indexed() || v.Kind() != 0 {
		panic("Fast Object required")
	}
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	keys := len(raw) - 5 - n*(width+8)
	if mode == 2 {
		keys -= (n + 7) / 8
	}
	return fastCursor{v.ptr, (*byte)(unsafe.Add(unsafe.Pointer(v.ptr), keys)), uint64(n) | uint64(t>>6)<<32 | uint64(mode)<<34}
}
func (c fastCursor) Get(key uint64) lengthValue {
	n := int(uint32(c.info))
	width := 1 << uint(c.info>>32&3)
	mode := byte(c.info >> 34 & 3)
	i := perfFind(unsafe.Slice(c.keys, n*width), width, key, false)
	if i < 0 {
		return lengthValue{}
	}
	cells := n * width
	if mode == 2 {
		cells += (n + 7) / 8
	}
	word := binary.LittleEndian.Uint64(unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(c.keys), cells+i*8)), 8))
	if mode == 0 || mode == 2 && *(*byte)(unsafe.Add(unsafe.Pointer(c.keys), n*width+i/8))&(1<<uint(i&7)) == 0 {
		return lengthWord(word)
	}
	return lengthValue{(*byte)(unsafe.Add(unsafe.Pointer(c.base), int(uint32(word)))), word >> 32}
}
func TestFastCursor(t *testing.T) {
	if unsafe.Sizeof(fastCursor{}) != 24 {
		t.Fatal("cursor footprint")
	}
	for _, c := range batchFixtures(t) {
		if c.source.Kind != Record {
			continue
		}
		view := lengthTrusted(c.raw)
		cursor := prepareFast(view)
		for _, f := range c.source.Fields {
			if a, b := cursor.Get(f.Key), view.Get(f.Key); a != b {
				t.Fatal("cursor value")
			}
		}
		if !cursor.Get(^uint64(0)).IsZero() {
			t.Fatal("cursor miss")
		}
	}
}
func BenchmarkFastCursor(b *testing.B) {
	for _, fixture := range []string{"local128", "global128"} {
		source := mmapFixture(fixture)
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		for _, rows := range []int{1, 32768} {
			b.Run(fmt.Sprintf("%s/rows%d", fixture, rows), func(b *testing.B) {
				d := newMmapDataset(b, raw, rows)
				warmMmapPayloads(d)
				packed := make([]fastPacked, rows)
				cached := make([]fastCursor, rows)
				for i, r := range d.rows {
					cached[i] = prepareFast(lengthTrusted(r))
					packed[i] = preparePacked(lengthTrusted(r))
				}
				for _, reads := range []int{1, 8, 128} {
					for _, mode := range []string{"common", "fused", "prepared24", "cached24", "prepared16", "cached16"} {
						b.Run(fmt.Sprintf("reads%d/%s", reads, mode), func(b *testing.B) {
							b.ReportAllocs()
							rng := uint64(0xdeadfedababc0123)
							var sum uint64
							for i := 0; i < b.N; i++ {
								rng ^= rng << 13
								rng ^= rng >> 7
								rng ^= rng << 17
								row := int(rng>>16) & (rows - 1)
								v := lengthTrusted(d.rows[row])
								var c fastCursor
								var p fastPacked
								if mode == "prepared16" {
									p = preparePacked(v)
								} else if mode == "cached16" {
									p = packed[row]
								}
								if mode == "prepared24" {
									c = prepareFast(v)
								} else if mode == "cached24" {
									c = cached[row]
								}
								for j := 0; j < reads; j++ {
									key := source.Fields[(int(rng)+j)&127].Key
									switch mode {
									case "common":
										sum += v.Get(key).Uint()
									case "fused":
										sum += perfGet(v, key, false).Uint()
									case "prepared16", "cached16":
										sum += p.Get(key).Uint()
									default:
										sum += c.Get(key).Uint()
									}
								}
							}
							sinkU = sum
							b.ReportMetric(float64(reads), "reads/op")
							if mode == "cached16" {
								b.ReportMetric(float64(16*rows), "cache-B")
							}
							if mode == "cached24" {
								b.ReportMetric(float64(24*rows), "cache-B")
							}
						})
					}
				}
			})
		}
	}
}
