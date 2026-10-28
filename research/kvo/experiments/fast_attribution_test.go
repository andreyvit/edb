package kvolab

// Controlled ablations on unchanged Fast bytes. The front-header and forced-key
// width cases are separate diagnostic layouts, not selected production formats.
import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
)

func attrLower[T ~uint8 | ~uint16 | ~uint32 | ~uint64](keys []T, key uint64) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		m := lo + (hi-lo)/2
		if uint64(keys[m]) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == len(keys) || uint64(keys[lo]) != key {
		return -1
	}
	return lo
}
func attrEqual[T ~uint8 | ~uint16 | ~uint32 | ~uint64](keys []T, key uint64) int {
	lo, hi := 0, len(keys)
	for lo < hi {
		m := lo + (hi-lo)/2
		k := uint64(keys[m])
		if k < key {
			lo = m + 1
		} else if k > key {
			hi = m
		} else {
			return m
		}
	}
	return -1
}
func attrFind(h *lengthIndexHeader, key uint64, equal bool) int {
	if h.n == 0 {
		return -1
	}
	p := unsafe.Pointer(unsafe.SliceData(h.raw[h.keys:h.codes]))
	switch h.width {
	case 1:
		keys := unsafe.Slice((*uint8)(p), h.n)
		if equal {
			return attrEqual(keys, key)
		}
		return attrLower(keys, key)
	case 2:
		keys := unsafe.Slice((*uint16)(p), h.n)
		if equal {
			return attrEqual(keys, key)
		}
		return attrLower(keys, key)
	case 4:
		keys := unsafe.Slice((*uint32)(p), h.n)
		if equal {
			return attrEqual(keys, key)
		}
		return attrLower(keys, key)
	case 8:
		keys := unsafe.Slice((*uint64)(p), h.n)
		if equal {
			return attrEqual(keys, key)
		}
		return attrLower(keys, key)
	}
	panic("width")
}
func attrScalar(h *lengthIndexHeader, i int) uint64 {
	if i < 0 {
		return 0
	}
	return binary.LittleEndian.Uint64(h.raw[h.cells+i*8:])
}
func attrGet(raw []byte, key uint64, mode string) uint64 {
	if mode == "common" {
		return lengthTrusted(raw).Get(key).Uint()
	}
	if mode == "immutable" {
		return kvo.LoadRecord(raw, nil).Root().Get(key)
	}
	h := lengthIndex(raw)
	switch mode {
	case "layout-known":
		return h.value(h.findNumber(key)).Uint()
	case "scalar-known":
		return attrScalar(&h, h.findNumber(key))
	case "typed-keys":
		return h.value(attrFind(&h, key, false)).Uint()
	case "typed-scalar":
		return attrScalar(&h, attrFind(&h, key, false))
	case "equal-search":
		return h.value(attrFind(&h, key, true)).Uint()
	case "sort-search":
		i := sort.Search(h.n, func(i int) bool { return h.number(i) >= key })
		if i == h.n || h.number(i) != key {
			return 0
		}
		return h.value(i).Uint()
	}
	panic(mode)
}
func attrFront(raw []byte) []byte {
	out := make([]byte, len(raw))
	copy(out, raw[len(raw)-5:])
	copy(out[5:], raw[:len(raw)-5])
	return out
}
func attrFrontGet(raw []byte, key uint64) uint64 {
	n := int(binary.LittleEndian.Uint32(raw))
	t := raw[4]
	h := lengthIndexHeader{raw: raw, n: n, width: 1 << uint(t>>6), cells: len(raw) - n*8, mode: 0}
	h.codes = h.cells
	h.keys = 5
	return h.value(attrFind(&h, key, false)).Uint()
}
func attrForcedWidth(raw []byte, width int) []byte {
	h := lengthIndex(raw)
	if h.kind != 0 || h.mode != 0 || h.keys != 0 {
		panic("scalar-only control")
	}
	out := make([]byte, 0, h.n*(width+8)+5)
	for i := 0; i < h.n; i++ {
		out = tagAppendWord(out, h.number(i), width)
	}
	out = append(out, raw[h.cells:len(raw)-5]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(h.n))
	exp := byte(0)
	for 1<<exp < width {
		exp++
	}
	return append(out, 1|exp<<6)
}
func TestFastAttribution(t *testing.T) {
	modes := []string{"common", "layout-known", "scalar-known", "typed-keys", "typed-scalar", "equal-search", "sort-search"}
	for _, n := range []int{0, 1, 2, 8, 128} {
		for _, base := range []uint64{0, 256, 65536, 1 << 50} {
			source := numeric(n, base)
			var w lengthWriter
			raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
			for _, data := range [][]byte{raw, attrForcedWidth(raw, 8)} {
				for _, offset := range []int{0, 1, 7} {
					owned := append(make([]byte, offset), data...)
					b := owned[offset:]
					for _, key := range []uint64{0, base, base + 1, base + uint64(n/2), base + uint64(n), base + uint64(n+1), math.MaxUint64} {
						want := lengthTrusted(b).Get(key).Uint()
						for _, mode := range modes {
							if got := attrGet(b, key, mode); got != want {
								t.Fatalf("%s n%d base%d key%d got%d want%d", mode, n, base, key, got, want)
							}
						}
						if got := attrFrontGet(attrFront(b), key); got != want {
							t.Fatal("front")
						}
					}
				}
			}
		}
	}
	// Typed key access remains valid for mixed Objects; only scalar-known variants
	// intentionally omit the span policy. Check borrowed spans and misses too.
	var w lengthWriter
	v := Obj(Field{1, U(9)}, Field{2, S("opaque")}, Field{65536, U(17)})
	raw := lengthEncode(&w, nil, v, func(int, Value) bool { return true })
	h := lengthIndex(raw)
	for _, key := range []uint64{0, 1, 2, 3, 65536, 65537} {
		i := attrFind(&h, key, false)
		a, b := h.value(i), lengthTrusted(raw).Get(key)
		if a.num != b.num || a.ptr != b.ptr {
			t.Fatal("mixed")
		}
	}
}
func BenchmarkFastAttribution(b *testing.B) {
	for _, fixture := range []string{"local8", "local128", "global128"} {
		n, base := 128, uint64(0)
		if fixture == "local8" {
			n = 8
		}
		if fixture == "global128" {
			base = 1 << 50
		}
		source := numeric(n, base)
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		for _, rows := range []int{1, 32768} {
			b.Run(fmt.Sprintf("%s/rows%d", fixture, rows), func(b *testing.B) {
				d := newMmapDataset(b, raw, rows)
				warmMmapPayloads(d)
				headers := make([]lengthIndexHeader, rows)
				for i, r := range d.rows {
					headers[i] = lengthIndex(r)
				}
				for _, mode := range []string{"common", "layout-known", "scalar-known", "typed-keys", "typed-scalar", "equal-search", "sort-search", "parsed-header"} {
					b.Run(mode, func(b *testing.B) {
						b.ReportAllocs()
						var sum uint64
						rng := uint64(0xdedabcde12345678)
						for i := 0; i < b.N; i++ {
							rng ^= rng << 13
							rng ^= rng >> 7
							rng ^= rng << 17
							row := int(rng>>16) & (rows - 1)
							key := base + (rng & uint64(n-1)) + 1
							if mode == "parsed-header" {
								h := &headers[row]
								sum += h.value(h.findNumber(key)).Uint()
							} else {
								sum += attrGet(d.rows[row], key, mode)
							}
						}
						sinkU = sum
						b.ReportMetric(float64(len(raw)), "row-B")
						b.ReportMetric(float64(d.fileBytes), "file-B")
					})
				}
				b.Run("immutable", func(b *testing.B) {
					data := oldKVO(source).Bytes()
					d := newMmapDataset(b, data, rows)
					warmMmapPayloads(d)
					b.ReportAllocs()
					b.ResetTimer()
					var sum uint64
					rng := uint64(0xdedabcde12345678)
					for i := 0; i < b.N; i++ {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						row := int(rng>>16) & (rows - 1)
						key := base + (rng & uint64(n-1)) + 1
						sum += attrGet(d.rows[row], key, "immutable")
					}
					sinkU = sum
					b.ReportMetric(float64(len(data)), "row-B")
					b.ReportMetric(float64(d.fileBytes), "file-B")
				})
			})
		}
	}
}
func BenchmarkFastPlacement(b *testing.B) {
	source := numeric(128, 0)
	var w lengthWriter
	raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
	wide := attrForcedWidth(raw, 8)
	for _, rows := range []int{1, 32768} {
		for _, mode := range []string{"narrow", "wide", "narrow-padded", "narrow-front", "wide-front"} {
			b.Run(fmt.Sprintf("rows%d/%s", rows, mode), func(b *testing.B) {
				data := raw
				switch mode {
				case "wide":
					data = wide
				case "narrow-padded":
					data = append(make([]byte, len(wide)-len(raw)), raw...)
				case "narrow-front":
					data = attrFront(raw)
				case "wide-front":
					data = attrFront(wide)
				}
				d := newMmapDataset(b, data, rows)
				warmMmapPayloads(d)
				if mode == "narrow-padded" {
					for i := range d.rows {
						d.rows[i] = d.rows[i][len(wide)-len(raw):]
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				var sum uint64
				rng := uint64(0xdedabcde12345678)
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					row := int(rng>>16) & (rows - 1)
					key := (rng & 127) + 1
					if mode == "narrow-front" || mode == "wide-front" {
						sum += attrFrontGet(d.rows[row], key)
					} else {
						sum += attrGet(d.rows[row], key, "typed-keys")
					}
				}
				sinkU = sum
				b.ReportMetric(float64(len(data)), "stored-row-B")
				b.ReportMetric(float64(d.fileBytes), "file-B")
			})
		}
	}
}

func BenchmarkFastSearchKernel(b *testing.B) {
	for _, width := range []int{1, 2, 4, 8} {
		source := numeric(128, 0)
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		raw = attrForcedWidth(raw, width)
		h := lengthIndex(raw)
		for _, mode := range []string{"byte-lower", "typed-lower", "typed-equal"} {
			b.Run(fmt.Sprintf("width%d/%s", width, mode), func(b *testing.B) {
				var sum uint64
				rng := uint64(0xdedabcde12345678)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					key := rng&127 + 1
					var pos int
					switch mode {
					case "byte-lower":
						pos = h.findNumber(key)
					case "typed-lower":
						pos = attrFind(&h, key, false)
					default:
						pos = attrFind(&h, key, true)
					}
					sum += uint64(pos)
				}
				sinkU = sum
			})
		}
	}
}

// Bucket.Get locates the value without necessarily touching every overflow page.
// Touch one byte per page plus the last byte before timing page-cache-warm reads.
func warmMmapPayloads(d *mmapDataset) {
	var sum uint64
	for _, raw := range d.rows {
		for at := 0; at < len(raw); at += 4096 {
			sum += uint64(raw[at])
		}
		if len(raw) != 0 {
			sum += uint64(raw[len(raw)-1])
		}
	}
	sinkU = sum
}
