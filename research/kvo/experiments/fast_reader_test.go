package kvolab

// Fast access with the existing bytes and 16-byte borrowed view. Parse only the
// selected container, avoid the 80-byte generic header and specialize key loads.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

const perfNativeWords = runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64"

func perfFind(keys []byte, width int, key uint64, equal bool) int {
	if len(keys) == 0 {
		return -1
	}
	p := unsafe.Pointer(unsafe.SliceData(keys))
	switch width {
	case 1:
		a := keys
		if equal {
			return attrEqual(a, key)
		}
		return attrLower(a, key)
	case 2:
		a := unsafe.Slice((*uint16)(p), len(keys)/2)
		if equal {
			return attrEqual(a, key)
		}
		return attrLower(a, key)
	case 4:
		a := unsafe.Slice((*uint32)(p), len(keys)/4)
		if equal {
			return attrEqual(a, key)
		}
		return attrLower(a, key)
	case 8:
		a := unsafe.Slice((*uint64)(p), len(keys)/8)
		if equal {
			return attrEqual(a, key)
		}
		return attrLower(a, key)
	}
	panic(draft.ErrFormat)
}
func perfGet(v lengthView, key uint64, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		return v.Get(key)
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
	cells := end - n*8
	codes := cells
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	i := perfFind(raw[keys:codes], width, key, equal)
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
func perfDictFind[T ~uint8 | ~uint16 | ~uint32 | ~uint64](raw []byte, offsets []T, target string, equal bool) int {
	lo, hi := 0, len(offsets)
	for lo < hi {
		m := lo + (hi-lo)/2
		n, rest := lengthU(raw[int(offsets[m]):])
		c := bytes.Compare(rest[:int(n)], tagStringBytes(target))
		if c < 0 {
			lo = m + 1
		} else if equal && c == 0 {
			return m
		} else {
			hi = m
		}
	}
	if lo == len(offsets) {
		return -1
	}
	n, rest := lengthU(raw[int(offsets[lo]):])
	if !bytes.Equal(rest[:int(n)], tagStringBytes(target)) {
		return -1
	}
	return lo
}
func perfGetString(v lengthView, target string, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		return v.GetString(target)
	}
	raw := v.Raw()
	end := len(raw) - 5
	t := raw[len(raw)-1]
	if t>>1&3 != 1 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return v.GetString(target)
	}
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	if n == 0 {
		return lengthValue{}
	}
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	codes := end
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	p := unsafe.Pointer(unsafe.SliceData(raw[keys:codes]))
	i := -1
	switch width {
	case 1:
		i = perfDictFind(raw, unsafe.Slice((*uint8)(p), n), target, equal)
	case 2:
		i = perfDictFind(raw, unsafe.Slice((*uint16)(p), n), target, equal)
	case 4:
		i = perfDictFind(raw, unsafe.Slice((*uint32)(p), n), target, equal)
	case 8:
		i = perfDictFind(raw, unsafe.Slice((*uint64)(p), n), target, equal)
	}
	if i < 0 {
		return lengthValue{}
	}
	at := int(getWidth(raw[keys+i*width:], width))
	size, rest := lengthU(raw[at:])
	start := len(raw) - len(rest) + int(size)
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return lengthWord(binary.LittleEndian.Uint64(raw[start:]))
	}
	finish := keys
	if i+1 < n {
		finish = int(getWidth(raw[keys+(i+1)*width:], width))
	}
	return lengthBytes(raw[start:finish])
}
func perfAt(v lengthView, index int) lengthValue {
	if v.n == 0 {
		panic(draft.ErrIndex)
	}
	raw := v.Raw()
	end := len(raw) - 5
	t := raw[len(raw)-1]
	if t>>1&3 != 2 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return v.At(index)
	}
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	if index < 0 || index >= n {
		panic(draft.ErrIndex)
	}
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	codes := end
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	start := int(getWidth(raw[keys+index*width:], width))
	if mode == 0 || mode == 2 && raw[codes+index/8]&(1<<uint(index&7)) == 0 {
		return lengthWord(binary.LittleEndian.Uint64(raw[start:]))
	}
	finish := keys
	if index+1 < n {
		finish = int(getWidth(raw[keys+(index+1)*width:], width))
	}
	return lengthBytes(raw[start:finish])
}

// Matched header-location controls: both general parsers retain identical kind /
// mode handling; both simple parsers assume the scalar-only diagnostic fixture.
func attrFrontGeneral(raw []byte) lengthIndexHeader {
	t := raw[4]
	n := int(binary.LittleEndian.Uint32(raw))
	h := lengthIndexHeader{raw: raw, kind: int(t >> 1 & 3), n: n, width: 1 << uint(t>>6), mode: t >> 3 & 3}
	h.cells = len(raw)
	if h.kind == 0 {
		h.cells -= n * 8
	}
	h.codes = h.cells
	if h.mode == 2 {
		h.codes -= (n + 7) / 8
	}
	h.keys = h.codes - n*h.width
	return h
}
func attrBackSimpleGet(raw []byte, key uint64) uint64 {
	end := len(raw) - 5
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	t := raw[len(raw)-1]
	h := lengthIndexHeader{raw: raw, n: n, width: 1 << uint(t>>6), cells: end - n*8, mode: 0}
	h.codes = h.cells
	return h.value(attrFind(&h, key, false)).Uint()
}
func TestPerfReader(t *testing.T) {
	all := fixtures()
	for n, v := range workloadFixtures(t) {
		all[n] = v
	}
	for n, v := range selectorListFixtures() {
		all[n] = v
	}
	for _, source := range all {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var w lengthWriter
			raw := lengthEncode(&w, nil, source, policy)
			var walk func(Value, []byte)
			walk = func(x Value, data []byte) {
				v := lengthTrusted(data)
				listIndex := 0
				check := func(key uint64, text string, child Value) {
					if x.Kind != List && tagZero(child) {
						return
					}
					var want lengthValue
					switch x.Kind {
					case Record:
						want = v.Get(key)
					case Dict:
						want = v.GetString(text)
					case List:
						want = v.At(listIndex)
					}
					for _, equal := range []bool{false, true} {
						var got lengthValue
						switch x.Kind {
						case Record:
							got = perfGet(v, key, equal)
						case Dict:
							got = perfGetString(v, text, equal)
						case List:
							got = perfAt(v, listIndex)
						}
						if got.ptr != want.ptr || got.num != want.num {
							t.Fatal("reader mismatch")
						}
					}
					if !tagZero(child) && (child.Kind == Record || child.Kind == Dict || child.Kind == List) {
						walk(child, want.Bytes())
					}
					listIndex++
				}
				switch x.Kind {
				case Record:
					for _, f := range x.Fields {
						check(f.Key, "", f.Value)
					}
				case Dict:
					for _, e := range x.Entries {
						check(0, e.Key, e.Value)
					}
				case List:
					for _, e := range x.Items {
						check(0, "", e)
					}
				}
			}
			walk(source, raw)
		}
	}
	for _, widthBase := range []uint64{0, 256, 65536, 1 << 50} {
		source := numeric(128, widthBase)
		for i := range source.Fields {
			source.Fields[i].Key = widthBase + uint64(i+1)*2
		}
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		for _, offset := range []int{0, 1, 7} {
			b := append(make([]byte, offset), raw...)[offset:]
			v := lengthTrusted(b)
			for key := widthBase; key < widthBase+260; key++ {
				for _, equal := range []bool{false, true} {
					if perfGet(v, key, equal).Uint() != v.Get(key).Uint() {
						t.Fatal("miss/unaligned")
					}
				}
			}
		}
		front := attrFront(raw)
		for _, f := range source.Fields {
			h := attrFrontGeneral(front)
			if h.value(attrFind(&h, f.Key, false)).Uint() != f.Value.U || attrBackSimpleGet(raw, f.Key) != f.Value.U {
				t.Fatal("header control")
			}
		}
	}
}
func BenchmarkFastReaders(b *testing.B) {
	for _, fixture := range []string{"local128", "global128", "local8"} {
		n, base := 128, uint64(0)
		if fixture == "local8" {
			n = 8
		}
		if fixture == "global128" {
			base = 1 << 50
		}
		source := numeric(n, base)
		for i := range source.Fields {
			source.Fields[i].Key = base + uint64(i+1)*2
		}
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		for _, rows := range []int{1, 32768} {
			b.Run(fmt.Sprintf("%s/rows%d", fixture, rows), func(b *testing.B) {
				d := newMmapDataset(b, raw, rows)
				warmMmapPayloads(d)
				for _, miss := range []int{0, 50, 100} {
					for _, mode := range []string{"common", "fused-lower", "fused-equal"} {
						b.Run(fmt.Sprintf("miss%d/%s", miss, mode), func(b *testing.B) {
							b.ReportAllocs()
							rng := uint64(0xdedabcde12345678)
							var sum uint64
							for i := 0; i < b.N; i++ {
								rng ^= rng << 13
								rng ^= rng >> 7
								rng ^= rng << 17
								key := base + (rng&uint64(n-1)+1)*2
								if miss == 100 || miss == 50 && rng&256 != 0 {
									key++
								}
								v := lengthTrusted(d.rows[int(rng>>16)&(rows-1)])
								if mode == "common" {
									sum += v.Get(key).Uint()
								} else {
									sum += perfGet(v, key, mode == "fused-equal").Uint()
								}
							}
							sinkU = sum
						})
					}
				}
			})
		}
	}
}
func BenchmarkFastHeaderControls(b *testing.B) {
	source := numeric(128, 0)
	var w lengthWriter
	raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
	for _, rows := range []int{1, 32768} {
		for _, mode := range []string{"back-general", "back-simple", "front-general", "front-simple", "fused-back"} {
			b.Run(fmt.Sprintf("rows%d/%s", rows, mode), func(b *testing.B) {
				data := raw
				if mode == "front-general" || mode == "front-simple" {
					data = attrFront(raw)
				}
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
					r := d.rows[int(rng>>16)&(rows-1)]
					key := rng&127 + 1
					switch mode {
					case "back-general":
						sum += attrGet(r, key, "typed-keys")
					case "back-simple":
						sum += attrBackSimpleGet(r, key)
					case "front-general":
						h := attrFrontGeneral(r)
						sum += h.value(attrFind(&h, key, false)).Uint()
					case "front-simple":
						sum += attrFrontGet(r, key)
					case "fused-back":
						sum += perfGet(lengthTrusted(r), key, false).Uint()
					}
				}
				sinkU = sum
			})
		}
	}
}
