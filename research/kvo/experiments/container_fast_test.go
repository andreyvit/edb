package kvolab

// Return the payload offset from Dictionary search instead of decoding the
// matching key again. The List control uses the canonical all-scalar invariant
// (contiguous eight-byte payloads), while retaining the existing stored offsets.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math"
	"sort"
	"testing"
	"unsafe"
)

func dictLocate[T ~uint8 | ~uint16 | ~uint32 | ~uint64](raw []byte, offsets []T, target string) (int, int) {
	lo, hi := 0, len(offsets)
	for lo < hi {
		m := lo + (hi-lo)/2
		n, rest := lengthU(raw[int(offsets[m]):])
		c := bytes.Compare(rest[:int(n)], tagStringBytes(target))
		if c == 0 {
			return m, len(raw) - len(rest) + int(n)
		}
		if c < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return -1, 0
}
func dictFusedGet(v lengthView, target string) lengthValue {
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
	i, start := -1, 0
	switch width {
	case 1:
		i, start = dictLocate(raw, unsafe.Slice((*uint8)(p), n), target)
	case 2:
		i, start = dictLocate(raw, unsafe.Slice((*uint16)(p), n), target)
	case 4:
		i, start = dictLocate(raw, unsafe.Slice((*uint32)(p), n), target)
	case 8:
		i, start = dictLocate(raw, unsafe.Slice((*uint64)(p), n), target)
	}
	if i < 0 {
		return lengthValue{}
	}
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return lengthWord(binary.LittleEndian.Uint64(raw[start:]))
	}
	finish := keys
	if i+1 < n {
		finish = int(getWidth(raw[keys+(i+1)*width:], width))
	}
	return lengthBytes(raw[start:finish])
}
func listScalarAt(v lengthView, index int) lengthValue {
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
	if mode == 0 {
		return lengthWord(binary.LittleEndian.Uint64(raw[index*8:]))
	}
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

func containerFastPath(raw []byte, path []perfStep) lengthValue {
	var v lengthValue
	for i, s := range path {
		view := lengthTrusted(raw)
		switch s.kind {
		case 0:
			v = keysLastTinyGet(view, s.key, true, 4)
		case 1:
			v = dictFusedGet(view, s.text)
		case 2:
			v = listScalarAt(view, int(s.key))
		}
		if i+1 < len(path) {
			raw = v.Bytes()
		}
	}
	return v
}
func TestContainerFastPath(t *testing.T) {
	for name, source := range workloadFixtures(t) {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var old lengthWriter
			back := lengthEncode(&old, nil, source, policy)
			var w keysLastWriter
			raw := keysLastEncode(&w, nil, source, policy)
			for _, leaf := range perfCollect(source, back) {
				if !equalLengthValue(containerFastPath(raw, leaf.path), leaf.value) {
					t.Fatal(name, "value")
				}
			}
		}
	}
	for name, source := range containerReadFixtures(t) {
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		v := lengthTrusted(raw)
		if source.Kind == Dict {
			for _, key := range []string{"", "\x00", "zzzzzzzzzzzzzzzz", source.Entries[0].Key + "\x00"} {
				if !equalLengthValue(dictFusedGet(v, key), perfGetString(v, key, true)) {
					t.Fatal(name, "miss")
				}
			}
		}
	}
}

// Representative container sizes selected from the replay, not uniform random
// payloads. Canonical path read tests above cover every replay container.
func containerReadFixtures(tb testing.TB) map[string]Value {
	result := map[string]Value{}
	scores := map[string]float64{}
	names := make([]string, 0, len(workloadFixtures(tb)))
	for name := range workloadFixtures(tb) {
		names = append(names, name)
	}
	sort.Strings(names)
	var walk func(Value)
	walk = func(v Value) {
		if v.Kind == Dict || v.Kind == List {
			n := len(v.Items)
			kind := "list"
			if v.Kind == Dict {
				n = len(v.Entries)
				kind = "dict"
			}
			if n > 0 {
				for _, target := range []int{4, 16, 128} {
					name := fmt.Sprintf("%s%d", kind, target)
					score := math.Abs(float64(n - target))
					previous, ok := scores[name]
					if !ok || score < previous {
						result[name] = v
						scores[name] = score
					}
				}
			}
		}
		for _, f := range v.Fields {
			walk(f.Value)
		}
		for _, e := range v.Entries {
			walk(e.Value)
		}
		for _, x := range v.Items {
			walk(x)
		}
	}
	for _, name := range names {
		walk(workloadFixtures(tb)[name])
	}
	// Isolate the all-scalar shortcut with the exact numeric width distribution
	// of one replay List when available; avoid manufacturing its value mix.
	var scalar Value
	for _, name := range names {
		var visit func(Value)
		visit = func(v Value) {
			if v.Kind == List && len(v.Items) > len(scalar.Items) {
				all := true
				for _, x := range v.Items {
					if x.Kind == Record || x.Kind == Dict || x.Kind == List || x.Kind == Text || x.Kind == Blob {
						all = false
					}
				}
				if all {
					scalar = v
				}
			}
			for _, f := range v.Fields {
				visit(f.Value)
			}
			for _, e := range v.Entries {
				visit(e.Value)
			}
			for _, x := range v.Items {
				visit(x)
			}
		}
		visit(workloadFixtures(tb)[name])
	}
	if len(scalar.Items) > 0 {
		result["list-scalars"] = scalar
	}
	return result
}
func BenchmarkContainerFast(b *testing.B) {
	fixtures := containerReadFixtures(b)
	names := make([]string, 0, len(fixtures))
	for name := range fixtures {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		source := fixtures[name]
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		for _, rows := range []int{1, 1024} {
			for _, mode := range []string{"previous", "fused"} {
				b.Run(fmt.Sprintf("%s/rows%d/%s", name, rows, mode), func(b *testing.B) {
					d := newMmapDataset(b, raw, rows)
					warmMmapPayloads(d)
					b.ReportAllocs()
					b.ResetTimer()
					rng := uint64(0xdedabcde12345678)
					var sum uint64
					for i := 0; i < b.N; i++ {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						v := lengthTrusted(d.rows[int(rng>>16)&(rows-1)])
						var value lengthValue
						if source.Kind == Dict {
							key := source.Entries[int(rng%uint64(len(source.Entries)))].Key
							if mode == "previous" {
								value = perfGetString(v, key, true)
							} else {
								value = dictFusedGet(v, key)
							}
						} else {
							index := int(rng % uint64(len(source.Items)))
							if mode == "previous" {
								value = perfAt(v, index)
							} else {
								value = listScalarAt(v, index)
							}
						}
						sum += value.num
					}
					b.StopTimer()
					sinkU = sum
					b.ReportMetric(float64(len(raw)), "row-B")
				})
			}
		}
	}
}
func BenchmarkContainerFastShape(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, r.source, func(int, Value) bool { return true })
		for _, mode := range []string{"previous", "fused"} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				d := newMmapDataset(b, raw, 1024)
				warmMmapPayloads(d)
				b.ReportAllocs()
				b.ResetTimer()
				rng := uint64(0xdedabcde12345678)
				var sum uint64
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					data := d.rows[int(rng>>16)&1023]
					path := r.leaves[int(rng%uint64(len(r.leaves)))].path
					var v lengthValue
					if mode == "previous" {
						v = keysLastTinyPath(data, path, 4)
					} else {
						v = containerFastPath(data, path)
					}
					sum += v.num
				}
				sinkU = sum
			})
		}
	}
}
