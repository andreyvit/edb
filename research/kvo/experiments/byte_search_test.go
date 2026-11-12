package kvolab

// Width-one directories can use the standard library's byte search. The
// bounds check before narrowing the target is essential for absent large keys.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"testing"
)

func keysLastByteGet(v lengthView, key uint64, equal bool) lengthValue {
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
			i = bytes.IndexByte(raw[keys:end], byte(key))
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
func keysLastBytePath(raw []byte, path []perfStep) lengthValue {
	var value lengthValue
	for i, s := range path {
		v := lengthTrusted(raw)
		switch s.kind {
		case 0:
			value = keysLastByteGet(v, s.key, true)
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
func BenchmarkByteSearchMmap(b *testing.B) {
	for _, fixture := range []string{"local128", "global128", "nested"} {
		source := mmapFixture(fixture)
		for _, mode := range []string{"back", "back-byte", "keys-last", "keys-byte", "immutable"} {
			b.Run(fixture+"/"+mode, func(b *testing.B) {
				var old lengthWriter
				var w keysLastWriter
				var u unifiedWriter
				policy := func(int, Value) bool { return true }
				var raw []byte
				switch mode {
				case "back", "back-byte":
					raw = lengthEncode(&old, nil, source, policy)
				case "keys-last", "keys-byte":
					raw = keysLastEncode(&w, nil, source, policy)
				case "unified", "unified-direct":
					raw = unifiedEncode(&u, nil, source, policy)
				case "immutable":
					raw = mmapEncode(source, mode)
				}
				d := newMmapDataset(b, raw, 32768)
				warmMmapPayloads(d)
				b.ReportAllocs()
				b.ResetTimer()
				rng := uint64(0xdedabcde12345678)
				var sum uint64
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					data := d.rows[int(rng>>16)&32767]
					if mode == "immutable" {
						sum += mmapRead(data, mode, fixture, rng)
						continue
					}
					key := rng&127 + 1
					if fixture == "global128" {
						key += 1 << 50
					}
					if fixture == "nested" {
						key = rng&7 + 1
					}
					var value lengthValue
					switch mode {
					case "back":
						value = perfGet(lengthTrusted(data), key, true)
					case "back-byte":
						value = backByteGet(lengthTrusted(data), key, true)
					case "keys-last":
						value = keysLastGet(lengthTrusted(data), key, true)
					case "keys-byte":
						value = keysLastByteGet(lengthTrusted(data), key, true)
					case "unified":
						value = unifiedTrusted(data).Get(key)
					case "unified-direct":
						value = unifiedDirectGet(lengthTrusted(data), key, true)
					}
					if fixture == "nested" {
						key = rng>>3&15 + 1
						switch mode {
						case "back":
							value = perfGet(value.Object(), key, true)
						case "back-byte":
							value = backByteGet(value.Object(), key, true)
						case "keys-last":
							value = keysLastGet(value.Object(), key, true)
						case "keys-byte":
							value = keysLastByteGet(value.Object(), key, true)
						case "unified":
							value = unifiedTrusted(value.Bytes()).Get(key)
						case "unified-direct":
							value = unifiedDirectGet(value.Object(), key, true)
						}
					}
					sum += value.Uint()
				}
				b.StopTimer()
				sinkU = sum
				b.ReportMetric(float64(len(raw)), "row-B")
			})
		}
	}
}
func BenchmarkByteSearchShape(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, mode := range []string{"back", "back-byte", "keys-last", "keys-byte"} {
			var old lengthWriter
			var u unifiedWriter
			var w keysLastWriter
			var out []byte
			encode := func() {
				if mode == "back" || mode == "back-byte" {
					out = lengthEncode(&old, out[:0], r.source, func(int, Value) bool { return true })
				} else if mode == "unified-direct" {
					out = unifiedEncode(&u, out[:0], r.source, func(int, Value) bool { return true })
				} else {
					out = keysLastEncode(&w, out[:0], r.source, func(int, Value) bool { return true })
				}
			}
			encode()
			b.Run(fmt.Sprintf("%s/%s/encode", name, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					encode()
				}
				sinkBytes = out
				b.ReportMetric(float64(len(out)), "encoded-B")
			})
			b.Run(fmt.Sprintf("%s/%s/read1024", name, mode), func(b *testing.B) {
				d := newMmapDataset(b, out, 1024)
				warmMmapPayloads(d)
				b.ReportAllocs()
				b.ResetTimer()
				rng := uint64(0x123abcde99887766)
				var sum uint64
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					raw := d.rows[int(rng>>16)&1023]
					path := r.leaves[int(rng%uint64(len(r.leaves)))].path
					var v lengthValue
					if mode == "back-byte" {
						v = backBytePath(raw, path)
					} else if mode == "back" {
						v = perfPath(lengthTrusted(raw), path, "equal")
					} else if mode == "unified-direct" {
						v = unifiedDirectPath(raw, path)
					} else if mode == "keys-byte" {
						v = keysLastBytePath(raw, path)
					} else {
						v = keysLastPath(raw, path)
					}
					sum += v.num
				}
				sinkU = sum
			})
		}
	}
}
func TestByteSearch(t *testing.T) {
	for _, source := range workloadFixtures(t) {
		var old lengthWriter
		back := lengthEncode(&old, nil, source, func(int, Value) bool { return true })
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		for _, leaf := range perfCollect(source, back) {
			if !equalLengthValue(backBytePath(back, leaf.path), leaf.value) {
				t.Fatal("back leaf")
			}
			if !equalLengthValue(keysLastBytePath(raw, leaf.path), leaf.value) {
				t.Fatal("leaf")
			}
		}
	}
	for n := 0; n <= 255; n++ {
		var source Value
		source.Kind = Record
		for i := 0; i < n; i++ {
			source.Fields = append(source.Fields, Field{uint64(i), U(uint64(i + 1))})
		}
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		for key := uint64(0); key < 512; key++ {
			if keysLastByteGet(lengthTrusted(raw), key, true) != keysLastGet(lengthTrusted(raw), key, true) {
				t.Fatal(n, key)
			}
		}
	}
}
func BenchmarkByteSearchKernel(b *testing.B) {
	for _, n := range []int{0, 1, 2, 4, 8, 16, 32, 64, 128, 255} {
		keys := make([]byte, n)
		for i := range keys {
			keys[i] = byte(i)
		}
		for _, misses := range []bool{false, true} {
			for _, mode := range []string{"binary", "byte"} {
				b.Run(fmt.Sprintf("n%d/miss%t/%s", n, misses, mode), func(b *testing.B) {
					b.ReportAllocs()
					var sum int
					rng := uint64(0x123456789abcde)
					for i := 0; i < b.N; i++ {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						key := uint64(255)
						if !misses && n > 0 {
							key = rng % uint64(n)
						}
						if mode == "byte" {
							sum += bytes.IndexByte(keys, byte(key))
						} else {
							sum += perfFind(keys, 1, key, true)
						}
					}
					sinkU = uint64(sum)
				})
			}
		}
	}
}

func backByteGet(v lengthView, key uint64, equal bool) lengthValue {
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
	i := -1
	if width == 1 {
		if key <= 255 {
			i = bytes.IndexByte(raw[keys:codes], byte(key))
		}
	} else {
		i = perfFind(raw[keys:codes], width, key, equal)
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
func backBytePath(raw []byte, path []perfStep) lengthValue {
	var value lengthValue
	for i, s := range path {
		v := lengthTrusted(raw)
		switch s.kind {
		case 0:
			value = backByteGet(v, s.key, true)
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
