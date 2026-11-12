package kvolab

// Placement control: Object cells, bitmap and keys precede the existing footer,
// bringing the keys next to the count/descriptor. Dictionary/List and Compact
// bytes are unchanged. No payload movement, prepass or added byte is required.
import (
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math"
	"math/bits"
	"testing"
)

type keysLastWriter struct{ lengthWriter }

func (w *keysLastWriter) Begin(key uint64, text string, kind int, fast bool) {
	if w.depth > 0 && !w.frames[w.depth-1].indexed {
		fast = false
	}
	w.lengthWriter.Begin(key, text, kind, fast)
}
func keysLastAppendIndex(dst []byte, f lengthFrame) []byte {
	if f.kind != 0 {
		return lengthAppendIndex(dst, f)
	}
	width := 1
	if len(f.entries) > 0 {
		width = tagKeyWidth(f.entries[len(f.entries)-1].key)
	}
	for _, m := range f.entries {
		dst = binary.LittleEndian.AppendUint64(dst, m.word)
	}
	mode := byte(0)
	if f.shared == 9 {
		mode = 1
	} else if f.shared == 15 && len(f.entries) > 0 {
		mode = 2
		at := len(dst)
		dst = append(dst, make([]byte, (len(f.entries)+7)/8)...)
		for i, m := range f.entries {
			if m.wire == 9 {
				dst[at+i/8] |= 1 << uint(i%8)
			}
		}
	}
	for _, m := range f.entries {
		dst = tagAppendWord(dst, m.key, width)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(f.entries)))
	return append(dst, 1|byte(f.kind)<<1|mode<<3|byte(bits.Len(uint(width))-1)<<6)
}
func (w *keysLastWriter) End() {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) == 0 && w.depth > 1 {
		key, rollback := f.parentKey, f.rollback
		w.out = w.out[:rollback]
		w.depth--
		if w.frames[w.depth-1].kind == 2 {
			if w.frames[w.depth-1].indexed {
				w.out = binary.LittleEndian.AppendUint64(w.out, 0)
			}
			w.add(key, 0, 0)
		}
		return
	}
	if f.indexed {
		w.out = appendBulkDirectory(w.out, f)
	} else {
		w.out = lengthAppendCompact(w.out, *f)
	}
	n := len(w.out) - f.start
	if n > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	key, start := f.parentKey, f.start
	w.depth--
	if w.depth > 0 {
		w.add(key, 9, uint64(start-w.frames[w.depth-1].start)|uint64(n)<<32)
	}
}
func keysLastEncode(w *keysLastWriter, dst []byte, v Value, policy func(depth int, v Value) bool) []byte {
	w.Reset(dst)
	var walk func(uint64, string, Value, int)
	walk = func(key uint64, text string, x Value, depth int) {
		switch x.Kind {
		case Record, Dict, List:
			kind := 0
			if x.Kind == Dict {
				kind = 1
			} else if x.Kind == List {
				kind = 2
			}
			indexed := policy != nil && policy(depth, x)
			w.Begin(key, text, kind, indexed)
			switch x.Kind {
			case Record:
				w.ReserveEntries(len(x.Fields))
				for _, f := range x.Fields {
					walk(f.Key, "", f.Value, depth+1)
				}
			case Dict:
				w.ReserveEntries(len(x.Entries))
				for _, p := range x.Entries {
					walk(0, p.Key, p.Value, depth+1)
				}
			case List:
				w.ReserveEntries(len(x.Items))
				for _, v := range x.Items {
					walk(0, "", v, depth+1)
				}
			}
			w.End()
		default:
			word := x.U
			switch x.Kind {
			case Null, False:
				word = 0
			case True:
				word = 1
			case Int:
				i := int64(x.U)
				word = uint64(i<<1) ^ uint64(i>>63)
			case Float:
				if math.Float64frombits(x.U) == 0 {
					word = 0
				}
			case Text:
				w.Put(key, text, lengthBytes(tagStringBytes(x.S)))
				return
			case Blob:
				w.Put(key, text, lengthBytes(x.B))
				return
			}
			w.Put(key, text, lengthWord(word))
		}
	}
	walk(0, "", v, 0)
	return w.BytesWritten()
}
func keysLastGet(v lengthView, key uint64, equal bool) lengthValue {
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
	i := perfFind(raw[keys:end], width, key, equal)
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
func keysLastPath(raw []byte, path []perfStep) lengthValue {
	var value lengthValue
	for i, s := range path {
		v := lengthTrusted(raw)
		switch s.kind {
		case 0:
			value = keysLastGet(v, s.key, true)
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
func TestKeysLast(t *testing.T) {
	all := fixtures()
	for k, v := range workloadFixtures(t) {
		all[k] = v
	}
	all["extreme"] = Obj(Field{0, U(1)}, Field{math.MaxUint64, U(math.MaxUint64)})
	for name, source := range all {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var old lengthWriter
			back := lengthEncode(&old, nil, source, policy)
			var w keysLastWriter
			raw := keysLastEncode(&w, nil, source, policy)
			if len(back) != len(raw) {
				t.Fatal(name, "size changed")
			}
			for _, leaf := range perfCollect(source, back) {
				if !equalLengthValue(keysLastPath(raw, leaf.path), leaf.value) {
					t.Fatal(name, "leaf")
				}
			}
		}
	}
}
func BenchmarkKeysLast(b *testing.B) {
	for _, fixture := range []string{"local128", "global128", "nested"} {
		source := mmapFixture(fixture)
		for _, mode := range []string{"back", "keys-last", "unified", "unified-direct", "immutable"} {
			b.Run(fixture+"/"+mode, func(b *testing.B) {
				var old lengthWriter
				var w keysLastWriter
				var u unifiedWriter
				policy := func(int, Value) bool { return true }
				var raw []byte
				switch mode {
				case "back":
					raw = lengthEncode(&old, nil, source, policy)
				case "keys-last":
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
					case "keys-last":
						value = keysLastGet(lengthTrusted(data), key, true)
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
						case "keys-last":
							value = keysLastGet(value.Object(), key, true)
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
func BenchmarkKeysLastShape(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, mode := range []string{"back", "keys-last", "unified-direct"} {
			var old lengthWriter
			var u unifiedWriter
			var w keysLastWriter
			var out []byte
			encode := func() {
				if mode == "back" {
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
					if mode == "back" {
						v = perfPath(lengthTrusted(raw), path, "equal")
					} else if mode == "unified-direct" {
						v = unifiedDirectPath(raw, path)
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
