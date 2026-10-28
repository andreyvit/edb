package kvolab

// Optional stored key base: fixed-width unsigned relative keys when they save
// more than the eight-byte base costs. The last footer byte uses reserved bit 5.
// This is a separately measured wire candidate, not the default representation.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/andreyvit/edb/kvo"
	"math"
	"math/bits"
	"slices"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type baseKeyWriter struct{ keysLastWriter }

func appendBaseKeyDirectory(dst []byte, f *lengthFrame) []byte {
	n := len(f.entries)
	width := 1
	if n > 0 {
		width = tagKeyWidth(f.entries[n-1].key)
	}
	base, extra := uint64(0), 0
	if f.kind == 0 && n > 0 {
		candidate := f.entries[0].key
		narrow := tagKeyWidth(f.entries[n-1].key - candidate)
		if n*(width-narrow) > 8 {
			base, extra, width = candidate, 8, narrow
		}
	}
	mode, bitmap := byte(0), 0
	if f.shared == 9 {
		mode = 1
	} else if f.shared == 15 && n > 0 {
		mode, bitmap = 2, (n+7)/8
	}
	size := n*width + bitmap + 5 + extra
	if f.kind == 0 {
		size += n * 8
	}
	start := len(dst)
	dst = slices.Grow(dst, size)
	dst = dst[:start+size]
	keys, codes := start, start+n*width
	if f.kind == 0 {
		codes = start + n*8
		keys = codes + bitmap
		for i, m := range f.entries {
			binary.LittleEndian.PutUint64(dst[start+i*8:], m.word)
		}
	}
	if bitmap > 0 {
		clear(dst[codes : codes+bitmap])
		for i, m := range f.entries {
			if m.wire == 9 {
				dst[codes+i/8] |= 1 << uint(i&7)
			}
		}
	}
	// Width is constant for the entire directory. Hoist its dispatch out of
	// the entry loop; explicit endian stores retain the portable baseline.
	switch width {
	case 1:
		for i, m := range f.entries {
			dst[keys+i] = byte(m.key - base)
		}
	case 2:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint16(dst[keys+i*2:], uint16(m.key-base))
		}
	case 4:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint32(dst[keys+i*4:], uint32(m.key-base))
		}
	case 8:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint64(dst[keys+i*8:], m.key-base)
		}
	}
	footer := start + size - 5
	if extra != 0 {
		binary.LittleEndian.PutUint64(dst[footer-8:], base)
	}
	binary.LittleEndian.PutUint32(dst[footer:], uint32(n))
	dst[footer+4] = 1 | byte(f.kind)<<1 | mode<<3 | byte(bits.Len(uint(width))-1)<<6
	if extra != 0 {
		dst[footer+4] |= 32
	}
	return dst
}

func (w *baseKeyWriter) End() {
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
		w.out = appendBaseKeyDirectory(w.out, f)
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
func baseKeyEncode(w *baseKeyWriter, dst []byte, v Value, policy func(depth int, v Value) bool) []byte {
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

func baseKeyGet(v lengthView, key uint64, equal bool, cutoff int) lengthValue {
	if v.n == 0 {
		return lengthValue{}
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
	if t&32 != 0 {
		end -= 8
		base := binary.LittleEndian.Uint64(raw[end:])
		if key < base {
			return lengthValue{}
		}
		key -= base
	}
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
		if perfNativeWords {
			i = perfFind(raw[keys:end], width, key, equal)
		} else {
			lo, hi := 0, n
			for lo < hi {
				m := lo + (hi-lo)/2
				k := getWidth(raw[keys+m*width:], width)
				if k == key {
					i = m
					break
				}
				if k < key {
					lo = m + 1
				} else {
					hi = m
				}
			}
		}
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
func baseKeyPath(raw []byte, path []perfStep, cutoff int) lengthValue {
	var value lengthValue
	for i, s := range path {
		v := lengthTrusted(raw)
		switch s.kind {
		case 0:
			value = baseKeyGet(v, s.key, true, cutoff)
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

func baseKeyFixture(name string) Value {
	if name == "local128" || name == "global128" || name == "nested" {
		return mmapFixture(name)
	}
	if name == "nested-global" {
		v := Obj()
		for i := 0; i < 8; i++ {
			v.Fields = append(v.Fields, Field{uint64(i+1) + (1 << 50), numeric(16, 1<<50)})
		}
		return v
	}
	v := numeric(128, 0)
	for i := range v.Fields {
		if name == "gapped" {
			v.Fields[i].Key = 1<<50 + uint64(i)*71
		} else {
			v.Fields[i].Key = uint64(i) * ((uint64(1) << 63) / 127)
		}
	}
	return v
}
func TestBaseKey(t *testing.T) {
	all := make(map[string]Value)
	for name, v := range workloadFixtures(t) {
		all[name] = v
	}
	for _, name := range []string{"local128", "global128", "nested", "nested-global", "gapped", "spread"} {
		all[name] = baseKeyFixture(name)
	}
	all["extreme"] = Obj(Field{0, U(1)}, Field{math.MaxUint64, U(2)})
	all["near-max"] = Obj(Field{math.MaxUint64 - 4, U(1)}, Field{math.MaxUint64 - 2, U(2)}, Field{math.MaxUint64, U(3)})
	for name, source := range all {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var old lengthWriter
			back := lengthEncode(&old, nil, source, policy)
			var w baseKeyWriter
			raw := baseKeyEncode(&w, nil, source, policy)
			for _, leaf := range perfCollect(source, back) {
				if !equalLengthValue(baseKeyPath(raw, leaf.path, 4), leaf.value) {
					t.Fatal(name, "value")
				}
			}
			if source.Kind == Record {
				for _, key := range []uint64{0, 1, 127, 255, 256, 1 << 49, 1 << 50, 1 << 63, math.MaxUint64} {
					if !equalLengthValue(baseKeyGet(lengthTrusted(raw), key, true, 4), lengthTrusted(back).Get(key)) {
						// Container bytes can differ recursively; compare those via leaf checks.
						a, b := baseKeyGet(lengthTrusted(raw), key, true, 4), lengthTrusted(back).Get(key)
						if a.ptr == nil || b.ptr == nil {
							t.Fatal(name, "miss or word", key)
						}
					}
				}
			}
		}
	}
	// Explicit present zeros and key truncation boundaries.
	var w baseKeyWriter
	w.Begin(0, "", 0, true)
	for i := uint64(0); i < 6; i++ {
		keysLastPut(&w.keysLastWriter, (1<<50)+i, "", lengthWord(i))
	}
	w.End()
	for i := uint64(0); i < 6; i++ {
		if baseKeyGet(lengthTrusted(w.out), (1<<50)+i, true, 4).Uint() != i {
			t.Fatal("present zero")
		}
	}
	for _, key := range []uint64{0, 1<<50 - 1, 1<<50 + 256, math.MaxUint64} {
		if !baseKeyGet(lengthTrusted(w.out), key, true, 4).IsZero() {
			t.Fatal("absent")
		}
	}
}
func BenchmarkBaseKey(b *testing.B) {
	for _, fixture := range []string{"local128", "global128", "gapped", "spread", "nested", "nested-global"} {
		source := baseKeyFixture(fixture)
		for _, rows := range []int{1, 32768} {
			for _, mode := range []string{"current", "base", "immutable"} {
				b.Run(fmt.Sprintf("%s/rows%d/%s", fixture, rows, mode), func(b *testing.B) {
					var old keysLastWriter
					var w baseKeyWriter
					var raw []byte
					policy := func(int, Value) bool { return true }
					switch mode {
					case "current":
						raw = keysLastEncode(&old, nil, source, policy)
					case "base":
						raw = baseKeyEncode(&w, nil, source, policy)
					case "immutable":
						raw = oldKVO(source).Bytes()
					}
					d := newMmapDataset(b, raw, rows)
					warmMmapPayloads(d)
					b.ReportAllocs()
					b.ResetTimer()
					rng := uint64(0xdedabcde12345678)
					var sum uint64
					nested := fixture == "nested" || fixture == "nested-global"
					for i := 0; i < b.N; i++ {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						data := d.rows[int(rng>>16)&(rows-1)]
						index := rng & 127
						if nested {
							index = rng & 7
						}
						field := source.Fields[index]
						if mode == "immutable" {
							v := kvo.LoadRecord(data, nil).Root()
							if nested {
								sum += v.GetMap(field.Key).Get(field.Value.Fields[rng>>3&15].Key)
							} else {
								sum += v.Get(field.Key)
							}
						} else {
							var value lengthValue
							if mode == "base" {
								value = baseKeyGet(lengthTrusted(data), field.Key, true, 4)
							} else {
								value = keysLastTrusted(data).Get(field.Key)
							}
							if nested {
								key := field.Value.Fields[rng>>3&15].Key
								if mode == "base" {
									value = baseKeyGet(value.Object(), key, true, 4)
								} else {
									value = keysLastTrusted(value.Bytes()).Get(key)
								}
							}
							sum += value.Uint()
						}
					}
					b.StopTimer()
					sinkU = sum
					b.ReportMetric(float64(len(raw)), "row-B")
				})
			}
		}
	}
}
func BenchmarkBaseKeyShape(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, mode := range []string{"current", "base"} {
			var old keysLastWriter
			var w baseKeyWriter
			var raw []byte
			encode := func() {
				if mode == "base" {
					raw = baseKeyEncode(&w, raw[:0], r.source, func(int, Value) bool { return true })
				} else {
					raw = keysLastEncode(&old, raw[:0], r.source, func(int, Value) bool { return true })
				}
			}
			encode()
			b.Run(name+"/"+mode+"/encode", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					encode()
				}
				sinkBytes = raw
				b.ReportMetric(float64(len(raw)), "row-B")
			})
			b.Run(name+"/"+mode+"/read1024", func(b *testing.B) {
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
					if mode == "base" {
						v = baseKeyPath(data, path, 4)
					} else {
						v = keysLastTinyPath(data, path, 4)
					}
					sum += v.num
				}
				b.StopTimer()
				sinkU = sum
				b.ReportMetric(float64(len(raw)), "row-B")
			})
		}
	}
}

func TestBaseKeyReplayStats(t *testing.T) {
	for _, layout := range []string{"fast", "mixed"} {
		previousBytes, baseBytes, objects, based := 0, 0, 0, 0
		var visit func(Value, []byte)
		visit = func(v Value, raw []byte) {
			if len(raw) == 0 {
				return
			}
			if v.Kind == Record {
				objects++
				if raw[len(raw)-1]&33 == 33 {
					based++
				}
			}
			child := func(value Value, data lengthValue) {
				if value.Kind == Record || value.Kind == Dict || value.Kind == List {
					visit(value, data.Bytes())
				}
			}
			switch v.Kind {
			case Record:
				for _, f := range v.Fields {
					if !tagZero(f.Value) {
						child(f.Value, baseKeyGet(lengthTrusted(raw), f.Key, true, 4))
					}
				}
			case Dict:
				for _, e := range v.Entries {
					if !tagZero(e.Value) {
						child(e.Value, perfGetString(lengthTrusted(raw), e.Key, true))
					}
				}
			case List:
				for i, x := range v.Items {
					child(x, perfAt(lengthTrusted(raw), i))
				}
			}
		}
		for _, source := range workloadFixtures(t) {
			var a keysLastWriter
			var b baseKeyWriter
			policy := func(depth int, _ Value) bool { return layout == "fast" || depth == 0 }
			old := keysLastEncode(&a, nil, source, policy)
			raw := baseKeyEncode(&b, nil, source, policy)
			previousBytes += len(old)
			baseBytes += len(raw)
			visit(source, raw)
		}
		t.Logf("%s old=%d base=%d objects=%d based=%d", layout, previousBytes, baseBytes, objects, based)
	}
}
