package kvolab

// A complete all-Fast front-header control. The writer reserves five bytes at
// Begin and fills them at End; payloads and children still go straight to the
// final destination. Same framing bytes; shifted offsets can cross a width boundary. Compact-child detection
// is intentionally not solved here: this codec is selected for an all-Fast row.
import (
	"bytes"
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math"
	"testing"
	"unsafe"
)

type frontWriter struct{ lengthWriter }

func (w *frontWriter) Begin(key uint64, text string, kind int, indexed bool) {
	if !indexed {
		panic("all-Fast control")
	}
	w.lengthWriter.Begin(key, text, kind, true)
	w.out = append(w.out, 0, 0, 0, 0, 0)
}

func frontHeader(raw []byte) lengthIndexHeader {
	if len(raw) == 0 {
		return lengthIndexHeader{}
	}
	t := raw[4]
	h := lengthIndexHeader{raw: raw, kind: int(t >> 1 & 3), n: int(binary.LittleEndian.Uint32(raw)), width: 1 << uint(t>>6), mode: t >> 3 & 3, cells: len(raw)}
	if h.kind == 0 {
		h.cells -= h.n * 8
	}
	h.codes = h.cells
	if h.mode == 2 {
		h.codes -= (h.n + 7) / 8
	}
	h.keys = h.codes - h.n*h.width
	return h
}
func frontPath(raw []byte, path []perfStep) lengthValue {
	var v lengthValue
	for i, s := range path {
		if len(raw) == 0 {
			return lengthValue{}
		}
		if s.kind == 0 {
			v = frontGet(lengthTrusted(raw), s.key, true)
		} else {
			if s.kind == 1 {
				v = frontGetString(lengthTrusted(raw), s.text, true)
			} else {
				v = frontAt(lengthTrusted(raw), int(s.key))
			}
		}
		if i+1 < len(path) {
			raw = v.Bytes()
		}
	}
	return v
}
func TestFrontFast(t *testing.T) {
	var delta, changed int
	for _, source := range workloadFixtures(t) {
		var w lengthWriter
		var f frontWriter
		back := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		front := frontEncode(&f, nil, source)
		if len(back) != len(front) {
			delta += len(front) - len(back)
			changed++
		}
		for _, l := range perfCollect(source, back) {
			v := frontPath(front, l.path)
			if l.value.ptr == nil {
				if v != l.value {
					t.Fatal("word")
				}
			} else if !bytes.Equal(v.Bytes(), l.value.Bytes()) {
				t.Fatal("span")
			}
		}
	}
	t.Logf("front-header offset growth: %d bytes across %d roots", delta, changed)
}
func BenchmarkFrontShape(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, mode := range []string{"back", "front"} {
			var w lengthWriter
			var fw frontWriter
			var out []byte
			encode := func() {
				if mode == "front" {
					out = frontEncode(&fw, out[:0], r.source)
				} else {
					out = lengthEncode(&w, out[:0], r.source, func(int, Value) bool { return true })
				}
			}
			encode()
			b.Run(name+"/"+mode+"/encode", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					encode()
				}
				sinkBytes = out
				b.ReportMetric(float64(len(out)), "encoded-B")
			})
			b.Run(name+"/"+mode+"/read1024", func(b *testing.B) {
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
					if mode == "front" {
						v = frontPath(raw, path)
					} else {
						v = perfPath(lengthTrusted(raw), path, "equal")
					}
					sum += v.num
				}
				sinkU = sum
			})
		}
	}
}
func (w *frontWriter) End() {
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
		w.out = lengthAppendIndex(w.out, *f)
		copy(w.out[f.start:f.start+5], w.out[len(w.out)-5:])
		w.out = w.out[:len(w.out)-5]
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
func frontEncode(w *frontWriter, dst []byte, v Value) []byte {
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
			indexed := true
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
func frontGet(v lengthView, key uint64, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		panic("native key loads unavailable")
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[4]
	if t>>1&3 != 0 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		panic("front Fast container required")
	}
	n := int(binary.LittleEndian.Uint32(raw))
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

func frontGetString(v lengthView, target string, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		panic("front Fast Dictionary required")
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[4]
	if t>>1&3 != 1 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		panic("front Fast Dictionary required")
	}
	n := int(binary.LittleEndian.Uint32(raw))
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
func frontAt(v lengthView, index int) lengthValue {
	if v.n == 0 {
		panic(draft.ErrIndex)
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[4]
	if t>>1&3 != 2 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		panic("front Fast List required")
	}
	n := int(binary.LittleEndian.Uint32(raw))
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
