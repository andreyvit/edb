package kvolab

// One front discriminator, with no added byte. Fast moves its five-byte header
// to the front. Compact moves only the low byte of its reversed footer there;
// any remaining high footer bytes stay at the end. Payloads remain forward-only.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math"
	"runtime"
	"testing"
	"unsafe"
)

type unifiedWriter struct{ lengthWriter }

func prepareUnifiedPacked(v unifiedView) fastPacked {
	if !v.Indexed() {
		panic(draft.ErrKind)
	}
	raw := v.Raw()
	t := raw[0]
	n := int(binary.LittleEndian.Uint32(raw[1:]))
	width := 1 << uint(t>>6)
	mode := int(t >> 3 & 3)
	kind := int(t >> 1 & 3)
	keys := len(raw) - n*width
	if kind == 0 {
		keys -= n * 8
	}
	if mode == 2 {
		keys -= (n + 7) / 8
	}
	if n >= 1<<21 || keys >= 1<<27 {
		panic(draft.ErrLimit)
	}
	return fastPacked{v.ptr, uint64(n) | uint64(keys)<<21 | uint64(t>>6)<<48 | uint64(mode)<<50 | uint64(kind)<<52 | 1<<54}
}

func unifiedClassIter(raw []byte) classIterator {
	t, b := unifiedTail(raw)
	size := t >> 3
	if t&1 != 0 || t>>1&3 == 3 || size > uint64(len(b)) {
		panic(errTag)
	}
	payload, meta := b[:len(b)-int(size)], b[len(b)-int(size):]
	h, meta := tagReadU(meta)
	if h>>4 > tagMaxEntries || h&15 > 9 && h&15 != 15 {
		panic(errTag)
	}
	return classIterator{meta: meta, payload: payload, kind: t >> 1 & 3, shared: draft.Wire(h & 15), left: int(h >> 4), rawLengths: true}
}
func unifiedPath(raw []byte, path []perfStep) lengthValue {
	var value lengthValue
	for i, s := range path {
		v := unifiedTrusted(raw)
		switch s.kind {
		case 0:
			value = v.Get(s.key)
		case 1:
			value = v.GetString(s.text)
		case 2:
			value = v.At(int(s.key))
		}
		if i+1 < len(path) {
			raw = value.Bytes()
		}
	}
	return value
}
func TestUnifiedFront(t *testing.T) {
	all := fixtures()
	for n, v := range workloadFixtures(t) {
		all[n] = v
	}
	all["extreme"] = Obj(Field{0, U(1)}, Field{math.MaxUint64, U(math.MaxUint64)})
	policies := []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }}
	for name, source := range all {
		for policyIndex, policy := range policies {
			var old lengthWriter
			back := lengthEncode(&old, nil, source, policy)
			var w unifiedWriter
			raw := unifiedEncode(&w, nil, source, policy)
			if policyIndex == 0 && len(back) != len(raw) {
				t.Fatal("Compact size changed")
			}
			var walk func([]byte, Value)
			walk = func(b []byte, x Value) {
				view, err := unifiedOpen(b)
				if err != nil {
					t.Fatal(name, policyIndex, err)
				}
				if view.Indexed() {
					p := prepareUnifiedPacked(view)
					if !bytes.Equal(p.Raw(), b) {
						t.Fatal("packed extent")
					}
					if x.Kind == Record {
						for _, f := range x.Fields {
							if p.Get(f.Key) != view.Get(f.Key) {
								t.Fatal("packed value")
							}
						}
					}
				}
				var rewrite unifiedWriter
				rewrite.Begin(0, "", view.Kind(), view.Indexed())
				it := view.Iter()
				count := 0
				for {
					k, text, v, ok := it.Next()
					if !ok {
						break
					}
					batchPut(&rewrite.lengthWriter, k, string(text), v)
					count++
				}
				rewrite.End()
				if !bytes.Equal(b, rewrite.BytesWritten()) {
					t.Fatal("opaque rewrite")
				}
				if count != view.Len() {
					t.Fatal("count")
				}
				check := func(k uint64, text string, child Value) {
					if x.Kind != List && tagZero(child) {
						return
					}
					var v lengthValue
					switch x.Kind {
					case Record:
						v = view.Get(k)
					case Dict:
						v = view.GetString(text)
					case List:
						v = view.At(int(k))
					}
					if !v.IsZero() && (child.Kind == Record || child.Kind == Dict || child.Kind == List) {
						walk(v.Bytes(), child)
					}
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
					for i, e := range x.Items {
						check(uint64(i), "", e)
					}
				}
			}
			walk(raw, source)
			for _, leaf := range perfCollect(source, back) {
				if !equalLengthValue(unifiedPath(raw, leaf.path), leaf.value) {
					t.Fatal(name, policyIndex, "leaf")
				}
			}
		}
	}
	// Presence is physical entry retention, independent of ordinary zero omission.
	for _, fast := range []bool{false, true} {
		var w unifiedWriter
		w.Begin(0, "", 0, fast)
		batchPut(&w.lengthWriter, 7, "", lengthWord(0))
		w.End()
		v, err := unifiedOpen(w.BytesWritten())
		if err != nil || v.Len() != 1 || !v.Get(7).IsZero() {
			t.Fatal("present zero")
		}
	}
}
func FuzzUnifiedFront(f *testing.F) {
	for _, p := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
		var w unifiedWriter
		f.Add(unifiedEncode(&w, nil, fixtures()["state"], p))
	}
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = unifiedOpen(b) })
}
func BenchmarkUnifiedRows(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, layout := range []string{"fast", "compact", "mixed"} {
			policy := func(d int, _ Value) bool { return layout == "fast" || layout == "mixed" && d == 0 }
			for _, codec := range []string{"back", "unified"} {
				var w lengthWriter
				var u unifiedWriter
				var out []byte
				encode := func() {
					if codec == "back" {
						out = lengthEncode(&w, out[:0], r.source, policy)
					} else {
						out = unifiedEncode(&u, out[:0], r.source, policy)
					}
				}
				encode()
				b.Run(fmt.Sprintf("%s/%s/%s/encode", name, layout, codec), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						encode()
					}
					sinkBytes = out
					b.ReportMetric(float64(len(out)), "encoded-B")
				})
				b.Run(fmt.Sprintf("%s/%s/%s/read1024", name, layout, codec), func(b *testing.B) {
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
						if codec == "back" {
							v = perfPath(lengthTrusted(raw), path, "equal")
						} else {
							v = unifiedPath(raw, path)
						}
						sum += v.num
					}
					sinkU = sum
				})
			}
		}
	}
}
func (w *unifiedWriter) Begin(key uint64, text string, kind int, indexed bool) {
	if w.depth > 0 && !w.frames[w.depth-1].indexed {
		indexed = false
	}
	w.lengthWriter.Begin(key, text, kind, indexed)
	n := 1
	if indexed {
		n = 5
	}
	w.out = append(w.out, make([]byte, n)...)
}

type unifiedView lengthView

func unifiedTrusted(b []byte) unifiedView { return unifiedView(lengthTrusted(b)) }
func (v unifiedView) Raw() []byte         { return lengthView(v).Raw() }
func (v unifiedView) Kind() int {
	if v.n == 0 {
		return 0
	}
	return int(v.Raw()[0] >> 1 & 3)
}
func (v unifiedView) Indexed() bool { return v.n > 0 && v.Raw()[0]&1 != 0 }
func (v unifiedView) Len() int {
	if v.n == 0 {
		return 0
	}
	if v.Indexed() {
		return int(binary.LittleEndian.Uint32(v.Raw()[1:]))
	}
	_, n, _, _, _ := unifiedHeader(v.Raw())
	return n
}
func unifiedTail(raw []byte) (uint64, []byte) {
	if len(raw) == 0 {
		panic(errTag)
	}
	first := raw[0]
	u := uint64(first & 127)
	b := raw[1:]
	if first < 128 {
		return u, b
	}
	for i := 1; i < 10 && len(b) > 0; i++ {
		x := b[len(b)-1]
		b = b[:len(b)-1]
		if i == 9 && x > 1 {
			panic(errTag)
		}
		u |= uint64(x&127) << uint(i*7)
		if x < 128 {
			if x == 0 {
				panic(errTag)
			}
			return u, b
		}
	}
	panic(errTag)
}
func unifiedHeader(raw []byte) (kind, n int, shared byte, payload, meta []byte) {
	t, b := unifiedTail(raw)
	kind = int(t >> 1 & 3)
	at := len(b) - int(t>>3)
	payload, meta = b[:at], b[at:]
	h, meta := lengthU(meta)
	return kind, int(h >> 4), byte(h & 15), payload, meta
}
func unifiedIndex(raw []byte) lengthIndexHeader {
	t := raw[0]
	h := lengthIndexHeader{raw: raw, kind: int(t >> 1 & 3), n: int(binary.LittleEndian.Uint32(raw[1:])), width: 1 << uint(t>>6), mode: t >> 3 & 3, cells: len(raw)}
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
func (w *unifiedWriter) End() {
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
		w.out[f.start] = w.out[len(w.out)-1]
		copy(w.out[f.start+1:f.start+5], w.out[len(w.out)-5:len(w.out)-1])
		w.out = w.out[:len(w.out)-5]
	} else {
		w.out = lengthAppendCompact(w.out, *f)
		w.out[f.start] = w.out[len(w.out)-1]
		w.out = w.out[:len(w.out)-1]
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
func unifiedEncode(w *unifiedWriter, dst []byte, v Value, policy func(depth int, v Value) bool) []byte {
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
func (v unifiedView) Get(target uint64) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if v.Kind() != 0 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		return unifiedFastGet(lengthView(v), target, true)
	}
	_, n, shared, payload, meta := unifiedHeader(v.Raw())
	var key uint64
	pos := 0
	if shared <= 8 {
		// Uniform scalar records do not need to read any value lengths.
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*int(shared), int(shared), shared)
			}
			if key > target {
				break
			}
		}
		return lengthValue{}
	}
	for i := 0; i < n; i++ {
		code := shared
		var d uint64
		if shared == 15 {
			d, code, meta = lengthMixed(meta)
		} else {
			d, meta = lengthU(meta)
		}
		key += d
		size := int(code)
		if code == 9 {
			u, r := lengthU(meta)
			size = int(u)
			meta = r
		}
		if key == target {
			return lengthCell(payload, pos, size, code)
		}
		if key > target {
			break
		}
		pos += size
	}
	return lengthValue{}
}
func (v unifiedView) GetString(target string) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if v.Kind() != 1 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		return unifiedFastGetString(lengthView(v), target, true)
	}
	_, n, shared, payload, meta := unifiedHeader(v.Raw())
	pos := 0
	for i := 0; i < n; i++ {
		u, r := lengthU(meta)
		text := r[:int(u)]
		meta = r[int(u):]
		cmp := bytes.Compare(text, tagStringBytes(target))
		if cmp > 0 {
			break
		}
		code := shared
		if shared == 15 {
			code, meta = meta[0], meta[1:]
		}
		size := int(code)
		if code == 9 {
			u, meta = lengthU(meta)
			size = int(u)
		}
		if cmp == 0 {
			return lengthCell(payload, pos, size, code)
		}
		pos += size
	}
	return lengthValue{}
}
func (v unifiedView) At(index int) lengthValue {
	if v.n == 0 {
		panic(draft.ErrIndex)
	}
	if v.Kind() != 2 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		return unifiedFastAt(lengthView(v), index)
	}
	_, n, shared, payload, meta := unifiedHeader(v.Raw())
	if index < 0 || index >= n {
		panic(draft.ErrIndex)
	}
	if shared <= 8 {
		return lengthCell(payload, index*int(shared), int(shared), shared)
	}
	pos := 0
	for i := 0; ; i++ {
		code := shared
		if shared == 15 {
			code, meta = meta[0], meta[1:]
		}
		size := int(code)
		if code == 9 {
			u, r := lengthU(meta)
			size = int(u)
			meta = r
		}
		if i == index {
			return lengthCell(payload, pos, size, code)
		}
		pos += size
	}
}

func unifiedFastGet(v lengthView, key uint64, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		panic("native key loads unavailable")
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[0]
	if t>>1&3 != 0 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		panic("front Fast container required")
	}
	n := int(binary.LittleEndian.Uint32(raw[1:]))
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

func unifiedFastGetString(v lengthView, target string, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		panic("front Fast Dictionary required")
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[0]
	if t>>1&3 != 1 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		panic("front Fast Dictionary required")
	}
	n := int(binary.LittleEndian.Uint32(raw[1:]))
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
func unifiedFastAt(v lengthView, index int) lengthValue {
	if v.n == 0 {
		panic(draft.ErrIndex)
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[0]
	if t>>1&3 != 2 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		panic("front Fast List required")
	}
	n := int(binary.LittleEndian.Uint32(raw[1:]))
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

type unifiedIterator struct {
	view                              unifiedView
	indexed                           lengthIndexHeader
	key                               uint64
	i, n, pos, kind, metaPos, metaEnd int
	shared                            byte
}

func (v unifiedView) Iter() unifiedIterator {
	it := unifiedIterator{view: v}
	if v.n == 0 {
		return it
	}
	if v.Indexed() {
		it.indexed = unifiedIndex(v.Raw())
		it.n = it.indexed.n
	} else {
		tail, b := unifiedTail(v.Raw())
		start := len(b) - int(tail>>3)
		h, meta := lengthU(b[start:])
		it.n = int(h >> 4)
		it.shared = byte(h & 15)
		it.kind = int(tail >> 1 & 3)
		it.metaEnd = len(b) + 1
		it.pos = 1
		it.metaPos = len(b) + 1 - len(meta)
	}
	return it
}
func (it *unifiedIterator) Next() (key uint64, text []byte, value lengthValue, ok bool) {
	if it.i == it.n {
		return
	}
	i := it.i
	it.i++
	if it.view.Indexed() {
		h := &it.indexed
		if h.kind == 0 {
			key = h.number(i)
		} else if h.kind == 1 {
			text = h.text(i)
		} else {
			key = uint64(i)
		}
		return key, text, h.value(i), true
	}
	raw := it.view.Raw()
	meta := raw[it.metaPos:it.metaEnd]
	code := it.shared
	if it.kind == 0 {
		var d uint64
		if code == 15 {
			d, code, meta = lengthMixed(meta)
		} else {
			d, meta = lengthU(meta)
		}
		it.key += d
		key = it.key
	} else {
		if it.kind == 1 {
			u, r := lengthU(meta)
			text = r[:int(u)]
			meta = r[int(u):]
		} else {
			key = uint64(i)
		}
		if code == 15 {
			code, meta = meta[0], meta[1:]
		}
	}
	size := int(code)
	if code == 9 {
		u, r := lengthU(meta)
		size = int(u)
		meta = r
	}
	value = lengthCell(raw, it.pos, size, code)
	it.pos += size
	it.metaPos = it.metaEnd - len(meta)
	return key, text, value, true
}

func unifiedOpen(raw []byte) (view unifiedView, err error) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(runtime.Error); p != errTag && !ok {
				panic(p)
			}
			view = unifiedView{}
			err = draft.ErrFormat
		}
	}()
	if len(raw) == 0 {
		return unifiedView{}, nil
	}
	if len(raw) > tagMaxBytes {
		return view, draft.ErrFormat
	}
	if raw[0]&1 == 0 {
		it := unifiedClassIter(raw)
		var prev []byte
		for {
			_, text, _, ok := it.next()
			if !ok {
				break
			}
			if it.kind == 1 && it.index > 1 && bytes.Compare(prev, text) >= 0 {
				panic(errTag)
			}
			prev = text
		}
	} else {
		if len(raw) < 5 {
			panic(errTag)
		}
		t := raw[0]
		if t&32 != 0 || t>>1&3 == 3 || t>>3&3 == 3 {
			panic(errTag)
		}
		h := unifiedIndex(raw)
		if h.n > tagMaxEntries || h.keys < 5 {
			panic(errTag)
		}
		if h.mode == 2 && h.n%8 != 0 && raw[h.cells-1]>>uint(h.n%8) != 0 {
			panic(errTag)
		}
		pos := 5
		var prevNum uint64
		var prevText []byte
		for i := 0; i < h.n; i++ {
			if h.kind == 0 {
				k := h.number(i)
				if i > 0 && k <= prevNum {
					panic(errTag)
				}
				prevNum = k
				if h.span(i) {
					u := binary.LittleEndian.Uint64(raw[h.cells+i*8:])
					at, n := int(uint32(u)), int(u>>32)
					if at != pos || n > h.keys-pos {
						panic(errTag)
					}
					pos += n
				}
			} else {
				start, end := h.entry(i)
				if start != pos || end < start || end > h.keys {
					panic(errTag)
				}
				if h.kind == 1 {
					n, b := tagReadU(raw[start:end])
					if n > uint64(len(b)) {
						panic(errTag)
					}
					text := b[:int(n)]
					if i > 0 && bytes.Compare(prevText, text) >= 0 {
						panic(errTag)
					}
					prevText = text
					start = end - len(b) + int(n)
				}
				if !h.span(i) && end-start != 8 {
					panic(errTag)
				}
				pos = end
			}
		}
		if pos != h.keys {
			panic(errTag)
		}
	}
	return unifiedTrusted(raw), nil
}
