package kvolab

// Validating/iterating counterpart of the adjacent-key placement control.
import (
	"bytes"
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"testing"
)

type keysLastView struct{ lengthView }

func keysLastTrusted(raw []byte) keysLastView { return keysLastView{lengthTrusted(raw)} }
func (v keysLastView) Get(key uint64) lengthValue {
	return keysLastTinyGet(v.lengthView, key, true, 4)
}

// GetByte retains byte search without the tiny-directory comparison loop.
func (v keysLastView) GetByte(key uint64) lengthValue {
	return keysLastByteGet(v.lengthView, key, true)
}

// GetBinary retains the previous entry point for implementation comparisons.
func (v keysLastView) GetBinary(key uint64) lengthValue { return keysLastGet(v.lengthView, key, true) }
func (v keysLastView) GetString(key string) lengthValue {
	return dictFusedGet(v.lengthView, key)
}
func (v keysLastView) At(i int) lengthValue { return listScalarAt(v.lengthView, i) }
func keysLastIndexPrevious(raw []byte) lengthIndexHeader {
	h := lengthIndex(raw)
	if h.kind == 0 {
		h.keys = len(raw) - 5 - h.n*h.width
		h.codes = h.keys
		if h.mode == 2 {
			h.codes -= (h.n + 7) / 8
		}
		h.cells = h.codes - h.n*8
	}
	return h
}
func keysLastIndex(raw []byte) lengthIndexHeader {
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	h := lengthIndexHeader{raw: raw, kind: int(t >> 1 & 3), n: n, width: 1 << uint(t>>6), mode: t >> 3 & 3}
	end := len(raw) - 5
	if h.kind == 0 {
		h.keys = end - n*h.width
		h.codes = h.keys
		if h.mode == 2 {
			h.codes -= (n + 7) / 8
		}
		h.cells = h.codes - n*8
	} else {
		h.cells = end
		h.codes = end
		if h.mode == 2 {
			h.codes -= (n + 7) / 8
		}
		h.keys = h.codes - n*h.width
	}
	return h
}
func keysLastOpen(raw []byte) (view keysLastView, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errTag {
				panic(p)
			}
			view = keysLastView{}
			err = draft.ErrFormat
		}
	}()
	if len(raw) == 0 {
		return keysLastView{}, nil
	}
	if len(raw) > tagMaxBytes {
		return view, draft.ErrFormat
	}
	if raw[len(raw)-1]&1 == 0 {
		it := classIter(raw, true)
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
		t := raw[len(raw)-1]
		if t&32 != 0 || t>>1&3 == 3 || t>>3&3 == 3 {
			panic(errTag)
		}
		h := keysLastIndex(raw)
		if h.n > tagMaxEntries || h.keys < 0 || h.cells < 0 || h.codes < 0 {
			panic(errTag)
		}
		if h.mode == 2 && h.n%8 != 0 && raw[h.codes+(h.n+7)/8-1]>>uint(h.n%8) != 0 {
			panic(errTag)
		}
		limit := h.keys
		if h.kind == 0 {
			limit = h.cells
		}
		pos := 0
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
					if at != pos || n > limit-pos {
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
		if pos != limit {
			panic(errTag)
		}
	}
	return keysLastTrusted(raw), nil
}

type keysLastIterator struct {
	view                              keysLastView
	indexed                           lengthIndexHeader
	key                               uint64
	i, n, pos, kind, metaPos, metaEnd int
	shared                            byte
}

func (v keysLastView) Iter() keysLastIterator {
	it := keysLastIterator{view: v}
	if v.n == 0 {
		return it
	}
	if v.Indexed() {
		it.indexed = keysLastIndex(v.Raw())
		it.n = it.indexed.n
	} else {
		tail, b := tagTail(v.Raw())
		start := len(b) - int(tail>>3)
		h, meta := lengthU(b[start:])
		it.n = int(h >> 4)
		it.shared = byte(h & 15)
		it.kind = int(tail >> 1 & 3)
		it.metaEnd = len(b)
		it.metaPos = len(b) - len(meta)
	}
	return it
}
func (it *keysLastIterator) Next() (key uint64, text []byte, value lengthValue, ok bool) {
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

func TestKeysLastViews(t *testing.T) {
	all := fixtures()
	for k, v := range workloadFixtures(t) {
		all[k] = v
	}
	for name, source := range all {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var w keysLastWriter
			raw := keysLastEncode(&w, nil, source, policy)
			var walk func([]byte, Value)
			walk = func(b []byte, x Value) {
				view, err := keysLastOpen(b)
				if err != nil {
					t.Fatal(name, err)
				}
				var rw keysLastWriter
				rw.Begin(0, "", view.Kind(), view.Indexed())
				it := view.Iter()
				n := 0
				for {
					k, s, v, ok := it.Next()
					if !ok {
						break
					}
					batchPut(&rw.lengthWriter, k, string(s), v)
					n++
				}
				rw.End()
				if !bytes.Equal(b, rw.BytesWritten()) || n != view.Len() {
					t.Fatal(name, "opaque rewrite/count")
				}
				check := func(k uint64, s string, child Value) {
					if x.Kind != List && tagZero(child) {
						return
					}
					var v lengthValue
					switch x.Kind {
					case Record:
						v = view.Get(k)
					case Dict:
						v = view.GetString(s)
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
		}
	}
	for _, fast := range []bool{false, true} {
		var w keysLastWriter
		w.Begin(0, "", 0, fast)
		batchPut(&w.lengthWriter, 7, "", lengthWord(0))
		w.End()
		v, err := keysLastOpen(w.BytesWritten())
		if err != nil || v.Len() != 1 || !v.Get(7).IsZero() {
			t.Fatal("presence")
		}
	}
}
func FuzzKeysLast(f *testing.F) {
	for _, p := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
		var w keysLastWriter
		f.Add(keysLastEncode(&w, nil, fixtures()["state"], p))
	}
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = keysLastOpen(b) })
}

func (v keysLastView) Patch(key uint64, text string, word uint64) bool {
	if !v.Indexed() {
		return false
	}
	h := keysLastIndex(v.Raw())
	if word == 0 && h.kind != 2 {
		return false
	}
	i := -1
	switch h.kind {
	case 0:
		if perfNativeWords {
			i = perfFind(h.raw[h.keys:h.keys+h.n*h.width], h.width, key, true)
		} else {
			i = keysLastSafeFind(&h, key)
		}
	case 1:
		i = h.findString(text)
	case 2:
		if key < uint64(h.n) {
			i = int(key)
		}
	}
	if i < 0 || h.span(i) {
		return false
	}
	at := h.cells + i*8
	if h.kind != 0 {
		start, end := h.entry(i)
		at = start
		if h.kind == 1 {
			n, b := lengthU(h.raw[start:end])
			at = end - len(b) + int(n)
		}
	}
	binary.LittleEndian.PutUint64(h.raw[at:], word)
	return true
}
