package kvolab

// Common 16-byte views for the selected catalog experiment. Compact bytes are
// unchanged. Indexed bytes use only a scalar/span bit instead of wire classes.
// This is an executable research reference, not the production KVO API.

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type lengthValue struct {
	ptr *byte
	num uint64
}

func lengthWord(n uint64) lengthValue { return lengthValue{num: n} }
func lengthBytes(b []byte) lengthValue {
	if len(b) == 0 {
		return lengthValue{}
	}
	return lengthValue{unsafe.SliceData(b), uint64(len(b))}
}
func (v lengthValue) Uint() uint64 {
	if v.ptr != nil {
		panic(draft.ErrType)
	}
	return v.num
}
func (v lengthValue) Bytes() []byte {
	if v.ptr == nil && v.num != 0 {
		panic(draft.ErrType)
	}
	return unsafe.Slice(v.ptr, int(v.num))
}
func (v lengthValue) IsZero() bool       { return v.ptr == nil && v.num == 0 }
func (v lengthValue) Object() lengthView { return lengthTrusted(v.Bytes()) }

type lengthView struct {
	ptr *byte
	n   int
}

func lengthTrusted(b []byte) lengthView { return lengthView{unsafe.SliceData(b), len(b)} }
func (v lengthView) Raw() []byte        { return unsafe.Slice(v.ptr, v.n) }

// Fast path is intentionally small enough to inline. All public trusted access
// is preceded by outer-container validation; arbitrary spans are not opened.
func lengthU(b []byte) (uint64, []byte) {
	if b[0] < 128 {
		return uint64(b[0]), b[1:]
	}
	return tagReadU(b)
}
func lengthHeader(raw []byte) (kind int, n int, shared byte, payload, meta []byte) {
	var t uint64
	b := raw[:len(raw)-1]
	if raw[len(raw)-1] < 128 {
		t = uint64(raw[len(raw)-1])
	} else {
		t, b = tagTail(raw)
	}
	kind = int(t >> 1 & 3)
	at := len(b) - int(t>>3)
	payload, meta = b[:at], b[at:]
	h, rest := lengthU(meta)
	return kind, int(h >> 4), byte(h & 15), payload, rest
}
func (v lengthView) Kind() int {
	if v.n == 0 {
		return 0
	}
	return int(v.Raw()[v.n-1] >> 1 & 3)
}
func (v lengthView) Indexed() bool { return v.n > 0 && v.Raw()[v.n-1]&1 != 0 }
func (v lengthView) Len() int {
	if v.n == 0 {
		return 0
	}
	if v.Indexed() {
		return int(binary.LittleEndian.Uint32(v.Raw()[v.n-5:]))
	}
	_, n, _, _, _ := lengthHeader(v.Raw())
	return n
}
func lengthCell(payload []byte, pos, n int, code byte) lengthValue {
	if code == 9 {
		return lengthBytes(payload[pos : pos+n])
	}
	return lengthWord(tagWord(payload[pos : pos+n]))
}
func lengthMixed(meta []byte) (delta uint64, code byte, rest []byte) {
	u, rest := lengthU(meta)
	code = byte(u & 15)
	if code != 15 {
		return u >> 4, code, rest
	}
	delta, rest = lengthU(rest)
	return delta, rest[0], rest[1:]
}
func (v lengthView) Get(target uint64) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if v.Kind() != 0 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		h := lengthIndex(v.Raw())
		return h.value(h.findNumber(target))
	}
	_, n, shared, payload, meta := lengthHeader(v.Raw())
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
func (v lengthView) GetString(target string) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if v.Kind() != 1 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		h := lengthIndex(v.Raw())
		return h.value(h.findString(target))
	}
	_, n, shared, payload, meta := lengthHeader(v.Raw())
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
func (v lengthView) At(index int) lengthValue {
	if v.n == 0 {
		panic(draft.ErrIndex)
	}
	if v.Kind() != 2 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		h := lengthIndex(v.Raw())
		if index < 0 || index >= h.n {
			panic(draft.ErrIndex)
		}
		return h.value(index)
	}
	_, n, shared, payload, meta := lengthHeader(v.Raw())
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

type lengthIndexHeader struct {
	raw                                []byte
	kind, n, width, keys, codes, cells int
	mode                               byte // 0 all scalars, 1 all spans, 2 mixed bitmap
}

func lengthIndex(raw []byte) lengthIndexHeader {
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	h := lengthIndexHeader{raw: raw, kind: int(t >> 1 & 3), n: n, width: 1 << uint(t>>6), mode: t >> 3 & 3}
	h.cells = len(raw) - 5
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
func (h *lengthIndexHeader) span(i int) bool {
	return h.mode == 1 || h.mode == 2 && h.raw[h.codes+i/8]&(1<<uint(i%8)) != 0
}
func (h *lengthIndexHeader) entry(i int) (start, end int) {
	start = int(getWidth(h.raw[h.keys+i*h.width:], h.width))
	end = h.keys
	if i+1 < h.n {
		end = int(getWidth(h.raw[h.keys+(i+1)*h.width:], h.width))
	}
	return
}
func (h *lengthIndexHeader) value(i int) lengthValue {
	if i < 0 {
		return lengthValue{}
	}
	if h.kind != 0 {
		start, end := h.entry(i)
		if h.kind == 1 {
			n, b := lengthU(h.raw[start:end])
			start = end - len(b) + int(n)
		}
		if h.span(i) {
			return lengthBytes(h.raw[start:end])
		}
		return lengthWord(binary.LittleEndian.Uint64(h.raw[start:end]))
	}
	word := binary.LittleEndian.Uint64(h.raw[h.cells+i*8:])
	if !h.span(i) {
		return lengthWord(word)
	}
	start, n := int(uint32(word)), int(word>>32)
	return lengthBytes(h.raw[start : start+n])
}
func (h *lengthIndexHeader) number(i int) uint64 { return getWidth(h.raw[h.keys+i*h.width:], h.width) }
func (h *lengthIndexHeader) text(i int) []byte {
	start, end := h.entry(i)
	n, b := lengthU(h.raw[start:end])
	return b[:int(n)]
}
func (h *lengthIndexHeader) findNumber(key uint64) int {
	keys := h.raw[h.keys:h.codes]
	n := h.n
	switch h.width {
	case 1:
		if key > 255 {
			return -1
		}
		lo, hi := 0, n
		for lo < hi {
			m := lo + (hi-lo)/2
			if uint64(keys[m]) < key {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo == n || uint64(keys[lo]) != key {
			return -1
		}
		return lo
	case 2:
		if key > 65535 {
			return -1
		}
		lo, hi := 0, n
		for lo < hi {
			m := lo + (hi-lo)/2
			if uint64(binary.LittleEndian.Uint16(keys[m*2:])) < key {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo == n || uint64(binary.LittleEndian.Uint16(keys[lo*2:])) != key {
			return -1
		}
		return lo
	case 4:
		if key > 4294967295 {
			return -1
		}
		lo, hi := 0, n
		for lo < hi {
			m := lo + (hi-lo)/2
			if uint64(binary.LittleEndian.Uint32(keys[m*4:])) < key {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo == n || uint64(binary.LittleEndian.Uint32(keys[lo*4:])) != key {
			return -1
		}
		return lo
	case 8:
		lo, hi := 0, n
		for lo < hi {
			m := lo + (hi-lo)/2
			if uint64(binary.LittleEndian.Uint64(keys[m*8:])) < key {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo == n || uint64(binary.LittleEndian.Uint64(keys[lo*8:])) != key {
			return -1
		}
		return lo
	default:
		panic(draft.ErrFormat)
	}
}
func (h *lengthIndexHeader) findString(key string) int {
	lo, hi := 0, h.n
	for lo < hi {
		m := lo + (hi-lo)/2
		if bytes.Compare(h.text(m), tagStringBytes(key)) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == h.n || !bytes.Equal(h.text(lo), tagStringBytes(key)) {
		return -1
	}
	return lo
}

// Open validates the current container's structure only. Spans can be strings,
// encrypted payloads, or unknown child formats and must remain opaque here.
func lengthOpen(raw []byte) (view lengthView, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errTag {
				panic(p)
			}
			view = lengthView{}
			err = draft.ErrFormat
		}
	}()
	if len(raw) == 0 {
		return lengthView{}, nil
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
		h := lengthIndex(raw)
		if h.n > tagMaxEntries || h.keys < 0 {
			panic(errTag)
		}
		if h.mode == 2 && h.n%8 != 0 && raw[h.cells-1]>>uint(h.n%8) != 0 {
			panic(errTag)
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
	return lengthTrusted(raw), nil
}

// Iteration decodes each selected scalar once. Unlike Get, it does not skip
// values. Its returned keys and values borrow the immutable backing object.
type lengthIterator struct {
	view                              lengthView
	indexed                           lengthIndexHeader
	key                               uint64
	i, n, pos, kind, metaPos, metaEnd int
	shared                            byte
}

func (v lengthView) Iter() lengthIterator {
	it := lengthIterator{view: v}
	if v.n == 0 {
		return it
	}
	if v.Indexed() {
		it.indexed = lengthIndex(v.Raw())
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
func (it *lengthIterator) Next() (key uint64, text []byte, value lengthValue, ok bool) {
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

// Patch requires exclusive ownership and a validated object. A zero in a map
// removes an entry and therefore needs rebuilding; a zero list element does not.
func (v lengthView) Patch(key uint64, text string, word uint64) bool {
	if !v.Indexed() {
		return false
	}
	h := lengthIndex(v.Raw())
	if word == 0 && h.kind != 2 {
		return false
	}
	i := -1
	switch h.kind {
	case 0:
		i = h.findNumber(key)
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
