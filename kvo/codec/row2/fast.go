package row2

import (
	"bytes"
	"encoding/binary"
	"math/bits"
	"strconv"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

type fastDirectoryMode byte

const (
	fastDirectoryScalars fastDirectoryMode = 0
	fastDirectorySpans   fastDirectoryMode = 1
	fastDirectoryMixed   fastDirectoryMode = 2
)

func (m fastDirectoryMode) String() string {
	switch m {
	case fastDirectoryScalars:
		return "scalars"
	case fastDirectorySpans:
		return "spans"
	case fastDirectoryMixed:
		return "mixed"
	default:
		return "fastDirectoryMode(" + strconv.Itoa(int(m)) + ")"
	}
}

func directory(raw []byte) fastDirectory {
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	h := fastDirectory{raw: raw, kind: Kind(t >> 1 & 3), n: n, width: 1 << uint(t>>6), mode: fastDirectoryMode(t >> 3 & 3)}
	end := len(raw) - 5
	if h.kind == Object {
		h.keys = end - n*h.width
		h.codes = h.keys
		if h.mode == fastDirectoryMixed {
			h.codes -= (n + 7) / 8
		}
		h.cells = h.codes - n*8
	} else {
		h.cells = end
		h.codes = end
		if h.mode == fastDirectoryMixed {
			h.codes -= (n + 7) / 8
		}
		h.keys = h.codes - n*h.width
	}
	return h
}

type fastDirectory struct {
	raw                          []byte
	kind                         Kind
	n, width, keys, codes, cells int
	mode                         fastDirectoryMode
}

func (h *fastDirectory) span(i int) bool {
	return h.mode == fastDirectorySpans || h.mode == fastDirectoryMixed && h.raw[h.codes+i/8]&(1<<uint(i%8)) != 0
}
func (h *fastDirectory) entry(i int) (start, end int) {
	start = int(getWidth(h.raw[h.keys+i*h.width:], h.width))
	end = h.keys
	if i+1 < h.n {
		end = int(getWidth(h.raw[h.keys+(i+1)*h.width:], h.width))
	}
	return
}
func (h *fastDirectory) value(i int) Field {
	if i < 0 {
		return Field{}
	}
	if h.kind != Object {
		start, end := h.entry(i)
		if h.kind == Dictionary {
			n, b := readSmallUvarint(h.raw[start:end])
			start = end - len(b) + int(n)
		}
		if h.span(i) {
			return payloadField(h.raw[start:end])
		}
		return rawWord(binary.LittleEndian.Uint64(h.raw[start:end]))
	}
	word := binary.LittleEndian.Uint64(h.raw[h.cells+i*8:])
	if !h.span(i) {
		return rawWord(word)
	}
	start, n := int(uint32(word)), int(word>>32)
	return payloadField(h.raw[start : start+n])
}
func (h *fastDirectory) number(i int) uint64 { return getWidth(h.raw[h.keys+i*h.width:], h.width) }
func (h *fastDirectory) text(i int) []byte {
	start, end := h.entry(i)
	n, b := readSmallUvarint(h.raw[start:end])
	return b[:int(n)]
}
func getWidth(b []byte, w int) uint64 {
	switch w {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	default:
		panic("width")
	}
}

// find returns the index in a sorted numeric directory. Small object keys
// usually fit in a byte and need no per-key integer decoding.
func (h *fastDirectory) find(key uint64) int {
	keys := h.raw[h.keys : h.keys+h.n*h.width]
	return findKey(keys, h.width, key)
}
func findKey(keys []byte, width int, key uint64) int {
	if width == 1 {
		if key > 255 {
			return -1
		}
		if len(keys) <= 4 {
			for i, b := range keys {
				if b == byte(key) {
					return i
				}
			}
			return -1
		}
		// Most object directories fit in two words. The lowest marked
		// byte is the first equal key, even when a borrow also marks
		// its following byte. The two loads may overlap.
		if len(keys) >= 8 && len(keys) <= 16 {
			const ones = uint64(0x0101010101010101)
			target := key * ones
			x := binary.LittleEndian.Uint64(keys) ^ target
			if mask := (x - ones) & ^x & (ones << 7); mask != 0 {
				return bits.TrailingZeros64(mask) / 8
			}
			last := len(keys) - 8
			x = binary.LittleEndian.Uint64(keys[last:]) ^ target
			if mask := (x - ones) & ^x & (ones << 7); mask != 0 {
				return last + bits.TrailingZeros64(mask)/8
			}
			return -1
		}
		return bytes.IndexByte(keys, byte(key))
	}
	lo, hi := 0, len(keys)/width
	for lo < hi {
		m := lo + (hi-lo)/2
		k := getWidth(keys[m*width:], width)
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
func (v View) RawField(key uint64) Field {
	if v.n == 0 {
		return Field{}
	}
	raw := v.Raw()
	t := raw[len(raw)-1]
	if Kind(t>>1&3) != Object {
		panic(ErrKind)
	}
	if t&1 == 0 {
		return compactGet(raw, key, "")
	}
	end := len(raw) - 5
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	width := 1 << (t >> 6)
	keys := end - n*width
	i := findKey(raw[keys:end], width, key)
	if i < 0 {
		return Field{}
	}
	mode := fastDirectoryMode(t >> 3 & 3)
	codes := keys
	if mode == fastDirectoryMixed {
		codes -= (n + 7) / 8
	}
	word := binary.LittleEndian.Uint64(raw[codes-8*(n-i):])
	if mode == fastDirectoryScalars || mode == fastDirectoryMixed && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return rawWord(word)
	}
	at, size := int(uint32(word)), int(word>>32)
	return payloadField(raw[at : at+size])
}

// RawString looks up a raw dictionary entry. A missing key returns the zero value.
func (v View) RawString(key string) Field {
	if v.Missing() {
		return Field{}
	}
	if v.Kind() != Dictionary {
		panic(ErrKind)
	}
	if v.Layout() != Fast2 {
		return compactGet(v.Raw(), 0, key)
	}
	h := directory(v.Raw())
	lo, hi := 0, h.n
	for lo < hi {
		m := lo + (hi-lo)/2
		switch bytes.Compare(h.text(m), []byte(key)) {
		case -1:
			lo = m + 1
		case 1:
			hi = m
		default:
			return h.value(m)
		}
	}
	return Field{}
}

func (v View) RawAt(i int) Field {
	if i < 0 || i >= v.Len() {
		panic(ErrIndex)
	}
	if v.Kind() != List {
		panic(ErrKind)
	}
	if v.Layout() != Fast2 {
		return compactGet(v.Raw(), uint64(i), "")
	}
	h := directory(v.Raw())
	return h.value(i)
}

// GetFast borrows a field from Fast2, whose words and spans already have the
// runtime layout. The caller must ensure Indexed() and handle Float32 fields
// through Get(key, Float32), since those retain float64 bits on disk.
func (v View) GetFast(key uint64) values.Value {
	f := v.RawField(key)
	return values.Value{R: f.num, P: unsafe.Pointer(f.ptr)}
}

// HasString distinguishes a missing dictionary entry from a present zero value.
func (v View) HasString(key string) bool {
	if v.Missing() {
		return false
	}
	if v.Kind() != Dictionary {
		panic(ErrKind)
	}
	if v.Layout() == Fast2 {
		h := directory(v.Raw())
		lo, hi := 0, h.n
		for lo < hi {
			m := lo + (hi-lo)/2
			switch bytes.Compare(h.text(m), []byte(key)) {
			case -1:
				lo = m + 1
			case 1:
				hi = m
			default:
				return true
			}
		}
		return false
	}
	it := v.Iter()
	for {
		_, s, _, ok := it.Next()
		if !ok {
			return false
		}
		if string(s) == key {
			return true
		}
	}
}
