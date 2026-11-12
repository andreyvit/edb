package row2

import (
	"bytes"
	"encoding/binary"
)

// Open panics on error. Use TryOpen to handle errors explicitly.
func Open(raw []byte) View {
	v, err := TryOpen(raw)
	if err != nil {
		panic(err)
	}
	return v
}

// TryOpen validates a single bounded container. Spans are opaque; for untrusted
// input, open a modeled child separately before trusting its contents.
func TryOpen(raw []byte) (v View, err error) {
	if len(raw) == 0 {
		return View{}, nil
	}
	if len(raw) > MaxBytes {
		return View{}, ErrFormat
	}
	defer func() {
		if p := recover(); p != nil {
			if p != ErrFormat {
				panic(p)
			}
			v = View{}
			err = ErrFormat
		}
	}()
	if raw[len(raw)-1]&1 == 0 {
		it := compactIter(raw)
		for {
			_, _, _, ok := it.Next()
			if !ok {
				break
			}
		}
	} else {
		if len(raw) < 5 {
			panic(ErrFormat)
		}
		t := raw[len(raw)-1]
		if t&32 != 0 || Kind(t>>1&3) > List || fastDirectoryMode(t>>3&3) > fastDirectoryMixed {
			panic(ErrFormat)
		}
		h := directory(raw)
		if h.n < 0 || h.n > MaxEntries || h.keys < 0 || h.cells < 0 || h.codes < 0 {
			panic(ErrFormat)
		}
		if h.mode == fastDirectoryMixed && h.n%8 != 0 && raw[h.codes+(h.n+7)/8-1]>>uint(h.n%8) != 0 {
			panic(ErrFormat)
		}
		limit := h.keys
		if h.kind == Object {
			limit = h.cells
		}
		pos := 0
		var prevNum uint64
		var prevText []byte
		for i := 0; i < h.n; i++ {
			if h.kind == Object {
				k := h.number(i)
				if i > 0 && k <= prevNum {
					panic(ErrFormat)
				}
				prevNum = k
				if h.span(i) {
					u := binary.LittleEndian.Uint64(raw[h.cells+i*8:])
					at, n := int(uint32(u)), int(u>>32)
					if at != pos || n < 0 || n > limit-pos {
						panic(ErrFormat)
					}
					pos += n
				}
			} else {
				start, end := h.entry(i)
				if start != pos || end < start || end > h.keys {
					panic(ErrFormat)
				}
				if h.kind == Dictionary {
					n, b := readUvarint(raw[start:end])
					if n > uint64(len(b)) {
						panic(ErrFormat)
					}
					text := b[:int(n)]
					if i > 0 && bytes.Compare(prevText, text) >= 0 {
						panic(ErrFormat)
					}
					prevText = text
					start = end - len(b) + int(n)
				}
				if !h.span(i) && end-start != 8 {
					panic(ErrFormat)
				}
				pos = end
			}
		}
		if pos != limit {
			panic(ErrFormat)
		}
	}
	return Trusted(raw), nil
}
