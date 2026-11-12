package row2

import (
	"bytes"
	"encoding/binary"
	"math"
	"unsafe"
)

// The flag uses the low bit; delta has all 64 bits. This is a 65-bit uvarint,
// with at most ten bytes. Unlike a reserved-code escape it also covers key zero.
func appendKey(dst []byte, key uint64, wide bool) []byte {
	first := byte(key&63) << 1
	if wide {
		first |= 1
	}
	key >>= 6
	if key == 0 {
		return append(dst, first)
	}
	dst = append(dst, first|128)
	return binary.AppendUvarint(dst, key)
}
func readKey(src []byte) (key uint64, wide bool, rest []byte) {
	if len(src) == 0 {
		panic(ErrFormat)
	}
	first := src[0]
	src = src[1:]
	key = uint64(first >> 1 & 63)
	wide = first&1 != 0
	if first < 128 {
		return key, wide, src
	}
	hi, r := readSmallUvarint(src)
	if hi > math.MaxUint64>>6 {
		panic(ErrFormat)
	}
	return key | hi<<6, wide, r
}
func appendTag(dst []byte, m entryMeta) []byte {
	if m.wire != wirePayload && m.word <= 127 {
		return append(dst, byte(m.word))
	}
	n := uint64(m.wire)
	if m.wire == wirePayload {
		n = m.word >> 32
	}
	if n < 127 {
		return append(dst, 128+byte(n))
	}
	dst = append(dst, 255)
	return binary.AppendUvarint(dst, n-127)
}

type compactIterator struct {
	payload, meta, previousText []byte
	kind                        Kind
	count, index, pos           int
	key                         uint64
}

func compactIter(raw []byte) compactIterator {
	if len(raw) == 0 {
		return compactIterator{}
	}
	if len(raw) > MaxBytes {
		panic(ErrFormat)
	}
	footer, body := readTail(raw)
	kind := Kind(footer >> 1 & 3)
	cat := footer >> 3
	if footer&1 != 0 || kind > List || cat > uint64(len(body)) {
		panic(ErrFormat)
	}
	at := len(body) - int(cat)
	meta := body[at:]
	count, meta := readSmallUvarint(meta)
	if count > MaxEntries {
		panic(ErrFormat)
	}
	n := int(count)
	return compactIterator{payload: body[:at], meta: meta, kind: kind, count: n}
}
func (it *compactIterator) Next() (key uint64, text []byte, value Field, ok bool) {
	if it.index == it.count {
		if it.pos != len(it.payload) || len(it.meta) != 0 {
			panic(ErrFormat)
		}
		return
	}
	if it.index >= MaxEntries {
		panic(ErrFormat)
	}
	var wide bool
	switch it.kind {
	case Object:
		var delta uint64
		delta, wide, it.meta = readKey(it.meta)
		if delta > math.MaxUint64-it.key || it.index > 0 && delta == 0 {
			panic(ErrFormat)
		}
		it.key += delta
		key = it.key
	case Dictionary:
		var n uint64
		n, wide, it.meta = readKey(it.meta)
		if n > uint64(len(it.meta)) {
			panic(ErrFormat)
		}
		text, it.meta = it.meta[:int(n)], it.meta[int(n):]
		if it.index > 0 && bytes.Compare(it.previousText, text) >= 0 {
			panic(ErrFormat)
		}
		it.previousText = text
	case List:
		key = uint64(it.index)
	}
	n := 8
	if !wide {
		if len(it.meta) == 0 {
			panic(ErrFormat)
		}
		tag := it.meta[0]
		it.meta = it.meta[1:]
		if tag < 128 {
			it.index++
			return key, text, rawWord(uint64(tag)), true
		}
		n = int(tag & 127)
		if n == 127 {
			u, r := readSmallUvarint(it.meta)
			it.meta = r
			if u > MaxBytes-127 {
				panic(ErrFormat)
			}
			n = 127 + int(u)
		}
	}
	if n > len(it.payload)-it.pos {
		panic(ErrFormat)
	}
	value = payloadField(it.payload[it.pos : it.pos+n])
	it.pos += n
	it.index++
	return key, text, value, true
}
func compactGet(raw []byte, target uint64, text string) Field {
	if len(raw) == 0 {
		return Field{}
	}
	footer := uint64(raw[len(raw)-1])
	body := raw[:len(raw)-1]
	if footer >= 128 {
		footer, body = readTail(raw)
	}
	kind := Kind(footer >> 1 & 3)
	at := len(body) - int(footer>>3)
	payload, meta := body[:at], body[at:]
	_, meta = readSmallUvarint(meta)
	var key uint64
	pos, index := 0, uint64(0)
	for len(meta) != 0 {
		wide := false
		cmp := 0
		switch kind {
		case Object:
			var delta uint64
			delta, wide, meta = readKey(meta)
			key += delta
			if key > target {
				return Field{}
			}
			if key < target {
				cmp = -1
			}
		case Dictionary:
			var n uint64
			n, wide, meta = readKey(meta)
			s := meta[:int(n)]
			meta = meta[int(n):]
			cmp = bytes.Compare(s, stringBytes(text))
			if cmp > 0 {
				return Field{}
			}
		case List:
			if index < target {
				cmp = -1
			}
			index++
		}
		n := 8
		if !wide {
			tag := meta[0]
			meta = meta[1:]
			if tag < 128 {
				if cmp == 0 {
					return rawWord(uint64(tag))
				}
				continue
			}
			n = int(tag & 127)
			if n == 127 {
				var u uint64
				u, meta = readSmallUvarint(meta)
				n += int(u)
			}
		}
		if cmp == 0 {
			return payloadField(payload[pos : pos+n])
		}
		pos += n
	}
	return Field{}
}
func stringBytes(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }
