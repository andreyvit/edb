package kvolab

// This file is a comparison experiment, not another supported KVO API. It keeps
// value framing, container framing, and construction strategy independent.

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"unsafe"
)

type tagFormat byte

const (
	tagLength  tagFormat = iota // only uvar(length); semantics are entirely external
	tagUnified                  // 128 positive, 63 negative, 64 lengths, one escape
	tagSplit                    // user's scalar/span partition; 16 codes reserved
	tagTuned                    // 128 positive, 16 negative, 8 scalar widths, 103 spans
)

var tagNames = [...]string{"length", "unified", "split", "tuned"}
var errTag = errors.New("invalid experimental tag encoding")

const tagMaxBytes = 64 << 20
const tagMaxEntries = 1 << 20

// All pointers remain GC-visible. No integer is stored in a pointer and no
// pointer is hidden in an integer. A nil pointer means an immediate word. For a
// span, bit 63 of num is a physical scalar hint; the other bits are its length.
// Length-only/unified spans have no hint: their interpretation is external.
type tagCell struct {
	ptr *byte
	num uint64
}

func tagSpan(b []byte, scalar bool) tagCell {
	if len(b) == 0 {
		return tagCell{}
	}
	n := uint64(len(b))
	if scalar {
		n |= 1 << 63
	}
	return tagCell{unsafe.SliceData(b), n}
}
func (c tagCell) raw() []byte {
	if c.ptr == nil {
		if c.num != 0 {
			panic(errTag)
		}
		return nil
	}
	return unsafe.Slice(c.ptr, int(c.num&math.MaxInt64))
}
func (c tagCell) uint() uint64 {
	if c.ptr == nil {
		return c.num
	}
	return tagWord(c.raw())
}
func (c tagCell) sint(f tagFormat) int64 {
	if f == tagLength {
		u := c.uint()
		return int64(u>>1) ^ -int64(u&1)
	}
	if c.ptr == nil {
		return int64(c.num)
	}
	b := c.raw()
	u := tagWord(b)
	if len(b) != 0 && len(b) < 8 && b[len(b)-1]&128 != 0 {
		u |= math.MaxUint64 << (uint(len(b)) * 8)
	}
	return int64(u)
}

func tagWord(b []byte) uint64 {
	switch len(b) {
	case 0:
		return 0
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	}
	if len(b) > 8 {
		panic(errTag)
	}
	var u uint64
	for i, x := range b {
		u |= uint64(x) << (uint(i) * 8)
	}
	return u
}
func tagAppendWord(dst []byte, u uint64, n int) []byte {
	switch n {
	case 0:
		return dst
	case 1:
		return append(dst, byte(u))
	case 2:
		return binary.LittleEndian.AppendUint16(dst, uint16(u))
	case 4:
		return binary.LittleEndian.AppendUint32(dst, uint32(u))
	case 8:
		return binary.LittleEndian.AppendUint64(dst, u)
	}
	for range n {
		dst = append(dst, byte(u))
		u >>= 8
	}
	return dst
}
func tagScalar(v Value, f tagFormat) (word uint64, size int, immediate bool, code byte) {
	u := v.U
	switch v.Kind {
	case Null, False:
		u = 0
	case True:
		u = 1
	case Float:
		if math.Float64frombits(u) == 0 {
			u = 0
		} else {
			return u, 8, false, 0
		}
	}
	if f == tagLength {
		if v.Kind == Int {
			x := int64(u)
			u = uint64(x<<1) ^ uint64(x>>63)
		}
		return u, (bits.Len64(u) + 7) / 8, false, 0
	}
	if u <= 127 {
		return u, 0, true, byte(u)
	}
	negatives := int64(-16)
	if f == tagUnified {
		negatives = -63
	}
	if v.Kind == Int && int64(u) >= negatives && int64(u) < 0 {
		return u, 0, true, byte(u)
	}
	if v.Kind == Int {
		// Minimal two's-complement bytes, preserving a sign bit. Larger signed
		// values need a signed model accessor; unknown fields stay opaque.
		x := int64(u)
		if x < 0 {
			u = ^u
		}
		size = max(1, (bits.Len64(u)+1+7)/8)
		return v.U, size, false, 0
	}
	return u, (bits.Len64(u) + 7) / 8, false, 0
}
func tagAppendHead(dst []byte, f tagFormat, n int, scalar bool) []byte {
	if n < 0 || n > tagMaxBytes {
		panic(errTag)
	}
	switch f {
	case tagLength:
		return binary.AppendUvarint(dst, uint64(n))
	case tagUnified:
		if n <= 63 {
			return append(dst, 0x80+byte(n))
		}
		dst = append(dst, 0xc0)
		return binary.AppendUvarint(dst, uint64(n-64))
	case tagSplit:
		if scalar {
			if n <= 30 {
				return append(dst, 0xc0+byte(n))
			}
			dst = append(dst, 0xdf)
			return binary.AppendUvarint(dst, uint64(n-31))
		}
		if n <= 62 {
			return append(dst, 0x80+byte(n))
		}
		dst = append(dst, 0xbf)
		return binary.AppendUvarint(dst, uint64(n-63))
	case tagTuned:
		if scalar {
			if n < 1 || n > 8 {
				panic(errTag)
			}
			return append(dst, 0xe7+byte(n))
		}
		if n <= 102 {
			return append(dst, 0x80+byte(n))
		}
		dst = append(dst, 0xe7)
		return binary.AppendUvarint(dst, uint64(n-103))
	}
	panic(errTag)
}
func tagReadU(b []byte) (uint64, []byte) {
	u, n := binary.Uvarint(b)
	if n <= 0 || n != max(1, (bits.Len64(u)+6)/7) {
		panic(errTag)
	}
	return u, b[n:]
}

// tagHead consumes metadata only. Catalog readers use the size to walk the
// separate payload stream; inline readers consume the body immediately.
func tagHead(b []byte, f tagFormat) (c tagCell, n int, scalar bool, rest []byte) {
	if f == tagLength {
		u, r := tagReadU(b)
		if u > tagMaxBytes {
			panic(errTag)
		}
		return tagCell{}, int(u), false, r
	}
	if len(b) == 0 {
		panic(errTag)
	}
	t := b[0]
	b = b[1:]
	if t < 128 {
		return tagCell{num: uint64(t)}, 0, true, b
	}
	if (f == tagUnified && t > 0xc0) || (f != tagUnified && t >= 0xf0) {
		return tagCell{num: uint64(int64(int8(t)))}, 0, true, b
	}
	var escape int
	switch f {
	case tagUnified:
		if t < 0xc0 {
			return tagCell{}, int(t - 0x80), false, b
		}
		escape = 64
	case tagSplit:
		if t <= 0xbe {
			return tagCell{}, int(t - 0x80), false, b
		}
		if t == 0xbf {
			escape = 63
		} else if t <= 0xde {
			return tagCell{}, int(t - 0xc0), true, b
		} else if t == 0xdf {
			escape, scalar = 31, true
		} else {
			panic(errTag)
		}
	case tagTuned:
		if t <= 0xe6 {
			return tagCell{}, int(t - 0x80), false, b
		}
		if t >= 0xe8 {
			return tagCell{}, int(t - 0xe7), true, b
		}
		escape = 103
	default:
		panic(errTag)
	}
	u, r := tagReadU(b)
	if u > uint64(tagMaxBytes-escape) {
		panic(errTag)
	}
	return tagCell{}, int(u) + escape, scalar, r
}
func tagRead(b []byte, f tagFormat) (tagCell, []byte) {
	c, n, scalar, rest := tagHead(b, f)
	if n > len(rest) {
		panic(errTag)
	}
	if n != 0 {
		c = tagSpan(rest[:n], scalar)
	}
	return c, rest[n:]
}
func tagZero(v Value) bool {
	switch v.Kind {
	case Null, False:
		return true
	case Uint, Int:
		return v.U == 0
	case Float:
		return math.Float64frombits(v.U) == 0
	case Text:
		return len(v.S) == 0
	case Blob:
		return len(v.B) == 0
	case Record:
		for _, x := range v.Fields {
			if !tagZero(x.Value) {
				return false
			}
		}
		return true
	case Dict:
		for _, x := range v.Entries {
			if !tagZero(x.Value) {
				return false
			}
		}
		return true
	case List:
		return len(v.Items) == 0
	}
	return false
}
func tagContainerKind(k Kind) uint64 {
	switch k {
	case Record:
		return 0
	case Dict:
		return 1
	case List:
		return 2
	}
	panic(errTag)
}

// A reversed canonical varint is decoded from the end of the bounded object.
// It avoids reserving a fixed four/eight-byte count or moving the entries when
// the final count crosses a varint threshold.
func tagAppendTail(dst []byte, u uint64) []byte {
	start := len(dst)
	dst = binary.AppendUvarint(dst, u)
	for i, j := start, len(dst)-1; i < j; i, j = i+1, j-1 {
		dst[i], dst[j] = dst[j], dst[i]
	}
	return dst
}
func tagTail(b []byte) (uint64, []byte) {
	var u uint64
	for i := 0; i < 10 && i < len(b); i++ {
		x := b[len(b)-1-i]
		if i == 9 && x > 1 {
			panic(errTag)
		}
		u |= uint64(x&127) << (uint(i) * 7)
		if x < 128 {
			if i != 0 && x == 0 {
				panic(errTag)
			}
			return u, b[:len(b)-1-i]
		}
	}
	panic(errTag)
}

type tagStrategy byte

const (
	tagBuffered  tagStrategy = iota // reuse child payload buffers, copy into parents
	tagBackpatch                    // one output, reserve five prefix bytes then compact
	tagCatalog                      // one output, buffer only metadata until End
)

type tagLevel struct{ scratch []byte }
type tagEncoder struct {
	format   tagFormat
	strategy tagStrategy
	fuseKeys bool // separate candidate grammar, selected by its outer envelope
	levels   []tagLevel
	// Counters are filled by an optional untimed accounting pass, not benchmarks.
	account *tagAccounting
}
type tagAccounting struct {
	leafBytes, childCopied, headerMoved, metadataCopied int
}

func (e *tagEncoder) ensure(depth int) {
	for len(e.levels) <= depth {
		e.levels = append(e.levels, tagLevel{})
	}
}
func (e *tagEncoder) object(dst []byte, v Value, depth int) []byte {
	e.ensure(depth)
	start := len(dst)
	meta := e.levels[depth].scratch[:0]
	n, previous := 0, uint64(0)
	var previousText string
	emit := func(key uint64, text string, value Value, first bool) {
		if !first && ((v.Kind == Record && key <= previous) || (v.Kind == Dict && text <= previousText)) {
			panic(errTag)
		}
		if v.Kind != List && tagZero(value) {
			// Keep ordering checks independent of omission; the emitted delta
			// below uses a separate running key outside this closure.
			return
		}
		n++
		if n > tagMaxEntries {
			panic(errTag)
		}
		bare := 0
		var word uint64
		if e.fuseKeys && v.Kind == Record && value.Kind <= Float {
			u, size, immediate, _ := tagScalar(value, e.format)
			if !immediate && (size == 2 || size == 4 || size == 8) && key-previous <= math.MaxUint64>>2 {
				bare, word = size, u
				// Combined tag 3 is the full-width-key escape. Key zero
				// with an eight-byte value uses the normal value tag instead.
				if key == previous && bare == 8 {
					bare = 0
				}
			}
		}
		if e.strategy == tagCatalog {
			switch v.Kind {
			case Record:
				meta = tagAppendKey(meta, key-previous, bare, e.fuseKeys)
			case Dict:
				meta = binary.AppendUvarint(meta, uint64(len(text)))
				meta = append(meta, text...)
			}
			if bare != 0 {
				dst = tagAppendWord(dst, word, bare)
			} else {
				dst, meta = e.catalogValue(dst, meta, value, depth)
			}
		} else {
			switch v.Kind {
			case Record:
				dst = tagAppendKey(dst, key-previous, bare, e.fuseKeys)
			case Dict:
				dst = binary.AppendUvarint(dst, uint64(len(text)))
				dst = append(dst, text...)
			}
			if bare != 0 {
				dst = tagAppendWord(dst, word, bare)
			} else {
				dst = e.inlineValue(dst, value, depth)
			}
		}
		previous, previousText = key, text
	}
	switch v.Kind {
	case Record:
		var checked uint64
		for i, x := range v.Fields {
			if i > 0 && x.Key <= checked {
				panic(errTag)
			}
			checked = x.Key
			emit(x.Key, "", x.Value, n == 0)
		}
	case Dict:
		for i, x := range v.Entries {
			if i > 0 && x.Key <= v.Entries[i-1].Key {
				panic(errTag)
			}
			emit(0, x.Key, x.Value, n == 0)
		}
	case List:
		for _, x := range v.Items {
			emit(0, "", x, n == 0)
		}
	default:
		panic(errTag)
	}
	if e.strategy == tagCatalog {
		begin := len(dst)
		dst = binary.AppendUvarint(dst, uint64(n))
		dst = append(dst, meta...)
		if e.account != nil {
			e.account.metadataCopied += len(meta)
		}
		dst = tagAppendTail(dst, uint64(len(dst)-begin)<<3|tagContainerKind(v.Kind)<<1)
	} else {
		dst = tagAppendTail(dst, uint64(n)<<3|tagContainerKind(v.Kind)<<1)
	}
	if e.strategy == tagCatalog {
		e.levels[depth].scratch = meta
	}
	if len(dst)-start > tagMaxBytes {
		panic(errTag)
	}
	return dst
}
func tagAppendKey(dst []byte, delta uint64, bare int, fused bool) []byte {
	if !fused {
		return binary.AppendUvarint(dst, delta)
	}
	if delta > math.MaxUint64>>2 {
		dst = append(dst, 3)
		return binary.AppendUvarint(dst, delta)
	}
	var code uint64
	if bare != 0 {
		code = uint64(bits.Len(uint(bare)) - 1)
	}
	return binary.AppendUvarint(dst, delta<<2|code)
}
func (e *tagEncoder) scalar(dst []byte, v Value) []byte {
	u, n, immediate, code := tagScalar(v, e.format)
	if immediate {
		return append(dst, code)
	}
	dst = tagAppendHead(dst, e.format, n, true)
	return tagAppendWord(dst, u, n)
}
func (e *tagEncoder) inlineValue(dst []byte, v Value, depth int) []byte {
	switch v.Kind {
	case Record, Dict, List:
		if tagZero(v) {
			return e.scalar(dst, Value{})
		}
		if e.strategy == tagBuffered {
			// A parent's scratch is not used by the child itself. Each child
			// may replace the previous child's buffer after it has been copied.
			child := e.object(e.levels[depth].scratch[:0], v, depth+1)
			e.levels[depth].scratch = child
			dst = tagAppendHead(dst, e.format, len(child), false)
			if e.account != nil {
				e.account.childCopied += len(child)
			}
			return append(dst, child...)
		}
		// Known 64 MiB limit needs at most five header bytes in these codecs.
		pos := len(dst)
		dst = append(dst, 0, 0, 0, 0, 0)
		dst = e.object(dst, v, depth+1)
		n := len(dst) - pos - 5
		var head [5]byte
		h := tagAppendHead(head[:0], e.format, n, false)
		copy(dst[pos:], h)
		copy(dst[pos+len(h):], dst[pos+5:])
		if e.account != nil {
			e.account.headerMoved += n
		}
		return dst[:len(dst)-5+len(h)]
	case Text, Blob:
		n := len(v.B)
		if v.Kind == Text {
			n = len(v.S)
		}
		dst = tagAppendHead(dst, e.format, n, false)
		if e.account != nil {
			e.account.leafBytes += n
		}
		if v.Kind == Text {
			return append(dst, v.S...)
		}
		return append(dst, v.B...)
	default:
		return e.scalar(dst, v)
	}
}
func (e *tagEncoder) catalogValue(dst, meta []byte, v Value, depth int) ([]byte, []byte) {
	switch v.Kind {
	case Record, Dict, List:
		if tagZero(v) {
			return dst, e.scalar(meta, Value{})
		}
		start := len(dst)
		dst = e.object(dst, v, depth+1)
		return dst, tagAppendHead(meta, e.format, len(dst)-start, false)
	case Text, Blob:
		start := len(dst)
		if v.Kind == Text {
			dst = append(dst, v.S...)
		} else {
			dst = append(dst, v.B...)
		}
		if e.account != nil {
			e.account.leafBytes += len(dst) - start
		}
		return dst, tagAppendHead(meta, e.format, len(dst)-start, false)
	default:
		u, n, immediate, code := tagScalar(v, e.format)
		if immediate {
			return dst, append(meta, code)
		}
		meta = tagAppendHead(meta, e.format, n, true)
		return tagAppendWord(dst, u, n), meta
	}
}

type tagIterator struct {
	meta, payload []byte
	format        tagFormat
	catalog       bool
	fused         bool
	kind          uint64
	left          int
	key           uint64
	index         uint64
}

func tagIter(raw []byte, f tagFormat, catalog bool, fused ...bool) tagIterator {
	u, body := tagTail(raw)
	k := u >> 1 & 3
	if u&1 != 0 || k == 3 {
		panic(errTag)
	}
	var payload []byte
	n := u >> 3
	if catalog {
		if n > uint64(len(body)) {
			panic(errTag)
		}
		payload, body = body[:len(body)-int(n)], body[len(body)-int(n):]
		n, body = tagReadU(body)
	}
	if n > tagMaxEntries {
		panic(errTag)
	}
	return tagIterator{meta: body, payload: payload, format: f, catalog: catalog, fused: len(fused) != 0 && fused[0], kind: k, left: int(n)}
}
func (it *tagIterator) next() (key uint64, text []byte, cell tagCell, ok bool) {
	if it.left == 0 {
		if len(it.meta) != 0 || len(it.payload) != 0 {
			panic(errTag)
		}
		return
	}
	b := it.meta
	bare := 0
	switch it.kind {
	case 0:
		u, r := tagReadU(b)
		if it.fused {
			if u == 3 {
				u, r = tagReadU(r)
				if u <= math.MaxUint64>>2 {
					panic(errTag)
				}
			} else {
				code := u & 3
				if code != 0 {
					bare = 1 << code
				}
				u >>= 2
			}
		}
		if u > math.MaxUint64-it.key || (it.index != 0 && u == 0) {
			panic(errTag)
		}
		it.key += u
		key, b = it.key, r
	case 1:
		u, r := tagReadU(b)
		if u > uint64(len(r)) {
			panic(errTag)
		}
		text, b = r[:int(u)], r[int(u):]
	case 2:
		key = it.index
	}
	if it.catalog {
		c, n, scalar, r := tagCell{}, bare, true, b
		if bare == 0 {
			c, n, scalar, r = tagHead(b, it.format)
		}
		if n > len(it.payload) {
			panic(errTag)
		}
		if n != 0 {
			c = tagSpan(it.payload[:n], scalar)
		}
		cell, it.meta, it.payload = c, r, it.payload[n:]
	} else {
		if bare != 0 {
			if bare > len(b) {
				panic(errTag)
			}
			cell, it.meta = tagSpan(b[:bare], true), b[bare:]
		} else {
			cell, it.meta = tagRead(b, it.format)
		}
	}
	it.left--
	it.index++
	return key, text, cell, true
}
func tagGet(raw []byte, f tagFormat, catalog bool, key uint64, fused ...bool) tagCell {
	it := tagIter(raw, f, catalog, fused...)
	for {
		k, _, v, ok := it.next()
		if !ok || k > key {
			return tagCell{}
		}
		if k == key {
			return v
		}
	}
}
