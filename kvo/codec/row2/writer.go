package row2

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
	"strconv"
	"sync"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// Kind identifies a row2 container. Its values are part of the wire format.
type Kind int

const (
	Object     Kind = 0
	Dictionary Kind = 1
	List       Kind = 2
)

func (k Kind) String() string {
	switch k {
	case Object:
		return "object"
	case Dictionary:
		return "dictionary"
	case List:
		return "list"
	default:
		return "Kind(" + strconv.Itoa(int(k)) + ")"
	}
}

// Layout selects the physical row2 container layout. The values match the
// low trailer bit: Compact2 is zero and Fast2 is one. They are not EDB's
// StorageFormat IDs, which also include other codecs.
type Layout uint8

const (
	Compact2 Layout = 0
	Fast2    Layout = 1
)

func (l Layout) String() string {
	switch l {
	case Fast2:
		return "Fast2"
	case Compact2:
		return "Compact2"
	default:
		return "Layout(" + strconv.Itoa(int(l)) + ")"
	}
}

// wireCode describes writer scratch entries. Codes 1..8 are scalar byte
// widths; inline scalars have no payload, and payload entries carry a span.
type wireCode byte

const (
	wireInline   wireCode = 0
	wireFullWord wireCode = 8
	wirePayload  wireCode = 9
	wireMixed    wireCode = 15
)

func (c wireCode) String() string {
	switch c {
	case wireInline:
		return "inline"
	case wirePayload:
		return "payload"
	case wireMixed:
		return "mixed"
	default:
		if c <= wireFullWord {
			return "scalar(" + strconv.Itoa(int(c)) + ")"
		}
		return "wireCode(" + strconv.Itoa(int(c)) + ")"
	}
}

type entryMeta struct {
	key, word uint64
	wire      wireCode
}

// Writer streams nested containers into one destination. Keys within an
// object or dictionary must arrive in strictly increasing order.
type Writer struct {
	out    []byte
	frames []frame
	depth  int
}
type frame struct {
	entries         []entryMeta
	keys            []byte
	last, parentKey uint64
	start, rollback int
	kind            Kind
	layout          Layout
	seen            bool
	shared          wireCode
}

var writerPool = sync.Pool{New: func() any { return new(Writer) }}

// AcquireWriter returns a writer with reusable directory scratch. dst and the
// encoded bytes belong to the caller, including after Release.
func AcquireWriter(dst []byte) *Writer {
	w := writerPool.Get().(*Writer)
	w.Reset(dst)
	return w
}

// Release an acquired writer exactly once. It returns directory scratch to the pool. Discard unusually large
// workspaces so one wide or deeply nested row cannot pin them between writes.
func (w *Writer) Release() {
	w.Reset(nil)
	used := cap(w.frames) * int(unsafe.Sizeof(frame{}))
	for _, f := range w.frames {
		used += cap(f.entries)*int(unsafe.Sizeof(entryMeta{})) + cap(f.keys)
	}
	if used <= 64<<10 {
		writerPool.Put(w)
	}
}

func (w *Writer) Reset(dst []byte) {
	w.out, w.depth = dst, 0
}
func (w *Writer) prepare(key uint64, text string) (uint64, int) {
	f := &w.frames[w.depth-1]
	switch f.kind {
	case Object:
		if f.seen && key <= f.last {
			panic(ErrOrder)
		}
	case Dictionary:
		if f.seen {
			at, n := int(uint32(f.last)), int(f.last>>32)
			if string(f.keys[at:at+n]) >= text {
				panic(ErrOrder)
			}
		}
	case List:
		key = uint64(len(f.entries))
		if f.layout == Fast2 {
			key = uint64(len(w.out) - f.start)
		}
	default:
		panic(ErrKind)
	}
	f.seen, f.last = true, key
	rollback := len(w.out)
	if f.kind == Dictionary {
		if len(text) > MaxBytes {
			panic(ErrLimit)
		}
		f.last = uint64(len(f.keys)) | uint64(len(text))<<32
		f.keys = append(f.keys, text...)
		if f.layout == Fast2 {
			key = uint64(len(w.out) - f.start)
			w.out = binary.AppendUvarint(w.out, uint64(len(text)))
			w.out = append(w.out, text...)
		} else {
			key = f.last
		}
	}
	return key, rollback
}
func (w *Writer) add(key uint64, code wireCode, word uint64) {
	f := &w.frames[w.depth-1]
	if f.layout == Fast2 && code != wirePayload {
		code = wireFullWord
	}
	if len(f.entries) == 0 {
		f.shared = code
	} else if f.shared != code {
		f.shared = wireMixed
	}
	f.entries = append(f.entries, entryMeta{key: key, word: word, wire: code})
	if len(f.entries) > MaxEntries || len(w.out)-f.start > MaxBytes {
		panic(ErrLimit)
	}
}
func (w *Writer) Begin(key uint64, text string, kind Kind, layout Layout) {
	if layout != Fast2 && layout != Compact2 {
		panic(ErrLayout)
	}
	if w.depth > 0 && w.frames[w.depth-1].layout == Compact2 {
		layout = Compact2
	}
	if kind < Object || kind > List {
		panic(ErrKind)
	}
	rollback := len(w.out)
	if w.depth > 0 {
		key, rollback = w.prepare(key, text)
	}
	if w.depth == len(w.frames) {
		w.frames = append(w.frames, frame{})
	}
	old := &w.frames[w.depth]
	*old = frame{entries: old.entries[:0], keys: old.keys[:0], start: len(w.out), rollback: rollback, parentKey: key, kind: kind, layout: layout, shared: wireMixed}
	w.depth++
}

// ReserveEntries is optional capacity advice, not a count or framing promise.
// A source slice/model can supply it without sizing or encoding any child.
func (w *Writer) ReserveEntries(n int) {
	if w.depth == 0 || n < 0 {
		panic(ErrValue)
	}
	n = min(n, MaxEntries)
	f := &w.frames[w.depth-1]
	if n > cap(f.entries) {
		grown := make([]entryMeta, len(f.entries), n)
		copy(grown, f.entries)
		f.entries = grown
	}
}
func (w *Writer) BytesWritten() []byte {
	if w.depth != 0 {
		panic(ErrKind)
	}
	return w.out
}
func keyWidth(k uint64) int {
	switch {
	case k <= 255:
		return 1
	case k <= 65535:
		return 2
	case k <= math.MaxUint32:
		return 4
	default:
		return 8
	}
}
func (w *Writer) Put(key uint64, text string, v values.Value, enc Encoding) {
	w.put(key, text, v, enc, false)
}
func (w *Writer) PutPresent(key uint64, text string, v values.Value, enc Encoding) {
	w.put(key, text, v, enc, true)
}

// PutRaw preserves an unknown field's representation, including explicit zero.
func (w *Writer) PutRaw(key uint64, text string, v Field) {
	if v.ptr != nil {
		w.put(key, text, values.Bytes(v.Payload()), Data, true)
	} else {
		w.put(key, text, values.Uint(v.num), rawEncoding, true)
	}
}

// rawEncoding keeps untyped Fast2 words full-width when copying into Compact2.
const rawEncoding Encoding = 255

func (w *Writer) put(key uint64, text string, v values.Value, enc Encoding, keep bool) {
	if enc == Timestamp && v.P != nil && v.R != 12 {
		v = values.Time(v.AsTime())
	}
	if w.depth == 0 {
		panic(ErrKind)
	}
	key, rollback := w.prepare(key, text)
	f := &w.frames[w.depth-1]
	if v.R == 0 && !keep && f.kind != List {
		w.out = w.out[:rollback]
		return
	}
	word := v.R
	code := wireInline
	if (enc == Data || enc == Timestamp && v.P != nil) && v.R != 0 {
		if v.R > MaxBytes {
			panic(ErrLimit)
		}
		word = uint64(len(w.out)-f.start) | v.R<<32
		w.out = append(w.out, v.AsBytes()...)
		code = wirePayload
	} else {
		word = wireWord(v, enc)
		if f.layout == Fast2 {
			code = wireFullWord
			if f.kind != Object {
				w.out = binary.LittleEndian.AppendUint64(w.out, word)
			}
		} else if word > 127 {
			code = wireCode(scalarWidth(word, enc))
			if enc == rawEncoding {
				code = wireFullWord
			}
			w.out = appendWord(w.out, word, int(code))
		}
	}
	w.add(key, code, word)
}
func appendCompact(dst []byte, f *frame) []byte {
	cat := len(dst)
	dst = binary.AppendUvarint(dst, uint64(len(f.entries)))
	var previous uint64
	for _, m := range f.entries {
		wide := m.wire == wireFullWord || m.wire == wirePayload && m.word>>32 == 8
		switch f.kind {
		case Object:
			dst = appendKey(dst, m.key-previous, wide)
			previous = m.key
		case Dictionary:
			at, n := int(uint32(m.key)), int(m.key>>32)
			dst = appendKey(dst, uint64(n), wide)
			dst = append(dst, f.keys[at:at+n]...)
		case List:
			wide = false
		}
		if !wide {
			dst = appendTag(dst, m)
		}
	}
	return appendTail(dst, uint64(len(dst)-cat)<<3|uint64(f.kind)<<1)
}

// Count reports entries emitted in the currently open container.
func (w *Writer) Count() int { return len(w.frames[w.depth-1].entries) }

// Finish closes a container, optionally preserving an explicitly empty child.
func (w *Writer) Finish(keepEmpty bool) { w.end(keepEmpty) }
func (w *Writer) End()                  { w.end(false) }
func (w *Writer) end(keepZero bool) {
	if w.depth == 0 {
		panic(ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) == 0 && w.depth > 1 {
		key, rollback := f.parentKey, f.rollback
		if keepZero {
			rollback = f.start
		}
		w.out = w.out[:rollback]
		w.depth--
		if w.frames[w.depth-1].kind == List || keepZero {
			if w.frames[w.depth-1].layout == Fast2 && w.frames[w.depth-1].kind != Object {
				w.out = binary.LittleEndian.AppendUint64(w.out, 0)
			}
			w.add(key, wireInline, 0)
		}
		return
	}
	if f.layout == Fast2 {
		w.out = appendFast(w.out, f)
	} else {
		w.out = appendCompact(w.out, f)
	}
	n := len(w.out) - f.start
	if n > MaxBytes {
		panic(ErrLimit)
	}
	key, start := f.parentKey, f.start
	w.depth--
	if w.depth > 0 {
		w.add(key, wirePayload, uint64(start-w.frames[w.depth-1].start)|uint64(n)<<32)
	}
}
func appendFast(dst []byte, f *frame) []byte {
	n := len(f.entries)
	width := 1
	if n > 0 {
		width = keyWidth(f.entries[n-1].key)
	}
	mode, bitmap := fastDirectoryScalars, 0
	if f.shared == wirePayload {
		mode = fastDirectorySpans
	} else if f.shared == wireMixed && n > 0 {
		mode, bitmap = fastDirectoryMixed, (n+7)/8
	}
	size := n*width + bitmap + 5
	if f.kind == Object {
		size += n * 8
	}
	start := len(dst)
	dst = slices.Grow(dst, size)
	dst = dst[:start+size]
	keys, codes := start, start+n*width
	if f.kind == Object {
		codes = start + n*8
		keys = codes + bitmap
		for i, m := range f.entries {
			binary.LittleEndian.PutUint64(dst[start+i*8:], m.word)
		}
	}
	if bitmap > 0 {
		clear(dst[codes : codes+bitmap])
		for i, m := range f.entries {
			if m.wire == wirePayload {
				dst[codes+i/8] |= 1 << uint(i&7)
			}
		}
	}

	switch width {
	case 1:
		for i, m := range f.entries {
			dst[keys+i] = byte(m.key)
		}
	case 2:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint16(dst[keys+i*2:], uint16(m.key))
		}
	case 4:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint32(dst[keys+i*4:], uint32(m.key))
		}
	case 8:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint64(dst[keys+i*8:], m.key)
		}
	}
	footer := start + size - 5
	binary.LittleEndian.PutUint32(dst[footer:], uint32(n))
	dst[footer+4] = 1 | byte(f.kind)<<1 | byte(mode)<<3 | byte(bits.Len(uint(width))-1)<<6
	return dst
}
