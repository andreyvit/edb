package row2

import (
	"encoding/binary"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// View borrows one encoded container. Raw and child spans are read-only.
type View struct {
	ptr *byte
	n   int
}

// Trusted borrows bytes produced by the codec or validated at ingestion.
// The bytes and any borrowed children must outlive the view.
func Trusted(b []byte) View { return View{unsafe.SliceData(b), len(b)} }
func (v View) Raw() []byte  { return unsafe.Slice(v.ptr, v.n) }

// Fast path is intentionally small enough to inline. All public trusted access
// operates on bytes from Writer or on a container already checked by Open.
func readSmallUvarint(b []byte) (uint64, []byte) {
	if len(b) == 0 {
		panic(ErrFormat)
	}
	if b[0] < 128 {
		return uint64(b[0]), b[1:]
	}
	return readUvarint(b)
}
func (v View) Kind() Kind {
	if v.n == 0 {
		return Object
	}
	return Kind(v.Raw()[v.n-1] >> 1 & 3)
}

// Layout returns Compact2 for a missing view; use Missing to test presence.
func (v View) Layout() Layout {
	if v.n == 0 {
		return Compact2
	}
	return Layout(v.Raw()[v.n-1] & 1)
}

type Iterator struct {
	indexed fastDirectory
	compact compactIterator
	index   int
	layout  Layout
}

func (v View) Iter() Iterator {
	if v.Layout() == Fast2 {
		return Iterator{layout: Fast2, indexed: directory(v.Raw())}
	}
	return Iterator{compact: compactIter(v.Raw())}
}
func (it *Iterator) Next() (key uint64, text []byte, value Field, ok bool) {
	if it.layout == Compact2 {
		return it.compact.Next()
	}
	i := it.index
	if i == it.indexed.n {
		return
	}
	it.index++
	h := &it.indexed
	switch h.kind {
	case Object:
		key = h.number(i)
	case Dictionary:
		text = h.text(i)
	case List:
		key = uint64(i)
	}
	return key, text, h.value(i), true
}

func (v View) Missing() bool { return v.n == 0 }
func (v View) Len() int {
	if v.n == 0 {
		return 0
	}
	if v.Layout() == Fast2 {
		return int(binary.LittleEndian.Uint32(v.Raw()[v.n-5:]))
	}
	return compactIter(v.Raw()).count
}

// Get decodes one property using its declared encoding; it does not decode a row.
func (v View) Get(key uint64, enc Encoding) values.Value       { return v.RawField(key).Decode(enc) }
func (v View) GetString(key string, enc Encoding) values.Value { return v.RawString(key).Decode(enc) }
func (v View) At(index int, enc Encoding) values.Value         { return v.RawAt(index).Decode(enc) }
