package kvolab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

// Logical paths and revision rules stay unchanged. The four-bit value length
// code shares the suffix-count varint. Code 10 means deletion; code 0 is an
// explicit zero assignment. There is no redundant outer value-length wrapper.
type lengthChange struct {
	Path   []draft.Step
	Value  lengthValue
	Delete bool
}
type lengthChanges struct {
	Base, Next uint64
	Items      []lengthChange
}

func lengthStepKind(k draft.StepKind) draft.StepKind {
	if k == draft.NumberStep {
		return draft.FieldStep
	}
	return k
}
func sameLengthStep(a, b draft.Step) bool {
	if lengthStepKind(a.Kind) != lengthStepKind(b.Kind) {
		return false
	}
	if a.Kind == draft.StringStep {
		return a.Text == b.Text
	}
	return a.Number == b.Number
}
func appendLengthStep(dst []byte, s draft.Step) []byte {
	switch s.Kind {
	case draft.FieldStep, draft.NumberStep:
		if s.Number <= math.MaxUint64>>2 {
			return binary.AppendUvarint(dst, s.Number<<2)
		}
		dst = append(dst, 1)
		return binary.AppendUvarint(dst, s.Number)
	case draft.IndexStep:
		if s.Number>>62 != 0 {
			panic(draft.ErrLimit)
		}
		return binary.AppendUvarint(dst, s.Number<<2|3)
	case draft.StringStep:
		if len(s.Text) > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		dst = binary.AppendUvarint(dst, uint64(len(s.Text))<<2|2)
		return append(dst, s.Text...)
	default:
		panic(draft.ErrValue)
	}
}
func appendLengthChanges(dst []byte, c lengthChanges) []byte {
	if c.Next <= c.Base {
		panic(draft.ErrValue)
	}
	if len(c.Items) > tagMaxEntries {
		panic(draft.ErrLimit)
	}
	dst = binary.AppendUvarint(dst, c.Base)
	dst = binary.AppendUvarint(dst, c.Next)
	dst = binary.AppendUvarint(dst, uint64(len(c.Items)))
	var prev []draft.Step
	for _, x := range c.Items {
		if len(x.Path) > tagMaxEntries {
			panic(draft.ErrLimit)
		}
		common := 0
		for common < len(prev) && common < len(x.Path) && sameLengthStep(prev[common], x.Path[common]) {
			common++
		}
		code := byte((bits.Len64(x.Value.num) + 7) / 8)
		if x.Delete {
			code = 10
		} else if x.Value.ptr != nil {
			code = 9
		}
		dst = binary.AppendUvarint(dst, uint64(common))
		dst = binary.AppendUvarint(dst, uint64(len(x.Path)-common)<<4|uint64(code))
		for _, s := range x.Path[common:] {
			dst = appendLengthStep(dst, s)
		}

		if code == 9 {
			if x.Value.num > tagMaxBytes {
				panic(draft.ErrLimit)
			}
			dst = binary.AppendUvarint(dst, x.Value.num)
			dst = append(dst, x.Value.Bytes()...)
		} else if code <= 8 {
			dst = tagAppendWord(dst, x.Value.num, int(code))
		}
		if len(dst) > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		prev = x.Path
	}
	return dst
}

type lengthPathPart struct {
	Number     uint64
	start, end uint32
	Kind       draft.StepKind
}
type lengthChangeView struct {
	Path   []lengthPathPart
	Value  lengthValue
	Delete bool
}
type lengthChangeReader struct {
	Base, Next                  uint64
	data                        []byte
	pos, left, start, count     int
	path                        []lengthPathPart
	metaEnd, valuePos, valueEnd int
	catalog                     bool
}

func newLengthChanges(raw []byte, scratch []lengthPathPart) lengthChangeReader {
	if len(raw) > tagMaxBytes {
		panic(errTag)
	}
	base, b := tagReadU(raw)
	next, b := tagReadU(b)
	count, b := tagReadU(b)
	if next <= base || count > tagMaxEntries {
		panic(errTag)
	}
	return lengthChangeReader{Base: base, Next: next, data: raw, pos: len(raw) - len(b), left: int(count), start: len(raw) - len(b), count: int(count), path: scratch[:0], metaEnd: len(raw)}
}
func (r *lengthChangeReader) Key(p lengthPathPart) []byte { return r.data[int(p.start):int(p.end)] }
func (r *lengthChangeReader) Reset() {
	r.pos, r.left = r.start, r.count
	r.path = r.path[:0]
	r.valuePos = 0
}
func (r *lengthChangeReader) NextChange() (out lengthChangeView, ok bool) {
	if r.left == 0 {
		if r.pos != r.metaEnd || r.catalog && r.valuePos != r.valueEnd {
			panic(errTag)
		}
		return
	}
	b := r.data[r.pos:r.metaEnd]
	common, b := tagReadU(b)
	suffix, b := tagReadU(b)
	code := byte(suffix & 15)
	suffix >>= 4
	if code > 10 || common > uint64(len(r.path)) || suffix > tagMaxEntries-common {
		panic(errTag)
	}
	r.path = r.path[:int(common)]
	for range int(suffix) {
		tag, rest := tagReadU(b)
		b = rest
		p := lengthPathPart{Kind: draft.StepKind(tag & 3), Number: tag >> 2}
		switch p.Kind {
		case draft.NumberStep:
			if tag != 1 {
				panic(errTag)
			}
			p.Number, b = tagReadU(b)
			p.Kind = draft.FieldStep
		case draft.StringStep:
			if p.Number > uint64(len(b)) {
				panic(errTag)
			}
			p.start = uint32(r.metaEnd - len(b))
			p.end = p.start + uint32(p.Number)
			b = b[int(p.Number):]
			p.Number = 0
		}
		if len(r.path) == cap(r.path) {
			grown := make([]lengthPathPart, len(r.path), max(8, len(r.path)*2))
			copy(grown, r.path)
			r.path = grown
		}
		r.path = r.path[:len(r.path)+1]
		r.path[len(r.path)-1] = p
	}
	n := uint64(code)
	if code == 9 {
		n, b = tagReadU(b)
	} else if code == 10 {
		n = 0
		out.Delete = true
	}
	body := b
	if r.catalog {
		body = r.data[r.valuePos:r.valueEnd]
	}
	if n > uint64(len(body)) {
		panic(errTag)
	}
	if code == 9 {
		out.Value = lengthBytes(body[:int(n)])
	} else if !out.Delete {
		out.Value = lengthWord(tagWord(body[:int(n)]))
	}
	if r.catalog {
		r.valuePos += int(n)
	} else {
		b = b[int(n):]
	}
	r.pos = r.metaEnd - len(b)
	r.left--
	out.Path = r.path
	return out, true
}
func openLengthChanges(raw []byte, scratch []lengthPathPart) (r lengthChangeReader, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errTag {
				panic(p)
			}
			r = lengthChangeReader{}
			err = draft.ErrFormat
		}
	}()
	r = newLengthChanges(raw, scratch)
	for {
		if _, ok := r.NextChange(); !ok {
			break
		}
	}
	r.Reset()
	return r, nil
}
func lengthChangeFixture(n int) (lengthChanges, draft.Changes) {
	c := lengthChanges{Base: 41, Next: 42}
	old := draft.Changes{Base: 41, Next: 42}
	for i := 0; i < n; i++ {
		path := []draft.Step{{Kind: draft.FieldStep, Number: 58}, {Kind: draft.StringStep, Text: fmt.Sprintf("key-%04d", i)}}
		value := lengthBytes([]byte("changed"))
		ov := draft.ByteString(value.Bytes())
		if i%3 == 0 {
			value = lengthWord(uint64(i * 997))
			ov = draft.Uint(value.Uint())
		}
		deleted := i%7 == 6
		c.Items = append(c.Items, lengthChange{Path: path, Value: value, Delete: deleted})
		old.Items = append(old.Items, draft.Change{Path: path, Value: ov, Delete: deleted})
	}
	return c, old
}
func TestLengthChanges(t *testing.T) {
	c, old := lengthChangeFixture(16)
	c.Items = append(c.Items, lengthChange{Path: []draft.Step{{Kind: draft.NumberStep, Number: math.MaxUint64}, {Kind: draft.IndexStep, Number: 2}}, Value: lengthWord(math.MaxUint64)})
	// An eight-part suffix exercises the count/code boundary that costs an
	// extra byte relative to the old one-bit deletion flag.
	path := make([]draft.Step, 8)
	for i := range path {
		path[i] = draft.Step{Kind: draft.FieldStep, Number: uint64(i)}
	}
	c.Items = append(c.Items, lengthChange{Path: path, Value: lengthWord(0)}, lengthChange{Path: path, Delete: true}, lengthChange{Path: []draft.Step{{Kind: draft.FieldStep, Number: math.MaxUint64}}, Value: lengthWord(17)})
	raw := appendLengthChanges(nil, c)
	var scratch [16]lengthPathPart
	r, err := openLengthChanges(raw, scratch[:])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range c.Items {
		got, ok := r.NextChange()
		if !ok || got.Delete != want.Delete || len(got.Path) != len(want.Path) {
			t.Fatal("change")
		}
		for i, p := range got.Path {
			w := want.Path[i]
			if p.Kind != lengthStepKind(w.Kind) || p.Number != w.Number || p.Kind == draft.StringStep && string(r.Key(p)) != w.Text {
				t.Fatal("path")
			}
		}
		if !got.Delete {
			if want.Value.ptr == nil {
				if got.Value.Uint() != want.Value.Uint() {
					t.Fatal("word")
				}
			} else if !bytes.Equal(got.Value.Bytes(), want.Value.Bytes()) {
				t.Fatal("bytes")
			}
		}
	}
	if _, ok := r.NextChange(); ok {
		t.Fatal("extra")
	}
	if _, err = openLengthChanges(append(bytes.Clone(raw), 0), nil); err == nil {
		t.Fatal("trailing garbage")
	}
	for n := 0; n < len(raw); n++ {
		if _, err = openLengthChanges(raw[:n], nil); err == nil {
			t.Fatalf("truncation %d", n)
		}
	}
	t.Logf("mixed 16 changes: old=%d new=%d", len(draft.AppendChanges(nil, old)), len(appendLengthChanges(nil, lengthChanges{Base: c.Base, Next: c.Next, Items: c.Items[:16]})))
	// The previous format's reader and this reader preserve identical actions.
	baseline := draft.AppendChanges(nil, old)
	var oldScratch [16]draft.PathPart
	or, err := draft.OpenChanges(baseline, oldScratch[:])
	if err != nil {
		t.Fatal(err)
	}
	nr, _ := openLengthChanges(appendLengthChanges(nil, lengthChanges{Base: c.Base, Next: c.Next, Items: c.Items[:16]}), scratch[:])
	for {
		a, ok, err := or.NextChange()
		if err != nil {
			t.Fatal(err)
		}
		b, bok := nr.NextChange()
		if bok != ok {
			t.Fatal("length")
		}
		if !ok {
			break
		}
		if a.Delete != b.Delete {
			t.Fatal("delete")
		}
		if !a.Delete {
			if a.Value.Wire() == draft.Bytes {
				if !bytes.Equal(a.Value.Bytes(), b.Value.Bytes()) {
					t.Fatal("span")
				}
			} else if a.Value.Uint() != b.Value.Uint() {
				t.Fatal("scalar")
			}
		}
	}
}
func FuzzLengthChanges(f *testing.F) {
	for _, n := range []int{0, 1, 8, 64} {
		c, _ := lengthChangeFixture(n)
		f.Add(appendLengthChanges(nil, c))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			return
		}
		var scratch [32]lengthPathPart
		r, err := openLengthChanges(raw, scratch[:])
		if err != nil {
			return
		}
		n := 0
		for {
			if _, ok := r.NextChange(); !ok {
				break
			}
			n++
		}
		if n != r.count {
			t.Fatal("count")
		}
	})
}
func BenchmarkLengthChanges(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		c, old := lengthChangeFixture(n)
		raw := appendLengthChanges(nil, c)
		oraw := draft.AppendChanges(nil, old)
		b.Run(fmt.Sprintf("n%d/old-write", n), func(b *testing.B) {
			dst := bytes.Clone(oraw)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dst = draft.AppendChanges(dst[:0], old)
			}
			sinkBytes = dst
		})
		b.Run(fmt.Sprintf("n%d/new-write", n), func(b *testing.B) {
			dst := bytes.Clone(raw)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dst = appendLengthChanges(dst[:0], c)
			}
			sinkBytes = dst
		})
		b.Run(fmt.Sprintf("n%d/old-read-checked", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var scratch [16]draft.PathPart
				r, err := draft.OpenChanges(oraw, scratch[:])
				if err != nil {
					b.Fatal(err)
				}
				for {
					_, ok, err := r.NextChange()
					if err != nil {
						b.Fatal(err)
					}
					if !ok {
						break
					}
				}
			}
		})
		b.Run(fmt.Sprintf("n%d/new-read-checked", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var scratch [16]lengthPathPart
				r := newLengthChanges(raw, scratch[:])
				for {
					if _, ok := r.NextChange(); !ok {
						break
					}
				}
			}
		})
		b.Run(fmt.Sprintf("n%d/new-open-then-read", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var scratch [16]lengthPathPart
				r, err := openLengthChanges(raw, scratch[:])
				if err != nil {
					b.Fatal(err)
				}
				for {
					if _, ok := r.NextChange(); !ok {
						break
					}
				}
			}
		})
	}
}

// The same payload-first construction also works for a batch of changes. Only
// path/length metadata is buffered. Call record after writing a value directly
// into out; this permits an unknown-size replacement object to use lengthWriter.
type lengthChangeWriter struct {
	out, meta  []byte
	prev       []draft.Step
	base, next uint64
	count      int
}

func (w *lengthChangeWriter) Reset(dst []byte, base, next uint64) {
	if next <= base {
		panic(draft.ErrValue)
	}
	w.out, w.meta, w.prev, w.base, w.next, w.count = dst, w.meta[:0], nil, base, next, 0
}
func (w *lengthChangeWriter) record(path []draft.Step, start int, code byte) {
	if code > 10 || start > len(w.out) || len(path) > tagMaxEntries {
		panic(draft.ErrValue)
	}
	common := 0
	for common < len(w.prev) && common < len(path) && sameLengthStep(w.prev[common], path[common]) {
		common++
	}
	w.meta = binary.AppendUvarint(w.meta, uint64(common))
	w.meta = binary.AppendUvarint(w.meta, uint64(len(path)-common)<<4|uint64(code))
	for _, s := range path[common:] {
		w.meta = appendLengthStep(w.meta, s)
	}

	n := len(w.out) - start
	if code == 9 {
		w.meta = binary.AppendUvarint(w.meta, uint64(n))
	} else if code <= 8 && n != int(code) || code == 10 && n != 0 {
		panic(draft.ErrValue)
	}
	w.count++
	w.prev = path
	if w.count > tagMaxEntries || len(w.meta) > tagMaxBytes || len(w.out) > tagMaxBytes {
		panic(draft.ErrLimit)
	}
}
func (w *lengthChangeWriter) Finish() []byte {
	start := len(w.out)
	w.out = binary.AppendUvarint(w.out, w.base)
	w.out = binary.AppendUvarint(w.out, w.next)
	w.out = binary.AppendUvarint(w.out, uint64(w.count))
	w.out = append(w.out, w.meta...)
	w.out = tagAppendTail(w.out, uint64(len(w.out)-start))
	w.prev = nil
	if len(w.out) > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	return w.out
}
func (w *lengthChangeWriter) Append(dst []byte, c lengthChanges) []byte {
	w.Reset(dst, c.Base, c.Next)
	for _, x := range c.Items {
		start := len(w.out)
		code := byte((bits.Len64(x.Value.num) + 7) / 8)
		if x.Delete {
			code = 10
		} else if x.Value.ptr != nil {
			code = 9
			w.out = append(w.out, x.Value.Bytes()...)
		} else {
			w.out = tagAppendWord(w.out, x.Value.num, int(code))
		}
		w.record(x.Path, start, code)
	}
	return w.Finish()
}
func newLengthCatalogChanges(raw []byte, scratch []lengthPathPart) lengthChangeReader {
	if len(raw) > tagMaxBytes {
		panic(errTag)
	}
	n, b := tagTail(raw)
	if n > uint64(len(b)) {
		panic(errTag)
	}
	at := len(b) - int(n)
	r := newLengthChanges(b[at:], scratch)
	r.data = raw
	r.start += at
	r.pos += at
	r.metaEnd = len(b)
	r.valueEnd = at
	r.catalog = true
	return r
}
func openLengthCatalogChanges(raw []byte, scratch []lengthPathPart) (r lengthChangeReader, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errTag {
				panic(p)
			}
			r = lengthChangeReader{}
			err = draft.ErrFormat
		}
	}()
	r = newLengthCatalogChanges(raw, scratch)
	for {
		if _, ok := r.NextChange(); !ok {
			break
		}
	}
	r.Reset()
	return r, nil
}
func TestLengthChangeCatalog(t *testing.T) {
	for _, n := range []int{0, 1, 4, 16, 64} {
		c, old := lengthChangeFixture(n)
		var w lengthChangeWriter
		raw := w.Append(nil, c)
		var scratch [8]lengthPathPart
		r, err := openLengthCatalogChanges(raw, scratch[:])
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range c.Items {
			got, ok := r.NextChange()
			if !ok || got.Delete != want.Delete || len(got.Path) != len(want.Path) {
				t.Fatal("item")
			}
			for i, s := range got.Path {
				if s.Kind != lengthStepKind(want.Path[i].Kind) || s.Number != want.Path[i].Number || s.Kind == draft.StringStep && string(r.Key(s)) != want.Path[i].Text {
					t.Fatal("path")
				}
			}
			if !got.Delete {
				if want.Value.ptr == nil {
					if got.Value.Uint() != want.Value.Uint() {
						t.Fatal("word")
					}
				} else if !bytes.Equal(want.Value.Bytes(), got.Value.Bytes()) {
					t.Fatal("span")
				}
			}
		}
		if _, ok := r.NextChange(); ok {
			t.Fatal("extra")
		}
		t.Logf("changes=%d old=%d inline-length=%d catalog-length=%d", n, len(draft.AppendChanges(nil, old)), len(appendLengthChanges(nil, c)), len(raw))
	}
	// An object of previously unknown size goes straight into the change output.
	var changes lengthChangeWriter
	var objects lengthWriter
	changes.Reset(nil, 1, 2)
	start := len(changes.out)
	objects.Reset(changes.out)
	objects.Begin(0, "", 0, false)
	objects.Put(1, "", lengthWord(7))
	objects.Begin(2, "", 1, true)
	objects.Put(0, "title", lengthBytes([]byte("sample")))
	objects.End()
	objects.End()
	changes.out = objects.BytesWritten()
	changes.record([]draft.Step{{Kind: draft.FieldStep, Number: 1}}, start, 9)
	raw := changes.Finish()
	var scratch [4]lengthPathPart
	r, err := openLengthCatalogChanges(raw, scratch[:])
	if err != nil {
		t.Fatal(err)
	}
	got, ok := r.NextChange()
	if !ok || got.Value.Object().Get(1).Uint() != 7 || string(got.Value.Object().Get(2).Object().GetString("title").Bytes()) != "sample" {
		t.Fatal("nested")
	}
}
func FuzzLengthCatalogChanges(f *testing.F) {
	for _, n := range []int{0, 1, 8, 64} {
		c, _ := lengthChangeFixture(n)
		var w lengthChangeWriter
		f.Add(w.Append(nil, c))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			return
		}
		var scratch [32]lengthPathPart
		r, err := openLengthCatalogChanges(raw, scratch[:])
		if err != nil {
			return
		}
		n := 0
		for {
			if _, ok := r.NextChange(); !ok {
				break
			}
			n++
		}
		if n != r.count {
			t.Fatal("count")
		}
	})
}
func BenchmarkLengthCatalogChanges(b *testing.B) {
	for _, n := range []int{1, 4, 16, 64} {
		c, _ := lengthChangeFixture(n)
		var w lengthChangeWriter
		raw := w.Append(nil, c)
		b.Run(fmt.Sprintf("n%d/write", n), func(b *testing.B) {
			dst := bytes.Clone(raw)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				dst = w.Append(dst[:0], c)
			}
			sinkBytes = dst
		})
		b.Run(fmt.Sprintf("n%d/read-checked", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var scratch [16]lengthPathPart
				r := newLengthCatalogChanges(raw, scratch[:])
				for {
					if _, ok := r.NextChange(); !ok {
						break
					}
				}
			}
		})
		b.Run(fmt.Sprintf("n%d/open-then-read", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var scratch [16]lengthPathPart
				r, err := openLengthCatalogChanges(raw, scratch[:])
				if err != nil {
					b.Fatal(err)
				}
				for {
					if _, ok := r.NextChange(); !ok {
						break
					}
				}
			}
		})
	}
}
