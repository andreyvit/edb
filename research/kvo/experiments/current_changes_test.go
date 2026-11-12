package kvolab

// Logical change values use the same tags and signed payloads as Compact.
// The low suffix-count bit distinguishes Delete from Set, including Set-zero.
import (
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type currentChange struct {
	Path   []draft.Step
	Value  currentValue
	Delete bool
}
type currentChanges struct {
	Base, Next uint64
	Items      []currentChange
}
type currentChangeWriter struct {
	out, meta  []byte
	prev       []draft.Step
	base, next uint64
	count      int
}

func (w *currentChangeWriter) Reset(dst []byte, base, next uint64) {
	if next <= base {
		panic(draft.ErrValue)
	}
	w.out, w.meta, w.prev = dst, w.meta[:0], w.prev[:0]
	w.base, w.next, w.count = base, next, 0
}
func (w *currentChangeWriter) record(path []draft.Step, deleted bool, value classMeta) {
	if len(path) > tagMaxEntries {
		panic(draft.ErrLimit)
	}
	common := 0
	for common < len(w.prev) && common < len(path) && sameLengthStep(w.prev[common], path[common]) {
		common++
	}
	flag := uint64(0)
	if deleted {
		flag = 1
	}
	w.meta = binary.AppendUvarint(w.meta, uint64(common))
	w.meta = binary.AppendUvarint(w.meta, uint64(len(path)-common)<<1|flag)
	for _, step := range path[common:] {
		w.meta = appendLengthStep(w.meta, step)
	}
	if !deleted {
		w.meta = oneBitAppendTag(w.meta, value)
	}
	w.prev = append(w.prev[:0], path...)
	w.count++
	if w.count > tagMaxEntries || len(w.out) > tagMaxBytes || len(w.meta) > tagMaxBytes {
		panic(draft.ErrLimit)
	}
}
func (w *currentChangeWriter) Put(path []draft.Step, value currentValue) {
	m := classMeta{wire: draft.Wire(value.scalarWidth()), word: value.num}
	if value.isSpan() {
		if value.num > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		m.wire, m.word = 9, value.num<<32
		w.out = append(w.out, value.Bytes()...)
	} else if value.num > 127 {
		w.out = tagAppendWord(w.out, value.num, int(m.wire))
	}
	w.record(path, false, m)
}
func (w *currentChangeWriter) Delete(path []draft.Step) { w.record(path, true, classMeta{}) }

// RecordSpan records bytes just emitted directly into out by a row writer.
func (w *currentChangeWriter) RecordSpan(path []draft.Step, start int) {
	if start < 0 || start > len(w.out) {
		panic(draft.ErrValue)
	}
	w.record(path, false, classMeta{wire: 9, word: uint64(len(w.out)-start) << 32})
}
func (w *currentChangeWriter) Finish() []byte {
	start := len(w.out)
	w.out = binary.AppendUvarint(w.out, w.base)
	w.out = binary.AppendUvarint(w.out, w.next)
	w.out = binary.AppendUvarint(w.out, uint64(w.count))
	w.out = append(w.out, w.meta...)
	w.out = tagAppendTail(w.out, uint64(len(w.out)-start))
	if len(w.out) > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	return w.out
}
func (w *currentChangeWriter) Append(dst []byte, c currentChanges) []byte {
	w.Reset(dst, c.Base, c.Next)
	for _, item := range c.Items {
		if item.Delete {
			w.Delete(item.Path)
		} else {
			w.Put(item.Path, item.Value)
		}
	}
	return w.Finish()
}

type currentChangeView struct {
	Path   []lengthPathPart
	Value  currentValue
	Delete bool
}
type currentChangeReader struct{ lengthChangeReader }

func newCurrentChanges(raw []byte, scratch []lengthPathPart) currentChangeReader {
	return currentChangeReader{newLengthCatalogChanges(raw, scratch)}
}
func (r *currentChangeReader) NextChange() (out currentChangeView, ok bool) {
	if r.left == 0 {
		if r.pos != r.metaEnd || r.valuePos != r.valueEnd {
			panic(errTag)
		}
		return
	}
	b := r.data[r.pos:r.metaEnd]
	common, b := tagReadU(b)
	suffix, b := tagReadU(b)
	out.Delete = suffix&1 != 0
	suffix >>= 1
	if common > uint64(len(r.path)) || suffix > tagMaxEntries-common {
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

	if !out.Delete {
		if len(b) == 0 {
			panic(errTag)
		}
		tag := b[0]
		b = b[1:]
		if tag < 128 {
			out.Value = currentWord(uint64(tag))
		} else {
			n := uint64(tag & 127)
			if n == 127 {
				var extra uint64
				extra, b = tagReadU(b)
				if extra > tagMaxBytes-127 {
					panic(errTag)
				}
				n += extra
			}
			if n > uint64(r.valueEnd-r.valuePos) {
				panic(errTag)
			}
			out.Value = currentBytes(r.data[r.valuePos : r.valuePos+int(n)])
			r.valuePos += int(n)
		}
	}
	r.pos = r.metaEnd - len(b)
	r.left--
	out.Path = r.path
	return out, true
}
func openCurrentChanges(raw []byte, scratch []lengthPathPart) (r currentChangeReader, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errTag {
				panic(p)
			}
			r, err = currentChangeReader{}, draft.ErrFormat
		}
	}()
	r = newCurrentChanges(raw, scratch)
	for {
		if _, ok := r.NextChange(); !ok {
			break
		}
	}
	r.Reset()
	return r, nil
}
