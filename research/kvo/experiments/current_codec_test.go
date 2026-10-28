package kvolab

// Selected row codec: the measured adjacent-key Fast directory, and counted
// Compact entries with direct positive tags and byte lengths. The old codecs
// remain untouched, runnable controls. Payloads still stream into one buffer.
import (
	"encoding/binary"
	"math"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type currentWriter struct{ lengthWriter }

func (w *currentWriter) Begin(key uint64, text string, kind int, fast bool) {
	if w.depth > 0 && !w.frames[w.depth-1].indexed {
		fast = false
	}
	w.lengthWriter.Begin(key, text, kind, fast)
}
func (w *currentWriter) Put(key uint64, text string, v currentValue) {
	w.put(key, text, v, false)
}
func (w *currentWriter) PutPresent(key uint64, text string, v currentValue) {
	w.put(key, text, v, true)
}
func (w *currentWriter) put(key uint64, text string, v currentValue, keepZero bool) {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	key, rollback := w.prepare(key, text)
	f := &w.frames[w.depth-1]
	if v.IsZero() && !keepZero && f.kind != 2 {
		w.out = w.out[:rollback]
		return
	}
	word, code := v.num, byte(0)
	if v.isSpan() {
		if v.num > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		code = 9
		word = uint64(len(w.out)-f.start) | v.num<<32
		w.out = append(w.out, v.Bytes()...)
	} else if f.indexed {
		code = 8
		if f.kind != 0 {
			w.out = binary.LittleEndian.AppendUint64(w.out, word)
		}
	} else if word > 127 {
		code = byte(v.scalarWidth())
		w.out = tagAppendWord(w.out, word, int(code))
	}
	w.add(key, code, word)
}
func currentAppendCompact(dst []byte, f *lengthFrame) []byte {
	cat := len(dst)
	dst = binary.AppendUvarint(dst, uint64(len(f.entries)))
	var previous uint64
	for _, m := range f.entries {
		wide := m.wire == 8 || m.wire == 9 && m.word>>32 == 8
		switch f.kind {
		case 0:
			dst = oneBitAppendKey(dst, m.key-previous, wide)
			previous = m.key
		case 1:
			at, n := int(uint32(m.key)), int(m.key>>32)
			dst = oneBitAppendKey(dst, uint64(n), wide)
			dst = append(dst, f.keys[at:at+n]...)
		case 2:
			wide = false
		}
		if !wide {
			dst = oneBitAppendTag(dst, m)
		}
	}
	return tagAppendTail(dst, uint64(len(dst)-cat)<<3|uint64(f.kind)<<1)
}
func (w *currentWriter) End() {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) == 0 && w.depth > 1 {
		key, rollback := f.parentKey, f.rollback
		w.out = w.out[:rollback]
		w.depth--
		if w.frames[w.depth-1].kind == 2 {
			if w.frames[w.depth-1].indexed {
				w.out = binary.LittleEndian.AppendUint64(w.out, 0)
			}
			w.add(key, 0, 0)
		}
		return
	}
	if f.indexed {
		w.out = appendBulkDirectory(w.out, f)
	} else {
		w.out = currentAppendCompact(w.out, f)
	}
	n := len(w.out) - f.start
	if n > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	key, start := f.parentKey, f.start
	w.depth--
	if w.depth > 0 {
		w.add(key, 9, uint64(start-w.frames[w.depth-1].start)|uint64(n)<<32)
	}
}

func currentEncode(w *currentWriter, dst []byte, v Value, policy func(int, Value) bool) []byte {
	w.Reset(dst)
	var walk func(uint64, string, Value, int)
	walk = func(key uint64, text string, x Value, depth int) {
		switch x.Kind {
		case Record, Dict, List:
			w.Begin(key, text, int(tagContainerKind(x.Kind)), policy != nil && policy(depth, x))
			switch x.Kind {
			case Record:
				w.ReserveEntries(len(x.Fields))
				for _, f := range x.Fields {
					walk(f.Key, "", f.Value, depth+1)
				}
			case Dict:
				w.ReserveEntries(len(x.Entries))
				for _, p := range x.Entries {
					walk(0, p.Key, p.Value, depth+1)
				}
			case List:
				w.ReserveEntries(len(x.Items))
				for _, item := range x.Items {
					walk(0, "", item, depth+1)
				}
			}
			w.End()
		case Int:
			w.Put(key, text, currentInt(int64(x.U)))
		case Text:
			w.Put(key, text, currentBytes(tagStringBytes(x.S)))
		case Blob:
			w.Put(key, text, currentBytes(x.B))
		default:
			n := x.U
			switch x.Kind {
			case Null, False:
				n = 0
			case True:
				n = 1
			case Float:
				if math.Float64frombits(n) == 0 {
					n = 0
				}
			}
			w.Put(key, text, currentWord(n))
		}
	}
	walk(0, "", v, 0)
	return w.BytesWritten()
}

type currentView struct{ lengthView }

func currentTrusted(raw []byte) currentView { return currentView{lengthTrusted(raw)} }
func (v currentView) Patch(key uint64, text string, word uint64) bool {
	return (keysLastView{v.lengthView}).Patch(key, text, word)
}
func currentOpen(raw []byte) (currentView, error) {
	if len(raw) > 0 && raw[len(raw)-1]&1 == 0 {
		if err := oneBitOpen(raw, false); err != nil {
			return currentView{}, err
		}
	} else if _, err := keysLastOpen(raw); err != nil {
		return currentView{}, err
	}
	return currentTrusted(raw), nil
}
func (v currentView) Len() int {
	if v.n == 0 {
		return 0
	}
	if v.Indexed() {
		return v.lengthView.Len()
	}
	return oneBitIter(v.Raw(), false).count
}
func (v currentView) GetWrapped(key uint64) currentValue {
	if v.n == 0 {
		return currentValue{}
	}
	if v.Indexed() {
		return currentFromFast(keysLastTinyGet(v.lengthView, key, true, 4))
	}
	if v.Kind() != 0 {
		panic(draft.ErrKind)
	}
	return currentFromCompact(oneBitTrustedGet(v.Raw(), false, key, ""))
}
func (v currentView) GetStringWrapped(key string) currentValue {
	if v.n == 0 {
		return currentValue{}
	}
	if v.Indexed() {
		return currentFromFast(dictFusedGet(v.lengthView, key))
	}
	if v.Kind() != 1 {
		panic(draft.ErrKind)
	}
	return currentFromCompact(oneBitTrustedGet(v.Raw(), false, 0, key))
}
func (v currentView) AtWrapped(index int) currentValue {
	if v.Indexed() {
		return currentFromFast(listScalarAt(v.lengthView, index))
	}
	if v.Kind() != 2 {
		panic(draft.ErrKind)
	}
	if index < 0 || index >= v.Len() {
		panic(draft.ErrIndex)
	}

	it := oneBitIter(v.Raw(), false)
	for {
		key, _, value, ok := it.Next()
		if !ok {
			panic(draft.ErrIndex)
		}
		if key == uint64(index) {
			return currentFromCompact(value)
		}
	}
}

// The selected iterator keeps only the state used by these two layouts.
type currentIterator struct {
	indexed lengthIndexHeader
	compact oneBitIterator
	index   int
	fast    bool
}

func (v currentView) Iter() currentIterator {
	if v.Indexed() {
		return currentIterator{fast: true, indexed: keysLastIndex(v.Raw())}
	}
	return currentIterator{compact: oneBitIter(v.Raw(), false)}
}
func (it *currentIterator) Next() (key uint64, text []byte, value currentValue, ok bool) {
	if !it.fast {
		var old lengthValue
		key, text, old, ok = it.compact.Next()
		return key, text, currentFromCompact(old), ok
	}
	i := it.index
	if i == it.indexed.n {
		return
	}
	it.index++
	h := &it.indexed
	switch h.kind {
	case 0:
		key = h.number(i)
	case 1:
		text = h.text(i)
	case 2:
		key = uint64(i)
	}
	return key, text, currentFromFast(h.value(i)), true
}

// Both larger iterator controls remain callable and benchmarked.
type currentWrappedIterator struct {
	fast    bool
	indexed keysLastIterator
	compact oneBitIterator
}

func (v currentView) IterWrapped() currentWrappedIterator {
	if v.Indexed() {
		return currentWrappedIterator{fast: true, indexed: (keysLastView{v.lengthView}).Iter()}
	}
	return currentWrappedIterator{compact: oneBitIter(v.Raw(), false)}
}
func (it *currentWrappedIterator) NextFused() (key uint64, text []byte, value currentValue, ok bool) {
	if !it.fast {
		var old lengthValue
		key, text, old, ok = it.compact.Next()
		return key, text, currentFromCompact(old), ok
	}
	// The layout is already known. Avoid another iterator dispatch/call for
	// each entry while preserving the same shared header and value decoder.
	i := it.indexed.i
	if i == it.indexed.n {
		return
	}
	it.indexed.i++
	h := &it.indexed.indexed
	switch h.kind {
	case 0:
		key = h.number(i)
	case 1:
		text = h.text(i)
	case 2:
		key = uint64(i)
	}
	return key, text, currentFromFast(h.value(i)), true
}

// Retained dispatch wrapper, for a paired implementation-only comparison.
func (it *currentWrappedIterator) Next() (key uint64, text []byte, value currentValue, ok bool) {
	var old lengthValue
	if it.fast {
		key, text, old, ok = it.indexed.Next()
		value = currentFromFast(old)
	} else {
		key, text, old, ok = it.compact.Next()
		value = currentFromCompact(old)
	}
	return
}
