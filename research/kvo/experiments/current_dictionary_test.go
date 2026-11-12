package kvolab

// A schema-bound Dictionary policy over the selected codec. Raw views/writers
// remain schema-free so they can preserve unknown fields and explicit zeros.
import (
	"bytes"
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type currentDictionaryPolicy struct {
	kind     int
	keepZero bool
}

func bindCurrentDictionary(model *kvo.Model, key uint64) currentDictionaryPolicy {
	prop := model.PropByCode(key)
	if prop == nil {
		panic(draft.ErrUnknownKey)
	}
	typ, ok := prop.AnyType().(*kvo.MapType)
	if !ok {
		panic(draft.ErrType)
	}
	p := currentDictionaryPolicy{keepZero: model.PropFlags(key)&kvo.KeepDictionaryZeroValues != 0}
	switch key := typ.MapKeyType().(type) {
	case *kvo.ScalarDataType:
		if key.NativeType().Kind() != reflect.String {
			panic(draft.ErrType)
		}
		p.kind = 1
	case *kvo.WordType:
		if !nativeUnsignedKind(key.NativeType().Kind()) {
			panic(draft.ErrType)
		}
	default:
		panic(draft.ErrType)
	}
	return p
}

func (p currentDictionaryPolicy) check(v currentView) {
	if v.n != 0 && v.Kind() != p.kind {
		panic(draft.ErrKind)
	}
}

func (p currentDictionaryPolicy) Get(v currentView, key uint64, text string) currentValue {
	p.check(v)
	if p.kind == 1 {
		return v.GetString(text)
	}
	return v.Get(key)
}

func (p currentDictionaryPolicy) Lookup(v currentView, key uint64, text string) (currentValue, bool) {
	p.check(v)
	if !p.keepZero {
		panic(draft.ErrValue)
	}
	return currentLookup(v, key, text)
}

// Structural presence on trusted bytes. Separate from ordinary Get so its hot
// path and return signature are unchanged. Compact searches stop at the key;
// Fast uses the existing searchable directory. No decode or allocation.
func currentLookup(v currentView, key uint64, text string) (currentValue, bool) {
	if v.n == 0 {
		return currentValue{}, false
	}
	kind := v.Kind()
	if kind == 2 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		if kind == 1 && perfNativeWords {
			return currentFastDictionaryLookup(v.lengthView, text)
		}
		h := keysLastIndex(v.Raw())
		i := -1
		if kind == 1 {
			i = h.findString(text)
		} else {
			i = keysLastSafeFind(&h, key)
		}
		return currentFromFast(h.value(i)), i >= 0
	}
	it := v.Iter()
	for {
		number, name, value, ok := it.Next()
		if !ok {
			return currentValue{}, false
		}
		if kind == 1 {
			switch cmp := bytes.Compare(name, tagStringBytes(text)); {
			case cmp == 0:
				return value, true
			case cmp > 0:
				return currentValue{}, false
			}
		} else if number == key {
			return value, true
		} else if number > key {
			return currentValue{}, false
		}
	}
}

func (p currentDictionaryPolicy) Put(w *currentWriter, key uint64, text string, value currentValue) {
	if w.depth == 0 || w.frames[w.depth-1].kind != p.kind {
		panic(draft.ErrKind)
	}
	w.put(key, text, value, p.keepZero)
}

// An absent dictionary has no physical kind; the schema supplies it when an
// editor creates the first entry. This also works for numeric dictionaries.
func (p currentDictionaryPolicy) Edit(v currentView, scratch []classMeta, arena []byte) currentLengthEditor {
	p.check(v)
	e := newCurrentLengthEditor(v, scratch, arena)
	e.kind = p.kind
	return e
}

func (p currentDictionaryPolicy) Set(e *currentLengthEditor, key uint64, text string, value currentValue) {
	if e.kind != p.kind {
		panic(draft.ErrKind)
	}
	e.store(key, text, value, false, p.keepZero)
}

func (p currentDictionaryPolicy) Delete(e *currentLengthEditor, key uint64, text string) {
	if e.kind != p.kind {
		panic(draft.ErrKind)
	}
	e.store(key, text, currentValue{}, true, false)
}

func (p currentDictionaryPolicy) LookupEdit(e *currentLengthEditor, key uint64, text string) (currentValue, bool) {
	if e.kind != p.kind {
		panic(draft.ErrKind)
	}
	if !p.keepZero {
		panic(draft.ErrValue)
	}
	i := e.locate(key, text)
	if i < len(e.edits) && e.compare(e.edits[i], key, text) == 0 {
		return e.value(e.edits[i]), e.edits[i].wire != 10
	}
	return p.Lookup(e.base, key, text)
}

func (p currentDictionaryPolicy) Append(e *currentLengthEditor, w *currentWriter, dst []byte, fast bool) []byte {
	if e.kind != p.kind {
		panic(draft.ErrKind)
	}
	return e.appendWithBaseZeros(w, dst, fast, p.keepZero)
}

type currentDictionaryIterator struct {
	iter     currentIterator
	keepZero bool
}

func (p currentDictionaryPolicy) Iter(v currentView) currentDictionaryIterator {
	p.check(v)
	return currentDictionaryIterator{v.Iter(), p.keepZero}
}

func (it *currentDictionaryIterator) Next() (uint64, []byte, currentValue, bool) {
	for {
		key, text, value, ok := it.iter.Next()
		if !ok || it.keepZero || !value.IsZero() {
			return key, text, value, ok
		}
	}
}

// Retained directory-helper control. The selected presence path below reuses
// ordinary Fast lookup's typed-offset search without changing its wire layout.
func currentDictionaryLookupDirectory(v currentView, text string) (currentValue, bool) {
	h := keysLastIndex(v.Raw())
	i := h.findString(text)
	return currentFromFast(h.value(i)), i >= 0
}

func currentFastDictionaryLookup(v lengthView, target string) (currentValue, bool) {
	if v.n == 0 {
		return currentValue{}, false
	}
	raw := v.Raw()
	end := len(raw) - 5
	t := raw[len(raw)-1]
	if t>>1&3 != 1 || t&1 == 0 {
		panic(draft.ErrKind)
	}
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	if n == 0 {
		return currentValue{}, false
	}
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	codes := end
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	p := unsafe.Pointer(unsafe.SliceData(raw[keys:codes]))
	i, start := -1, 0
	switch width {
	case 1:
		i, start = dictLocate(raw, unsafe.Slice((*uint8)(p), n), target)
	case 2:
		i, start = dictLocate(raw, unsafe.Slice((*uint16)(p), n), target)
	case 4:
		i, start = dictLocate(raw, unsafe.Slice((*uint32)(p), n), target)
	case 8:
		i, start = dictLocate(raw, unsafe.Slice((*uint64)(p), n), target)
	}
	if i < 0 {
		return currentValue{}, false
	}
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return currentRawWord(binary.LittleEndian.Uint64(raw[start:])), true
	}
	finish := keys
	if i+1 < n {
		finish = int(getWidth(raw[keys+(i+1)*width:], width))
	}
	return currentBytes(raw[start:finish]), true
}
