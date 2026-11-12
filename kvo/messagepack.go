package kvo

import (
	"bytes"
	"cmp"

	"reflect"
	"slices"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Get's common value representation uses row2 container bytes. Materialize only
// the requested aggregate, directly from MessagePack spans, without Go objects.
func (p *nativePlan) messagePackValue(raw msgpack.View, layout row2.Layout) values.Value {
	w := row2.AcquireWriter(nil)
	defer w.Release()
	p.emitMessagePack(w, raw, 0, "", layout, false, 0)
	return values.Bytes(w.BytesWritten())
}
func (p *nativePlan) emitMessagePack(w *row2.Writer, raw msgpack.View, key uint64, text string, layout row2.Layout, keep bool, depth int) {
	if depth > 256 {
		panic(row2.ErrLimit)
	}
	if p.msgFallback || p.hasMessagePackInlineAlias(raw) || p.kind == nativeJSON {
		p.emit(w, decodeProperty(p, raw), key, text, layout, keep, depth)
		return
	}
	if p.kind == nativeOption {
		if raw.Lookup("Present").Bool() {
			p.elem.emitMessagePack(w, raw.Lookup("Value"), key, text, layout, true, depth+1)
		}
		return
	}
	if p.kind == nativePointer {
		if !raw.Nil() {
			p.elem.emitMessagePack(w, raw, key, text, layout, true, depth+1)
		} else if keep {
			w.PutPresent(key, text, values.Value{}, row2.Word)
		}
		return
	}
	switch p.kind {
	case nativeObject:
		w.Begin(key, text, row2.Object, layout)
		for _, f := range p.fields {
			childLayout := layout
			if f.layout == row2.Compact2 {
				childLayout = row2.Compact2
			}
			var child msgpack.View
			if f.msgName != "-" {
				child = (recordView{msg: raw}).messagePackField(&f)
			}
			f.plan.emitMessagePack(w, child, f.key, "", childLayout, f.significant, depth+1)
		}
		w.Finish(keep)
	case nativeList:
		w.Begin(key, text, row2.List, layout)
		if p.typ.Elem().Kind() == reflect.Uint8 {
			// MessagePack encodes both []byte and [N]byte as binary strings,
			// even when the logical property is declared as a list of uint8.
			data := raw.Bytes()
			length := len(data)
			if p.typ.Kind() == reflect.Array {
				length = p.typ.Len()
				if len(data) > length {
					panic(row2.ErrValue)
				}
			}
			w.ReserveEntries(length)
			for _, n := range data {
				w.PutPresent(0, "", values.Uint(uint64(n)), row2.Word)
			}
			for i := len(data); i < length; i++ {
				w.PutPresent(0, "", values.Value{}, row2.Word)
			}
			w.Finish(keep || p.typ.Kind() == reflect.Slice && !raw.Nil())
			return
		}
		it := raw.Iter()
		length := it.Len()
		if p.typ.Kind() == reflect.Array {
			length = p.typ.Len()
			if it.Len() > length {
				panic(row2.ErrValue)
			}
		}
		w.ReserveEntries(length)
		for range length {
			_, v, ok := it.Next()
			if !ok {
				v = msgpack.View{} // omitted fixed-array elements are zero
			}
			if p.elem.nilable() && v.Nil() {
				panic(row2.ErrValue)
			}
			p.elem.emitMessagePack(w, v, 0, "", layout, true, depth+1)
		}
		w.Finish(keep || p.typ.Kind() == reflect.Slice && !raw.Nil())
	case nativeDictionary, nativeNumbers:
		kind := row2.Object
		if p.kind == nativeDictionary {
			kind = row2.Dictionary
		}
		w.Begin(key, text, kind, layout)
		it := raw.Iter()
		// Sorting references costs one allocation, rather than a Go map and a
		// reflected allocation per key/value. Small maps fit in local scratch.
		var scratch [16]messagePackEntry
		entries := scratch[:0]
		if it.Len() > len(scratch) {
			entries = make([]messagePackEntry, 0, it.Len())
		}
		for {
			k, v, ok := it.Next()
			if !ok {
				break
			}
			e := messagePackEntry{value: v}
			if kind == row2.Dictionary {
				e.name = p.mapKey.messagePackScalar(k).AsBytes()
			} else {
				e.key = p.mapKey.messagePackScalar(k).AsUint()
			}
			entries = append(entries, e)
		}
		compare := func(a, b messagePackEntry) int {
			if kind == row2.Dictionary {
				return bytes.Compare(a.name, b.name)
			}
			return cmp.Compare(a.key, b.key)
		}
		slices.SortStableFunc(entries, compare)
		count := 0
		for i, e := range entries {
			if i+1 < len(entries) && compare(e, entries[i+1]) == 0 {
				continue
			}
			if p.elem.nilable() && e.value.Nil() {
				panic(row2.ErrValue)
			}
			p.elem.emitMessagePack(w, e.value, e.key, string(e.name), layout, p.keepDictionaryZeros, depth+1)
			count++
		}
		if w.Count() != count {
			panic(ErrOmittedEntry)
		}
		w.Finish(keep || !raw.Nil())
	default:
		value := p.messagePackScalar(raw)
		if keep || p.kind == nativeBytes && !raw.Nil() {
			w.PutPresent(key, text, value, p.encoding())
		} else {
			w.Put(key, text, value, p.encoding())
		}
	}
}

type messagePackEntry struct {
	key   uint64
	name  []byte
	value msgpack.View
}

func (p *nativePlan) messagePackScalar(raw msgpack.View) values.Value {
	if p.msgDecoder != nil {
		return p.msgDecoder(raw)
	}
	if raw.Nil() {
		return values.Value{}
	}
	switch p.kind {
	case nativeTimestamp:
		return raw.Time()
	case nativeTime:
		return Timestamp(raw.Time().AsTime())
	case nativeSigned:
		n := raw.Int()
		shift := 64 - p.typ.Bits()
		return values.Int(n << shift >> shift)
	case nativeUnsigned:
		n := raw.Uint()
		shift := 64 - p.typ.Bits()
		return values.Uint(n << shift >> shift)
	case nativeBool:
		if raw.Bool() {
			return values.Uint(1)
		}
		return values.Value{}
	case nativeFloat:
		n := raw.Float()
		if p.typ.Kind() == reflect.Float32 {
			return values.Float32(float32(n))
		}
		return values.Float(n)
	case nativeString, nativeBytes:
		return values.Bytes(raw.Bytes())
	}
	panic(row2.ErrType)
}

func (r recordView) messagePackSnapshot() row2.View {
	var w row2.Writer
	w.Begin(0, "", row2.Object, row2.Compact2)
	for _, f := range r.plan.fields {
		f.plan.emitMessagePack(&w, r.messagePackField(&f), f.key, "", row2.Compact2, f.significant, 0)
	}
	w.Finish(true)
	return row2.Trusted(w.BytesWritten())
}
