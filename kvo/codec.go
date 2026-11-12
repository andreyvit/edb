package kvo

import (
	"errors"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Binding is immutable schema metadata, reusable across transactions and
// programs. Retain configured bindings: using one publishes permanent
// representation IDs, never per-record or per-transaction metadata.
type Binding struct {
	root            *nativePlan
	generated       *Generated
	representations *representationSet
}

// Bind returns the shared default binding for a Go struct and logical model.
// Configure generated/native access once and reuse the returned binding.
func Bind(t reflect.Type, model *Model) *Binding {
	if model == nil || t == nil || t.Kind() != reflect.Struct {
		nativePanic(t, "expected a registered struct and model")
	}
	bindings.Lock()
	defer bindings.Unlock()
	key := bindingKey{t, model}
	if b := bindings.full[key]; b != nil {
		return b
	}
	b := &Binding{root: nativeCompile(t, model.Type(), make(map[nativePlanKey]*nativePlan), nil)}
	b.initRepresentations()
	bindings.full[key] = b
	return b
}

// TryEncode reports data that the declared storage policies cannot represent.
func (b *Binding) TryEncode(dst []byte, row reflect.Value, layout row2.Layout) (out []byte, err error) {
	defer func() {
		if x := recover(); x != nil {
			err = recoveredError("KVO encode", x)
		}
	}()
	return b.Encode(dst, row, layout), nil
}
func (b *Binding) Encode(dst []byte, row reflect.Value, layout row2.Layout) []byte {
	if b.generated != nil {
		if !row.CanAddr() {
			copy := reflect.New(row.Type())
			copy.Elem().Set(row)
			return b.generated.Encode(dst, copy.Interface(), layout)
		}
		return b.generated.Encode(dst, row.Addr().Interface(), layout)
	}
	w := row2.AcquireWriter(dst)
	defer w.Release()
	b.root.emit(w, row, 0, "", layout, false, 0)
	return w.BytesWritten()
}

func numericKey(v reflect.Value) uint64 {
	if nativeSignedKind(v.Kind()) {
		return uint64(v.Int())
	}
	return v.Uint()
}
func keyFits(t reflect.Type, key uint64) bool {
	if nativeSignedKind(t.Kind()) {
		return !t.OverflowInt(int64(key))
	}
	return !t.OverflowUint(key)
}

var ErrOmittedEntry = errors.New("zero map entry requires KeepDictionaryZeroValues")

func Timestamp(t time.Time) values.Value {
	n := TimeToUint64(t)
	if !Uint64ToTime(n).Equal(t) {
		panic(errors.New("KVO timestamp requires microsecond precision"))
	}
	return values.Uint(n)
}
func (p *nativePlan) nilable() bool {
	return p.typ.Kind() == reflect.Pointer || p.typ.Kind() == reflect.Map || p.typ.Kind() == reflect.Slice
}
func (p *nativePlan) requirePresent(v reflect.Value) {
	if p.nilable() && v.IsNil() {
		panic(row2.ErrValue)
	}
}
func (p *nativePlan) emit(w *row2.Writer, v reflect.Value, key uint64, text string, layout row2.Layout, keep bool, depth int) {
	if depth > 256 {
		panic(row2.ErrLimit)
	}
	if v.Type() != p.typ {
		panic(row2.ErrType)
	}
	var value values.Value
	switch p.kind {
	case nativeOption:
		if !v.Field(1).Bool() {
			return
		}
		p.elem.requirePresent(v.Field(0))
		p.elem.emit(w, v.Field(0), key, text, layout, true, depth+1)
		return
	case nativePointer:
		if !v.IsNil() {
			p.elem.requirePresent(v.Elem())
			p.elem.emit(w, v.Elem(), key, text, layout, true, depth+1)
			return
		}
	case nativeSigned:
		value = values.Int(v.Int())
	case nativeUnsigned:
		value = values.Uint(v.Uint())
	case nativeBool:
		if v.Bool() {
			value = values.Uint(1)
		}
	case nativeTime:
		value = Timestamp(v.Interface().(time.Time))
	case nativeFloat:
		if p.typ.Kind() == reflect.Float32 {
			value = values.Float32(float32(v.Float()))
		} else {
			value = values.Float(v.Float())
		}
	case nativeBytes:
		value = values.Bytes(v.Bytes())
		keep = keep || !v.IsNil()
	case nativeTimestamp:
		value = values.Time(nativeTimestampValue(v))
	case nativeJSON:
		value = JSONValue(v.Interface())
	case nativeString:
		value = values.String(v.String())
	case nativeObject:
		w.Begin(key, text, row2.Object, layout)
		for _, f := range p.fields {
			childLayout := layout
			if f.layout == row2.Compact2 {
				childLayout = row2.Compact2
			}
			f.plan.emit(w, v.FieldByIndex(f.path), f.key, "", childLayout, f.significant, depth+1)
		}
		w.Finish(keep)
		return
	case nativeList:
		w.Begin(key, text, row2.List, layout)
		for i := 0; i < v.Len(); i++ {
			p.elem.requirePresent(v.Index(i))
			p.elem.emit(w, v.Index(i), 0, "", layout, true, depth+1)
		}
		w.Finish(keep || v.Kind() == reflect.Slice && !v.IsNil())
		return
	case nativeDictionary, nativeNumbers:
		kind := row2.Object
		if p.kind == nativeDictionary {
			kind = row2.Dictionary
		}
		w.Begin(key, text, kind, layout)
		keys := v.MapKeys()
		if kind == row2.Dictionary {
			slices.SortFunc(keys, func(a, b reflect.Value) int { return strings.Compare(a.String(), b.String()) })
		} else {
			slices.SortFunc(keys, func(a, b reflect.Value) int {
				if numericKey(a) < numericKey(b) {
					return -1
				}
				if numericKey(a) > numericKey(b) {
					return 1
				}
				return 0
			})
		}
		for _, k := range keys {
			var num uint64
			var name string
			if kind == row2.Dictionary {
				name = k.String()
			} else {
				num = numericKey(k)
			}
			x := v.MapIndex(k)
			p.elem.requirePresent(x)
			p.elem.emit(w, x, num, name, layout, p.keepDictionaryZeros, depth+1)
		}
		if w.Count() != v.Len() {
			panic(ErrOmittedEntry)
		}
		w.Finish(keep || !v.IsNil())
		return
	}
	if keep {
		w.PutPresent(key, text, value, p.encoding())
	} else {
		w.Put(key, text, value, p.encoding())
	}
}

// Decode panics on error. Use TryDecode to handle errors explicitly.
func (b *Binding) Decode(raw []byte, dst reflect.Value) {
	err := b.TryDecode(raw, dst)
	if err != nil {
		panic(err)
	}
}

// TryDecode materializes owned Go values. Validation follows the model into each child.
func (b *Binding) TryDecode(raw []byte, dst reflect.Value) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = recoveredError("KVO decode", x)
		}
	}()
	if len(raw) == 0 || dst.Type() != b.root.typ {
		return row2.ErrType
	}
	dst.SetZero()
	if b.generated != nil {
		b.generated.Decode(raw, dst.Addr().Interface())
		return nil
	}
	b.root.read(dst, values.Bytes(raw), true, 0)
	return nil
}
func (p *nativePlan) read(dst reflect.Value, v values.Value, present bool, depth int) {
	if depth > 256 {
		panic(row2.ErrLimit)
	}
	switch p.kind {
	case nativeOption:
		dst.Field(1).SetBool(present)
		p.elem.read(dst.Field(0), v, present, depth+1)
	case nativePointer:
		if present {
			dst.Set(reflect.New(dst.Type().Elem()))
			p.elem.read(dst.Elem(), v, true, depth+1)
		}
	case nativeSigned:
		n := v.AsInt()
		if dst.OverflowInt(n) {
			panic(row2.ErrValue)
		}
		dst.SetInt(n)
	case nativeUnsigned:
		n := v.AsUint()
		if dst.OverflowUint(n) {
			panic(row2.ErrValue)
		}
		dst.SetUint(n)
	case nativeBool:
		dst.SetBool(v.AsBool())
	case nativeTime:
		dst.Set(reflect.ValueOf(Uint64ToTime(v.AsUint()).UTC()))
	case nativeFloat:
		n := v.AsFloat()
		if p.typ.Kind() == reflect.Float32 {
			n = float64(v.AsFloat32())
		}
		dst.SetFloat(n)
		if dst.Float() != n && !math.IsNaN(n) {
			panic(row2.ErrValue)
		}
	case nativeTimestamp:
		value := reflect.ValueOf(v.AsTime())
		if value.Type() != p.typ {
			value = value.Convert(p.typ)
		}
		dst.Set(value)
	case nativeJSON:
		DecodeJSON(v, dst.Addr().Interface())
	case nativeString:
		dst.SetString(string(v.AsBytes()))
	case nativeBytes:
		if present {
			dst.SetBytes(append([]byte{}, v.AsBytes()...))
		}
	default:
		view := p.openContainer(v)
		if p.kind == nativeList {
			if dst.Kind() == reflect.Slice {
				if present {
					dst.Set(reflect.MakeSlice(dst.Type(), view.Len(), view.Len()))
				}
			} else if present && view.Len() != dst.Len() {
				panic(row2.ErrValue)
			}
		} else if dst.Kind() == reflect.Map && present {
			dst.Set(reflect.MakeMapWithSize(dst.Type(), view.Len()))
		}
		it := view.Iter()
		for {
			k, name, x, ok := it.Next()
			if !ok {
				break
			}
			switch p.kind {
			case nativeObject:
				f := p.field(k)
				f.plan.read(dst.FieldByIndex(f.path), x.Decode(f.plan.encoding()), true, depth+1)
			case nativeList:
				p.elem.read(dst.Index(int(k)), x.Decode(p.elem.encoding()), true, depth+1)
			default:
				key := reflect.New(dst.Type().Key()).Elem()
				if p.kind == nativeDictionary {
					key.SetString(string(name))
				} else {
					if !keyFits(key.Type(), k) {
						panic(row2.ErrValue)
					}
					if nativeSignedKind(key.Kind()) {
						key.SetInt(int64(k))
					} else {
						key.SetUint(k)
					}
				}
				val := reflect.New(p.elem.typ).Elem()
				p.elem.read(val, x.Decode(p.elem.encoding()), true, depth+1)
				dst.SetMapIndex(key, val)
			}
		}
	}
}

func (p *nativePlan) openContainer(v values.Value) row2.View {
	view, err := row2.TryOpen(v.AsBytes())
	if err != nil {
		panic(err)
	}
	kind := row2.Object
	if p.kind == nativeList {
		kind = row2.List
	} else if p.kind == nativeDictionary {
		kind = row2.Dictionary
	}
	if !view.Missing() && view.Kind() != kind {
		panic(row2.ErrKind)
	}
	return view
}
