package kvo

import (
	"encoding/json"
	"math"
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Validate panics on error. Use TryValidate to handle errors explicitly.
func (b *Binding) Validate(raw []byte) {
	err := b.TryValidate(raw)
	if err != nil {
		panic(err)
	}
}

// TryValidate checks that Decode can materialize a row without allocating the Go
// graph. Packed writers use this before publishing bytes supplied by a caller.
func (b *Binding) TryValidate(raw []byte) (err error) {
	defer func() {
		if x := recover(); x != nil {
			err = recoveredError("KVO validate", x)
		}
	}()
	if len(raw) == 0 {
		return row2.ErrType
	}
	b.root.validate(values.Bytes(raw), 0)
	return nil
}

func (p *nativePlan) validate(x values.Value, depth int) {
	if depth > 256 {
		panic(row2.ErrLimit)
	}
	switch p.kind {
	case nativeOption, nativePointer:
		p.elem.validate(x, depth+1)
	case nativeSigned:
		if p.typ.OverflowInt(x.AsInt()) {
			panic(row2.ErrValue)
		}
	case nativeUnsigned:
		if p.typ.OverflowUint(x.AsUint()) {
			panic(row2.ErrValue)
		}
	case nativeBool:
		x.AsBool()
	case nativeTime:
		x.AsUint()
	case nativeFloat:
		n := x.AsFloat()
		if p.typ.Kind() == reflect.Float32 {
			n = float64(x.AsFloat32())
		}
		if p.typ.Kind() == reflect.Float32 && !math.IsNaN(n) && float64(float32(n)) != n {
			panic(row2.ErrValue)
		}
	case nativeTimestamp:
		x.AsTime()
	case nativeJSON:
		if b := x.AsBytes(); len(b) != 0 && !json.Valid(b) {
			panic(row2.ErrValue)
		}
	case nativeString, nativeBytes:
		x.AsBytes()
	case nativeObject, nativeList, nativeDictionary, nativeNumbers:
		view := p.openContainer(x)
		if p.typ.Kind() == reflect.Array && view.Len() != p.typ.Len() {
			panic(row2.ErrValue)
		}
		it := view.Iter()
		for {
			k, _, value, ok := it.Next()
			if !ok {
				break
			}
			child := p.elem
			if p.kind == nativeObject {
				child = p.field(k).plan
			} else if p.kind == nativeNumbers && !keyFits(p.typ.Key(), k) {
				panic(row2.ErrValue)
			}
			child.validate(value.Decode(child.encoding()), depth+1)
		}
	default:
		panic(row2.ErrType)
	}
}
