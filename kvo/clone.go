package kvo

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/kvo/codec/row2"
)

// Clone returns an independent snapshot, including when the source already
// owns its storage. Encoded records keep their codec, binding, and unknown
// fields. Native records keep their Go type and copy their data graph, including
// fields outside the model. Maps, slices, pointers, interfaces, and strings are
// detached. Non-nil functions, channels, and unsafe pointers cannot be cloned
// and panic. No serialization or user codec is invoked for native objects.
//
// Use Own for retention without an independent mutable snapshot. Fastest is an
// access hint and can retain an existing native or decoded object unchanged.
func (r Record) Clone() Record {
	if r.Missing() {
		return r
	}
	e := r.representation()
	switch e.kind {
	case nativeRepresentation:
		source := reflect.NewAt(e.plan.typ, r.value.P)
		copy := (&nativeCloner{seen: make(map[cloneAddress]reflect.Value)}).value(source, 0)
		return e.set.value(nativeRepresentation, copy.UnsafePointer(), 0)
	case memoryRepresentation:
		v := row2.Trusted(bytes.Clone(r.Snapshot().Raw()))
		return recordView{memory: decodeMemory(v, e.layout, 0), layout: e.layout}.compact()
	default:
		data := bytes.Clone(unsafe.Slice((*byte)(r.value.P), r.value.Auxiliary()))
		return spanRecord(e.set, e.kind, data).markOwned()
	}
}

type cloneAddress struct {
	typ              reflect.Type
	ptr              unsafe.Pointer
	length, capacity int
}
type nativeCloner struct {
	seen map[cloneAddress]reflect.Value
}

// Addressable copies let reflection traverse private data fields too. This is
// used only at the explicit clone boundary, never in normal property access.
func cloneField(v reflect.Value) reflect.Value {
	if v.CanInterface() {
		return v
	}
	return reflect.NewAt(v.Type(), v.Addr().UnsafePointer()).Elem()
}
func (c *nativeCloner) value(v reflect.Value, depth int) reflect.Value {
	if depth > 256 {
		panic("record: native clone exceeds 256 levels")
	}
	v = cloneField(v)
	if v.Type() == reflect.TypeFor[time.Time]() {
		// time.Time is a value; its Location graph is immutable public metadata.
		return v
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		key := cloneAddress{typ: v.Type(), ptr: v.UnsafePointer()}
		if old, ok := c.seen[key]; ok {
			return old
		}
		out := reflect.New(v.Type().Elem())
		c.seen[key] = out
		out.Elem().Set(c.value(v.Elem(), depth+1))
		return out
	case reflect.Interface:
		out := reflect.New(v.Type()).Elem()
		if !v.IsNil() {
			out.Set(c.value(v.Elem(), depth+1))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		key := cloneAddress{typ: v.Type(), ptr: v.UnsafePointer()}
		if old, ok := c.seen[key]; ok {
			return old
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		c.seen[key] = out
		it := v.MapRange()
		for it.Next() {
			out.SetMapIndex(c.value(it.Key(), depth+1), c.value(it.Value(), depth+1))
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		key := cloneAddress{v.Type(), v.UnsafePointer(), v.Len(), v.Cap()}
		if old, ok := c.seen[key]; ok {
			return old
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Cap())
		c.seen[key] = out
		// Capacity is observable by reslicing; detach those elements as well.
		src, dst := v.Slice(0, v.Cap()), out.Slice(0, out.Cap())
		for i := range src.Len() {
			dst.Index(i).Set(c.value(src.Index(i), depth+1))
		}
		return out
	case reflect.Struct, reflect.Array:
		if !v.CanAddr() {
			copy := reflect.New(v.Type()).Elem()
			copy.Set(v)
			v = copy
		}
		out := reflect.New(v.Type()).Elem()
		if v.Kind() == reflect.Array {
			for i := range v.Len() {
				out.Index(i).Set(c.value(v.Index(i), depth+1))
			}
		} else {
			for i := range v.NumField() {
				cloneField(out.Field(i)).Set(c.value(v.Field(i), depth+1))
			}
		}
		return out
	case reflect.String:
		out := reflect.New(v.Type()).Elem()
		out.SetString(strings.Clone(v.String()))
		return out
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		if !v.IsNil() {
			panic(fmt.Sprintf("record: cannot clone native %s", v.Type()))
		}
	}
	return v
}

type collectionCopy uint8

const (
	borrowElements collectionCopy = iota
	ownElements
	cloneElements
)

func (c collectionCopy) String() string {
	switch c {
	case borrowElements:
		return "borrow"
	case ownElements:
		return "own"
	case cloneElements:
		return "clone"
	default:
		return "invalid"
	}
}
