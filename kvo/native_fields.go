package kvo

import (
	"math"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

// NativeSchema is implemented by evmbind.Schema. It connects a KVO projection
// to the same field operations and nominal type used by EVM host bindings.
type NativeSchema interface {
	NativeFields(reflect.Type) *nativebind.Struct
	NativeType(reflect.Type) *values.Type
}

func withNativeFields(root *nativePlan, bind func(reflect.Type, *nativebind.Struct, *values.Type) (*nativebind.Struct, *values.Type)) *nativePlan {
	seen := make(map[*nativePlan]*nativePlan)
	var clone func(*nativePlan) *nativePlan
	clone = func(p *nativePlan) *nativePlan {
		if p == nil {
			return nil
		}
		if c := seen[p]; c != nil {
			return c
		}
		c := *p
		c.representations = &representationSet{plan: &c}
		seen[p] = &c
		c.native, c.valueType = bind(p.typ, p.native, p.valueType)
		c.inlined = slices.Clone(p.inlined)
		for i, fields := range c.inlined {
			c.inlined[i], _ = bind(fields.Type, fields, nil)
		}
		c.fields = slices.Clone(p.fields)
		for i := range c.fields {
			f := &c.fields[i]
			f.plan = clone(f.plan)
			f.access = c.fieldAccess(f.path)
		}
		c.elem = clone(p.elem)
		return &c
	}
	return clone(root)
}
func bindSchema(root *nativePlan, s NativeSchema) *nativePlan {
	if s.NativeFields(root.typ) == nil || s.NativeType(root.typ) == nil {
		panic("record: native schema does not bind root struct")
	}
	return withNativeFields(root, func(t reflect.Type, fields *nativebind.Struct, valueType *values.Type) (*nativebind.Struct, *values.Type) {
		if f := s.NativeFields(t); f != nil {
			if f.Type != t {
				panic("record: native schema type mismatch")
			}
			return f, s.NativeType(t)
		}
		return fields, valueType
	})
}
func (b Binding) WithNativeSchema(s NativeSchema) *Binding {
	b.root = bindSchema(b.root, s)
	b.initRepresentations()
	return &b
}
func (p Projection) WithNativeSchema(s NativeSchema) *Projection {
	p.root = bindSchema(p.root, s)
	return &p
}

// NativeType is the attached values type, or nil without a native binding.
func (r recordView) NativeType() *values.Type {
	if r.plan == nil {
		return nil
	}
	return r.plan.valueType
}

// Native returns the live Go struct pointer. Encoded records are decoded into
// an independent object first; retain Fastest's result when reusing that object.
// A record without a native binding returns nil.
func (r recordView) Native() any {
	if r.plan == nil || r.Missing() {
		return nil
	}
	r = r.Fastest()
	return reflect.NewAt(r.plan.typ, r.object).Interface()
}
func (r recordView) objectValue() reflect.Value { return reflect.NewAt(r.plan.typ, r.object).Elem() }

// unwrapValue consumes the shared binding carrier, preserving borrowed native
// aggregate addresses and pointer identity without reflecting over fields.
func unwrapValue(p *nativePlan, v values.Value) (*nativePlan, values.Value, bool) {
	for {
		switch p.kind {
		case nativePointer:
			if v.P == nil {
				return p.elem, values.Value{}, false
			}
			p = p.elem
			v = nativebind.Load(p.typ, v.P)
		case nativeOption:
			if p.native.Field("Present").Get(v.P).R == 0 {
				return p.elem, values.Value{}, false
			}
			v = p.native.Field("Value").Get(v.P)
			p = p.elem
		default:
			return p, v, true
		}
	}
}
func nativeFieldValue(p *nativePlan, v values.Value, layout row2.Layout) values.Value {
	var ok bool
	p, v, ok = unwrapValue(p, v)
	if !ok {
		return values.Value{}
	}
	switch p.kind {
	case nativeSigned:
		return values.Int(v.AsInt())
	case nativeUnsigned, nativeBool:
		return values.Uint(v.R)
	case nativeFloat, nativeString:
		return v
	case nativeTime:
		return values.Uint(TimeToUint64(*(*time.Time)(v.P)))
	case nativeTimestamp:
		return values.Time(*(*time.Time)(v.P))
	default:
		x := reflect.NewAt(p.typ, v.P).Elem()
		if p.nilable() && x.IsNil() {
			return values.Value{}
		}
		return nativeValue(p, x, layout)
	}
}
func nativeFieldHas(p *nativePlan, v values.Value) bool {
	switch p.kind {
	case nativeTime, nativeTimestamp:
		return !(*time.Time)(v.P).IsZero()
	case nativePointer:
		return v.P != nil
	case nativeOption:
		return p.native.Field("Present").Get(v.P).R != 0
	case nativeObject:
		return true
	case nativeSigned, nativeUnsigned, nativeBool, nativeString:
		return v.R != 0
	case nativeFloat:
		if p.typ.Kind() == reflect.Float32 {
			return v.AsFloat32() != 0
		}
		return v.AsFloat() != 0
	default:
		return nativeHas(p, reflect.NewAt(p.typ, v.P).Elem())
	}
}

func scalarAssignment(p *nativePlan, x values.Value) (values.Value, bool) {
	switch p.kind {
	case nativeSigned:
		n := x.AsInt()
		if p.typ.OverflowInt(n) {
			panic(row2.ErrValue)
		}
		return values.Int(n), true
	case nativeUnsigned:
		n := x.AsUint()
		if p.typ.OverflowUint(n) {
			panic(row2.ErrValue)
		}
		return values.Uint(n), true
	case nativeBool:
		return values.Bool(x.AsBool()), true
	case nativeFloat:
		n := x.AsFloat()
		if p.typ.Kind() == reflect.Float32 {
			n = float64(x.AsFloat32())
		}
		if p.typ.Kind() == reflect.Float32 {
			f := float32(n)
			if float64(f) != n && !math.IsNaN(n) {
				panic(row2.ErrValue)
			}
			return values.Float32(f), true
		}
		return values.Float(n), true
	case nativeString:
		return x, true
	default:
		return values.Value{}, false
	}
}

// Keep a pointer-shaped carrier for non-scalar assignments after conversion.
func nativeAssignment(p *nativePlan, x reflect.Value) values.Value {
	return nativebind.Load(p.typ, x.Addr().UnsafePointer())
}

func storeAssigned(p *nativePlan, dst reflect.Value, v values.Value) {
	nativebind.Store(p.typ, dst.Addr().UnsafePointer(), v)
}

func unwrapForEdit(p *nativePlan, v values.Value, address unsafe.Pointer, create bool) (*nativePlan, values.Value, bool) {
	for {
		switch p.kind {
		case nativePointer:
			if v.P == nil {
				if !create {
					return p.elem, values.Value{}, false
				}
				v.P = reflect.New(p.elem.typ).UnsafePointer()
				nativebind.Store(p.typ, address, v)
			}
			address = v.P
			p = p.elem
			v = nativebind.Load(p.typ, address)
		case nativeOption:
			present := p.native.Field("Present")
			if present.Get(v.P).R == 0 {
				if !create {
					return p.elem, values.Value{}, false
				}
				present.Set(v.P, values.Bool(true))
			}
			field := p.native.Field("Value")
			address = field.Address(v.P)
			v = field.Get(v.P)
			p = p.elem
		default:
			return p, v, true
		}
	}
}

func (p *nativePlan) indexFields() {
	if len(p.fields) == 0 {
		return
	}
	p.base = p.fields[0].key
	span := p.fields[len(p.fields)-1].key - p.base
	// A bounded lookup also covers sparse application-wide property codes.
	// Keep large external codes on the binary-search path.
	if span < 4096 {
		p.index = make([]uint16, span+1)
		for i, f := range p.fields {
			p.index[f.key-p.base] = uint16(i + 1)
		}
	}
}

func (p *nativePlan) fieldBinding(t reflect.Type) *nativebind.Struct {
	if t == p.typ {
		return p.native
	}
	for _, b := range p.inlined {
		if b.Type == t {
			return b
		}
	}
	return nil
}
func (p *nativePlan) fieldAccess(path []int) *nativebind.Field {
	t := p.typ
	var offset uintptr
	for _, i := range path[:len(path)-1] {
		f := t.Field(i)
		offset += f.Offset
		t = f.Type
	}
	return p.fieldBinding(t).Field(t.Field(path[len(path)-1]).Name).AtOffset(offset)
}
