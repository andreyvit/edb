package nativebind

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// Field accesses a field of a live Go struct. The owner pointer must refer to
// the registered Go type and remain alive for the operation. Structs, arrays,
// maps and slices are borrowed by address; pointers retain their native target.
// Scalar and string values use the ordinary values.Value representation.
type Field struct {
	Name   string
	Type   reflect.Type
	Offset uintptr
	Get    func(unsafe.Pointer) values.Value
	Set    func(unsafe.Pointer, values.Value)
}

func (f *Field) Address(owner unsafe.Pointer) unsafe.Pointer {
	if owner == nil {
		panic("nativebind: nil struct")
	}
	return unsafe.Add(owner, f.Offset)
}

// AtOffset reuses this field's getter/setter inside a value-struct embedding.
// The offset is resolved at registration; generated leaf access stays generated.
func (f *Field) AtOffset(offset uintptr) *Field {
	if offset == 0 {
		return f
	}
	c := *f
	c.Offset += offset
	c.Get = func(p unsafe.Pointer) values.Value {
		if p == nil {
			panic("nativebind: nil struct")
		}
		return f.Get(unsafe.Add(p, offset))
	}
	c.Set = func(p unsafe.Pointer, v values.Value) {
		if p == nil {
			panic("nativebind: nil struct")
		}
		f.Set(unsafe.Add(p, offset), v)
	}
	return &c
}

type GeneratedField struct {
	Name string
	Get  func(unsafe.Pointer) values.Value
	Set  func(unsafe.Pointer, values.Value)
}

// GeneratedFields is emitted alongside evmbind wrappers or KVO codecs. Type
// and the complete exposed field list are checked once during registration.
type GeneratedFields struct {
	Type   reflect.Type
	New    func() unsafe.Pointer
	Fields []GeneratedField
}

// Struct is an immutable native field binding. It is independent of KVO
// property numbers, VM memory layouts and the lifetime of any one object.
type Struct struct {
	Type      reflect.Type
	Fields    []Field
	New       func() unsafe.Pointer
	Generated bool
	byName    map[string]int
}

func BindFields(t reflect.Type, generated *GeneratedFields, mode Mode) *Struct {
	if t == nil || t.Kind() != reflect.Struct {
		panic("nativebind: expected struct type")
	}
	b := &Struct{Type: t, byName: map[string]int{}}
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		typ, offset := sf.Type, sf.Offset
		b.byName[sf.Name] = len(b.Fields)
		b.Fields = append(b.Fields, Field{Name: sf.Name, Type: typ, Offset: offset,
			Get: func(p unsafe.Pointer) values.Value {
				if p == nil {
					panic("nativebind: nil struct")
				}
				return Load(typ, unsafe.Add(p, offset))
			},
			Set: func(p unsafe.Pointer, v values.Value) {
				if p == nil {
					panic("nativebind: nil struct")
				}
				Store(typ, unsafe.Add(p, offset), v)
			},
		})
	}
	compatible := generated != nil && generated.Type == t && generated.New != nil && len(generated.Fields) == len(b.Fields)
	if compatible {
		for i, f := range b.Fields {
			g := generated.Fields[i]
			if g.Name != f.Name || g.Get == nil || g.Set == nil {
				compatible = false
				break
			}
		}
	}
	// Exact native type identity checks the Go layout; the field list checks that
	// generated code covers the same public operations as reflected access.
	if mode.Select(t.String(), "fields", "fields", compatible) {
		b.Generated = true
		b.New = generated.New
		for i, g := range generated.Fields {
			b.Fields[i].Get = g.Get
			b.Fields[i].Set = g.Set
		}
	} else {
		b.New = func() unsafe.Pointer { return reflect.New(t).UnsafePointer() }
	}
	return b
}

func (b *Struct) Field(name string) *Field {
	i, ok := b.byName[name]
	if !ok {
		panic(fmt.Sprintf("nativebind: %v has no exposed field %q", b.Type, name))
	}
	return &b.Fields[i]
}

// Load and Store are the shared reflection fallback for native storage. The
// supplied pointer addresses a Go value of exactly t, never VM cell storage.
func Load(t reflect.Type, p unsafe.Pointer) values.Value {
	if p == nil {
		panic("nativebind: nil address")
	}
	v := reflect.NewAt(t, p).Elem()
	switch t.Kind() {
	case reflect.Bool:
		return values.Bool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return values.Int(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return values.Uint(v.Uint())
	case reflect.Float32:
		return values.Float32(float32(v.Float()))
	case reflect.Float64:
		return values.Float(v.Float())
	case reflect.String:
		return values.String(v.String())
	case reflect.Pointer:
		return values.Value{P: v.UnsafePointer()}
	default:
		return values.Value{P: p}
	}
}
func Store(t reflect.Type, p unsafe.Pointer, x values.Value) {
	if p == nil {
		panic("nativebind: nil address")
	}
	v := reflect.NewAt(t, p).Elem()
	switch t.Kind() {
	case reflect.Bool:
		v.SetBool(x.R != 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(int64(x.R))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		v.SetUint(x.R)
	case reflect.Float32:
		v.SetFloat(float64(math.Float32frombits(uint32(x.R))))
	case reflect.Float64:
		v.SetFloat(math.Float64frombits(x.R))
	case reflect.String:
		v.SetString(x.AsString())
	case reflect.Pointer:
		if x.P == nil {
			v.SetZero()
		} else {
			v.Set(reflect.NewAt(t.Elem(), x.P))
		}
	default:
		if x.P == nil {
			v.SetZero()
		} else {
			v.Set(reflect.NewAt(t, x.P).Elem())
		}
	}
}
