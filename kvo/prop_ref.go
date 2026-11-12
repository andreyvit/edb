package kvo

import (
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// PropRef is a shared, typed access descriptor for one declared property.
// It is constructed once, never per record. Its result type can be a scalar
// or a borrowed view; it does not imply decoding the owning record.
type PropRef[T any] struct {
	code   uint64
	decode func(values.Value) T
	encode func(T) values.Value
	view   func(Record, uint64) T
}

func (p *PropRef[T]) Code() uint64 { return p.code }

// BindProp attaches reflection-free scalar conversion to an existing property.
// Type compatibility is checked here, never on the hot access path.
func BindProp[T any](schema *Schema, code uint64, codec values.Codec[T]) *PropRef[T] {
	prop := schema.MustPropByCode(code)
	checkCodec(prop.AnyType(), codec)
	return &PropRef[T]{code: code, decode: codec.Decode, encode: codec.Encode}
}

// NewScalarProp declares a property and returns its typed access descriptor.
func NewScalarProp[T any](schema *Schema, code uint64, name string, typ AnyType, codec values.Codec[T]) *PropRef[T] {
	checkCodec(typ, codec)
	NewProp(schema, code, name, typ, nil)
	return BindProp(schema, code, codec)
}

func checkCodec[T any](typ AnyType, codec values.Codec[T]) {
	native, ok := typ.(interface{ NativeType() reflect.Type })
	if !ok || codec.Type != reflect.TypeFor[T]() || codec.Decode == nil || codec.Encode == nil || native.NativeType().Kind() != codec.Type.Kind() {
		panic(fmt.Sprintf("record: codec for %v does not match property type %s", reflect.TypeFor[T](), typ.Name()))
	}
	// A []byte carrier is not a general Go slice representation.
	if codec.Type.Kind() == reflect.Slice && codec.Type.Elem().Kind() != reflect.Uint8 {
		panic("record: scalar slice codec must contain bytes")
	}
}

// Property is implemented by typed codes and shared access descriptors.
// Generic methods keep the concrete property type; they do not box it.
type Property[T any] interface {
	Code() uint64
	Get(Record) T
	readEditor(*Editor) T
	toValue(T) values.Value
}

func (p *PropRef[T]) Get(r Record) T {
	if p.view != nil {
		return p.view(r, p.code)
	}
	return p.decode(r.GetValue(p.code))
}
func (p *PropRef[T]) readEditor(e *Editor) T {
	if p.view != nil {
		r := e.currentView()
		return p.view(r.compact(), p.code)
	}
	return p.decode(e.GetValue(p.code))
}
func (p *PropRef[T]) toValue(v T) values.Value    { return p.encode(v) }
func (r Record) Get[T any, P Property[T]](p P) T  { return p.Get(r) }
func (e *Editor) Get[T any, P Property[T]](p P) T { return p.readEditor(e) }
func (e *Editor) Set[T any, P Property[T]](p P, value T) {
	e.SetValue(p.Code(), p.toValue(value))
}
func (e *Editor) TrySet[T any, P Property[T]](p P, value T) (err error) {
	defer func() {
		if ex := recover(); ex != nil {
			err = recoveredError("property assignment", ex)
		}
	}()
	return e.TrySetValue(p.Code(), p.toValue(value))
}

// BindRecordProp returns child records as views, without decoding their structs.
func BindRecordProp(schema *Schema, code uint64) *PropRef[Record] {
	entity, ok := schema.MustPropByCode(code).AnyType().(*EntityType)
	if !ok {
		panic("record: child property must have an entity type")
	}
	return &PropRef[Record]{code: code, view: func(r Record, k uint64) Record { return r.Child(row2.Step{Key: k}) }, encode: func(r Record) values.Value {
		if !r.Missing() && r.Model() != entity.Model() {
			panic("record: wrong child model")
		}
		return values.Bytes(r.Snapshot().Raw())
	}}
}
func NewRecordProp(schema *Schema, code uint64, name string, typ AnyType) *PropRef[Record] {
	if _, ok := typ.(*EntityType); !ok {
		panic("record: child property must have an entity type")
	}
	NewProp(schema, code, name, typ, nil)
	return BindRecordProp(schema, code)
}

// ValuePropRef keeps the shared carrier for properties whose access type is
// still an encoded aggregate or dynamic value. Typed collection traversal is
// available for lists; maps and arbitrary JSON retain their raw API semantics.
type ValuePropRef struct{ code uint64 }

func (p ValuePropRef) Code() uint64              { return p.code }
func (p ValuePropRef) Get(r Record) values.Value { return r.GetValue(p.code) }
func (p ValuePropRef) readEditor(e *Editor) values.Value {
	return e.GetValue(p.code)
}
func (p ValuePropRef) toValue(v values.Value) values.Value { return v }
func BindValueProp(schema *Schema, code uint64) ValuePropRef {
	schema.MustPropByCode(code)
	return ValuePropRef{code}
}
func NewValueProp(schema *Schema, code uint64, name string, typ AnyType) ValuePropRef {
	NewProp(schema, code, name, typ, nil)
	return ValuePropRef{code}
}
