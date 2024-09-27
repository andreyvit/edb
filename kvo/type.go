package kvo

import (
	"fmt"
	"reflect"
	"sync"
)

type typeCodeSet [8]uint16

var lastTypeCode uint16 = 9 // 0..9 are reserved

const (
	typeCodeNone = 0
	typeCodeMap  = 1
)

func allocateTypeCode() typeCodeSet {
	lastTypeCode++
	return typeCodeSet{lastTypeCode}
}

func (cs *typeCodeSet) len() int {
	for i, c := range cs {
		if c == 0 {
			return i
		}
	}
	return len(cs)
}

func (cs *typeCodeSet) append(peer typeCodeSet) {
	i := cs.len()
	for _, c := range peer {
		if c == 0 {
			break
		}
		cs[i] = c // a crash here means typeCodeSet length needs to be increased
		i++
	}
}

type (
	IntegerValue interface {
		~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
	}
	FloatValue interface {
		float32 | ~float64
	}
)

type AnyType interface {
	Name() string
	ValueKind() ValueKind
	Model() *Model
	ItemType() AnyType
	// reflectType() reflect.Type
	typeCodeSet() typeCodeSet
}

type AnyScalarType interface {
	AnyType
}

type IntegerType struct {
	name    string
	rtype   reflect.Type
	codeSet typeCodeSet
}

func (typ *IntegerType) Name() string             { return typ.name }
func (typ *IntegerType) ValueKind() ValueKind     { return ValueKindInteger }
func (typ *IntegerType) ItemType() AnyType        { return nil }
func (typ *IntegerType) typeCodeSet() typeCodeSet { return typ.codeSet }
func (typ *IntegerType) Model() *Model            { return nil }

// func (typ *Type) ScalarConverter() ScalarConverter[T] {
// 	return typ.conv
// }

func (typ *IntegerType) reflectType() reflect.Type {
	return typ.rtype
}

func NewScalarType[T any](name string, conv ScalarConverter[T]) *IntegerType {
	return &IntegerType{
		name:  name,
		rtype: reflect.TypeFor[T](),
		// conv:  conv,
		codeSet: allocateTypeCode(),
	}
}

func NewIntType[T IntegerValue](name string) *IntegerType {
	return NewScalarType(name, intScalarConverter[T]{})
}

func NewFloatType[T FloatValue](name string) *IntegerType {
	return NewScalarType(name, floatScalarConverter[T]{})
}

func NewScalarSubtype[T any](name string, base *IntegerType) *IntegerType {
	return &IntegerType{
		name:  name,
		rtype: reflect.TypeFor[T](),
		// conv:  base.conv,
	}
}

type EntityType struct {
	name    string
	codeSet typeCodeSet
	schema  *Schema
	model   *Model
	once    sync.Once
}

func NewEntityType(schema *Schema, name string) *EntityType {
	return &EntityType{
		schema:  schema,
		name:    name,
		codeSet: allocateTypeCode(),
	}
}

func (typ *EntityType) Name() string             { return typ.name }
func (typ *EntityType) ValueKind() ValueKind     { return ValueKindSubobject }
func (typ *EntityType) ItemType() AnyType        { return nil }
func (typ *EntityType) typeCodeSet() typeCodeSet { return typ.codeSet }

func (typ *EntityType) Model() *Model {
	// this delays until schema is finished initializing
	typ.once.Do(func() {
		typ.model = typ.schema.modelsByTypeCodeSet[typ.codeSet]
	})
	return typ.model
}

type MapType struct {
	name     string
	keyType  AnyScalarType
	itemType AnyType
	codeSet  typeCodeSet
}

func (typ *MapType) Name() string             { return typ.name }
func (typ *MapType) ValueKind() ValueKind     { return ValueKindSubobject }
func (typ *MapType) ItemType() AnyType        { return typ.itemType }
func (typ *MapType) typeCodeSet() typeCodeSet { return typ.codeSet }
func (typ *MapType) Model() *Model            { return nil }

func Map(keyType AnyScalarType, itemType AnyType) *MapType {
	codeSet := typeCodeSet{typeCodeMap}
	codeSet.append(keyType.typeCodeSet())
	codeSet.append(itemType.typeCodeSet())
	return &MapType{
		name:     fmt.Sprintf("map[%s]%s", keyType.Name(), itemType.Name()),
		keyType:  keyType,
		itemType: itemType,
		codeSet:  codeSet,
	}
}

var (
	Int64  = NewIntType[int64]("int64")
	Uint64 = NewIntType[uint64]("uint64")
)
