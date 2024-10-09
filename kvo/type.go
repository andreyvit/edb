package kvo

import (
	"fmt"
	"strconv"
	"time"
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
	IntegerStringer interface {
		IntegerValue
		fmt.Stringer
	}
)

type AnyType interface {
	Name() string
	String() string
	ValueKind() ValueKind
	Model() *Model
	ItemType() AnyType
	MapKeyType() AnyType
	MapProp(key uint64) PropImpl
	MapValueType(key uint64) AnyType
	Schema() *Schema // can return nil for generic types
	FormatValue(fc *FmtContext, value uint64) string
	// reflectType() reflect.Type
	typeCodeSet() typeCodeSet
}

type AnyScalarType interface {
	AnyType
}

type WordFormatter = func(fc *FmtContext, v uint64) string

type WordType struct {
	name      string
	codeSet   typeCodeSet
	formatter func(fc *FmtContext, v uint64) string
}

func (typ *WordType) Name() string                                { return typ.name }
func (typ *WordType) String() string                              { return typ.name }
func (typ *WordType) Schema() *Schema                             { return nil }
func (typ *WordType) ValueKind() ValueKind                        { return ValueKindWord }
func (typ *WordType) ItemType() AnyType                           { return nil }
func (typ *WordType) typeCodeSet() typeCodeSet                    { return typ.codeSet }
func (typ *WordType) Model() *Model                               { return nil }
func (typ *WordType) MapKeyType() AnyType                         { return nil }
func (typ *WordType) MapProp(key uint64) PropImpl                 { return nil }
func (typ *WordType) MapValueType(key uint64) AnyType             { return nil }
func (typ *WordType) FormatValue(fc *FmtContext, v uint64) string { return typ.formatter(fc, v) }

// func (typ *Type) ScalarConverter() ScalarConverter[T] {
// 	return typ.conv
// }

func NewScalarType[T any](name string, formatter func(fc *FmtContext, v uint64) string) *WordType {
	return &WordType{
		name:      name,
		codeSet:   allocateTypeCode(),
		formatter: formatter,
	}
}

func NewIntType[T IntegerValue](name string, formatter func(fc *FmtContext, v T) string) *WordType {
	conv := intScalarConverter[T]{}
	return NewScalarType[T](name, func(fc *FmtContext, v uint64) string {
		return formatter(fc, conv.ScalarToValue(v))
	})
}

func NewIntStringerType[T IntegerStringer](name string) *WordType {
	conv := intScalarConverter[T]{}
	return NewScalarType[T](name, func(fc *FmtContext, v uint64) string {
		return conv.ScalarToValue(v).String()
	})
}

func NewFloatType[T FloatValue](name string, formatter func(fc *FmtContext, v T) string) *WordType {
	conv := floatScalarConverter[T]{}
	return NewScalarType[T](name, func(fc *FmtContext, v uint64) string {
		return formatter(fc, conv.ScalarToValue(v))
	})
}

func NewScalarSubtype[T any](name string, base *WordType) *WordType {
	return &WordType{
		name:      name,
		codeSet:   allocateTypeCode(),
		formatter: base.formatter,
	}
}

func NewUnknownTypeWithErrorCode(errorCode string) *WordType {
	return NewScalarType[uint64](errorCode, func(fc *FmtContext, v uint64) string {
		return "0x" + strconv.FormatUint(v, 16) + "::" + errorCode
	})
}

type EntityType struct {
	name    string
	codeSet typeCodeSet
	schema  *Schema
	model   *Model
}

func NewEntityType(schema *Schema, name string) *EntityType {
	return &EntityType{
		schema:  schema,
		name:    name,
		codeSet: allocateTypeCode(),
	}
}

func (typ *EntityType) Name() string                                    { return typ.name }
func (typ *EntityType) String() string                                  { return typ.name }
func (typ *EntityType) Schema() *Schema                                 { return typ.schema }
func (typ *EntityType) ValueKind() ValueKind                            { return ValueKindMap }
func (typ *EntityType) ItemType() AnyType                               { return nil }
func (typ *EntityType) typeCodeSet() typeCodeSet                        { return typ.codeSet }
func (typ *EntityType) MapKeyType() AnyType                             { return typ.schema.TPropCode }
func (typ *EntityType) FormatValue(fc *FmtContext, value uint64) string { panic("unsupported") }

func (typ *EntityType) Model() *Model {
	if typ.model == nil {
		panic(fmt.Sprintf("no model defined for entity type %s", typ.name))
	}
	return typ.model
}

func (typ *EntityType) MapProp(key uint64) PropImpl {
	return typ.model.PropByCode(key)
}

func (typ *EntityType) MapValueType(key uint64) AnyType {
	return typ.model.MustPropByCode(key).AnyType()
}

type MapType struct {
	name     string
	keyType  AnyScalarType
	itemType AnyType
	schema   *Schema
	codeSet  typeCodeSet
}

func (typ *MapType) Name() string                                    { return typ.name }
func (typ *MapType) String() string                                  { return typ.name }
func (typ *MapType) Schema() *Schema                                 { return typ.schema }
func (typ *MapType) ValueKind() ValueKind                            { return ValueKindMap }
func (typ *MapType) ItemType() AnyType                               { return typ.itemType }
func (typ *MapType) typeCodeSet() typeCodeSet                        { return typ.codeSet }
func (typ *MapType) Model() *Model                                   { return nil }
func (typ *MapType) MapKeyType() AnyType                             { return typ.keyType }
func (typ *MapType) MapProp(key uint64) PropImpl                     { return nil }
func (typ *MapType) MapValueType(key uint64) AnyType                 { return typ.itemType }
func (typ *MapType) FormatValue(fc *FmtContext, value uint64) string { panic("unsupported") }

func Map(keyType AnyScalarType, itemType AnyType) *MapType {
	codeSet := typeCodeSet{typeCodeMap}
	codeSet.append(keyType.typeCodeSet())
	codeSet.append(itemType.typeCodeSet())

	schema := itemType.Schema()
	if schema == nil {
		schema = keyType.Schema()
	}

	return &MapType{
		name:     fmt.Sprintf("map[%s]%s", keyType.String(), itemType.Name()),
		keyType:  keyType,
		itemType: itemType,
		schema:   schema,
		codeSet:  codeSet,
	}
}

func ensureCanAccessKey(typ AnyType, key uint64) {
	if typ == nil {
		return
	}
	valueType := typ.MapValueType(key)
	if valueType == nil {
		reportCannotAccessKey(typ, key)
	}
}

func reportCannotAccessKey(typ AnyType, key uint64) {
	if schema := typ.Schema(); schema != nil {
		if prop := schema.PropByCode(key); prop != nil {
			panic(fmt.Sprintf("type %s does not allow key %s", typ.Name(), prop.Name()))
		}
	}
	panic(fmt.Sprintf("type %s does not allow key %d", typ.Name(), key))
}

var (
	TInt64 = NewIntType[int64]("int64", func(fc *FmtContext, v int64) string {
		return strconv.FormatInt(v, 10)
	})
	TUint64 = NewIntType[uint64]("uint64", func(fc *FmtContext, v uint64) string {
		return strconv.FormatUint(v, 10)
	})
	TUint64Hex = NewIntType[uint64]("uint64_hex", func(fc *FmtContext, v uint64) string {
		return "0x" + strconv.FormatUint(v, 16)
	})
	TUnknownUint64 = NewIntType[uint64]("unknown_uint64", func(fc *FmtContext, v uint64) string {
		return strconv.FormatUint(v, 10)
	})
	TUknownKey = NewIntType[uint64]("unknown_key", func(fc *FmtContext, v uint64) string {
		return "0x" + strconv.FormatUint(v, 16)
	})

	TTime = NewScalarType[time.Time]("time", func(fc *FmtContext, v uint64) string {
		return Uint64ToTime(v).Format(time.RFC3339)
	})

	TBool = NewScalarType[bool]("bool", func(fc *FmtContext, v uint64) string {
		switch v {
		case 0:
			return "false"
		case 1:
			return "true"
		default:
			return fmt.Sprintf("?bool(0x%x)", v)
		}
	})
)
