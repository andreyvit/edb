// Package values defines two-word values shared by KVO adapters and EVM.
// Concrete types and presence are supplied separately; interface values encode
// their dynamic type using a program-local TypeTable.
package values

import (
	"math"
	"unsafe"
)

type Value struct {
	R uint64
	P unsafe.Pointer
}

func Int(v int64) Value { return Value{R: uint64(v)} }

func Uint(v uint64) Value          { return Value{R: v} }
func Float32(v float32) Value      { return Uint(uint64(math.Float32bits(v))) }
func (v Value) AsFloat32() float32 { return math.Float32frombits(uint32(v.R)) }
func Float(v float64) Value        { return Value{R: math.Float64bits(v)} }
func Bool(v bool) Value {
	if v {
		return Value{R: 1}
	}
	return Value{}
}

// String retains the immutable string data without allocating a wrapper.
func String(v string) Value {
	return Value{R: uint64(len(v)), P: unsafe.Pointer(unsafe.StringData(v))}
}

// AsString requires StringType. The data must remain valid and immutable.
func (v Value) AsString() string { return unsafe.String((*byte)(v.P), v.R) }
func (v Value) AsInt() int64     { return int64(v.R) }
func (v Value) AsFloat() float64 { return math.Float64frombits(v.R) }

type Repr uint8

const (
	Raw     Repr = 1
	Pointer Repr = 2
	Pair         = Raw | Pointer
)

// Type is external metadata. Identity is by pointer, not by name.
type Type struct {
	name string
	repr Repr
	own  OwnFunc
}

func NewType(name string, repr Repr) *Type {
	if name == "" || repr < Raw || repr > Pair {
		panic("invalid value type")
	}
	return &Type{name: name, repr: repr}
}
func (t *Type) Name() string { return t.name }
func (t *Type) Repr() Repr   { return t.repr }

var (
	Int64      = NewType("i64", Raw)
	Uint64     = NewType("u64", Raw)
	Float64    = NewType("f64", Raw)
	Boolean    = NewType("bool", Raw)
	StringType = NewOwnedType("string", Pair, ownString)
	AnyType    = NewType("any", Pair)
	ErrorType  = NewType("error", Pair)
)

// Bytes borrows an immutable byte span. R is its byte length, unlike a mutable
// VM slice whose header packs length and capacity. The external type distinguishes
// byte spans, strings and VM slices. The backing bytes must outlive this value.
func Bytes(b []byte) Value      { return Value{R: uint64(len(b)), P: unsafe.Pointer(unsafe.SliceData(b))} }
func (v Value) AsBytes() []byte { return unsafe.Slice((*byte)(v.P), int(v.R)) }
func (v Value) AsUint() uint64  { return v.R }
func (v Value) AsBool() bool {
	if v.R > 1 {
		panic("invalid boolean value")
	}
	return v.R != 0
}
