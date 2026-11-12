package kvo

import (
	"reflect"
	"time"

	"github.com/andreyvit/edb/values"
)

// TimePropRef returns time.Time directly, without parsing or reflection.
type TimePropRef struct{ code uint64 }

func (p TimePropRef) Code() uint64           { return p.code }
func (p TimePropRef) Get(r Record) time.Time { return r.GetValue(p.code).AsTime() }
func (p TimePropRef) readEditor(e *Editor) time.Time {
	return e.GetValue(p.code).AsTime()
}
func (p TimePropRef) toValue(t time.Time) values.Value { return values.Time(t) }
func NewTimeProp(schema *Schema, code uint64, name string) TimePropRef {
	NewProp(schema, code, name, TTimestamp, nil)
	return TimePropRef{code}
}

// LegacyTimePropRef retains TTime's offset-microsecond word encoding.
type LegacyTimePropRef struct{ code uint64 }

func (p LegacyTimePropRef) Code() uint64                { return p.code }
func (p LegacyTimePropRef) FromWord(v uint64) time.Time { return Uint64ToTime(v) }
func (p LegacyTimePropRef) ToWord(v time.Time) uint64   { return TimeToUint64(v) }
func (p LegacyTimePropRef) Get(r Record) time.Time      { return p.FromWord(r.GetValue(p.code).AsUint()) }
func (p LegacyTimePropRef) readEditor(e *Editor) time.Time {
	return p.FromWord(e.GetValue(p.code).AsUint())
}
func (p LegacyTimePropRef) toValue(v time.Time) values.Value { return values.Uint(p.ToWord(v)) }
func NewLegacyTimeProp(schema *Schema, code uint64, name string) LegacyTimePropRef {
	NewProp(schema, code, name, TTime, nil)
	return LegacyTimePropRef{code}
}

// LegacyTimeCodec preserves TTime's offset-microsecond word representation.
// New persistent timestamps should use NewTimeProp and TTimestamp instead.
func LegacyTimeCodec() values.Codec[time.Time] {
	return values.Codec[time.Time]{
		Type:   reflect.TypeFor[time.Time](),
		Decode: func(v values.Value) time.Time { return Uint64ToTime(v.AsUint()) },
		Encode: func(t time.Time) values.Value { return values.Uint(TimeToUint64(t)) },
	}
}

// Named time types share time.Time's layout. Their custom MessagePack decoders
// remain in force; conversion here concerns the decoded native value only.
func nativeTimestampValue(v reflect.Value) time.Time {
	if v.Type() != reflect.TypeFor[time.Time]() {
		v = v.Convert(reflect.TypeFor[time.Time]())
	}
	return v.Interface().(time.Time)
}
