package kvo_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
)

type numericRow[T comparable] struct {
	Value T `kvo:"value" msgpack:"v"`
}

func TestBuiltinNumericBindings(t *testing.T) {
	checkBuiltinNumeric(t, kvo.TInt, -int(^uint(0)>>1)-1)
	checkBuiltinNumeric(t, kvo.TInt8, int8(math.MinInt8))
	checkBuiltinNumeric(t, kvo.TInt16, int16(math.MinInt16))
	checkBuiltinNumeric(t, kvo.TInt32, int32(math.MinInt32))
	checkBuiltinNumeric(t, kvo.TInt64, int64(math.MinInt64))
	checkBuiltinNumeric(t, kvo.TUint, ^uint(0))
	checkBuiltinNumeric(t, kvo.TUint8, uint8(math.MaxUint8))
	checkBuiltinNumeric(t, kvo.TUint16, uint16(math.MaxUint16))
	checkBuiltinNumeric(t, kvo.TUint32, uint32(math.MaxUint32))
	checkBuiltinNumeric(t, kvo.TUint64, uint64(math.MaxUint64))
	checkBuiltinNumeric(t, kvo.TUintptr, ^uintptr(0))
	checkBuiltinNumeric(t, kvo.TFloat32, float32(1.234567))
	checkBuiltinNumeric(t, kvo.TFloat64, -1.234567891234567)
}

func checkBuiltinNumeric[T comparable](t *testing.T, typ *kvo.WordType, value T) {
	t.Helper()
	t.Run(typ.Name(), func(t *testing.T) {
		if typ.NativeType() != reflect.TypeFor[T]() {
			t.Fatal("builtin has wrong Go type", typ.NativeType())
		}
		s := kvo.NewSchema()
		code := kvo.NewProp(s, 1, "value", typ, nil)
		model := kvo.NewModel(s, kvo.NewEntityType(s, "numeric", reflect.TypeFor[numericRow[T]]()), func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(code)) })
		binding := kvo.Bind(reflect.TypeFor[numericRow[T]](), model)
		row := numericRow[T]{value}
		want := binding.Record(reflect.ValueOf(&row)).GetValue(code)
		// The Go MessagePack encoder does not support uintptr fields.
		if typ != kvo.TUintptr {
			if got := binding.MessagePack(msgpack.Encode(nil, reflect.ValueOf(&row))).GetValue(code); got != want {
				t.Fatal("MessagePack differs from native", got, want)
			}
		}
		for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
			raw := binding.Encode(nil, reflect.ValueOf(row), layout)
			if got := binding.Encoded(row2.Trusted(raw)).GetValue(code); got != want {
				t.Fatal("encoded access differs from native", got, want)
			}
			var got numericRow[T]
			binding.Decode(raw, reflect.ValueOf(&got).Elem())
			if got != row {
				t.Fatal("round trip changed numeric value", got, row)
			}
		}
	})
}
