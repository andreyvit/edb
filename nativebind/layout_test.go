package nativebind_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/nativebind"
)

func TestNativeScalarLayouts(t *testing.T) {
	type count int16
	type label string
	n := 7
	for _, want := range []any{true, int(-3), int8(-4), count(-5), int32(-6), int64(-7), uint(3), uint8(4), uint16(5), uint32(6), uint64(7), uintptr(8), float32(-1.25), float64(3.5), label("native"), &n} {
		typ := reflect.TypeOf(want)
		src := reflect.New(typ)
		src.Elem().Set(reflect.ValueOf(want))
		for _, direct := range []bool{false, true} {
			layout := nativebind.Layout(typ, nil, direct)
			dst := layout.New()
			value := layout.Load(src.UnsafePointer())
			layout.Store(dst, value)
			if got := reflect.NewAt(typ, dst).Elem().Interface(); !reflect.DeepEqual(got, want) {
				t.Fatalf("%v direct=%v: %v != %v", typ, direct, got, want)
			}
		}
	}
}
