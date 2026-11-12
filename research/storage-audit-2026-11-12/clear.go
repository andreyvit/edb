//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/prairiegroupinc/fire/nm"
	"github.com/prairiegroupinc/fire/nm/nmbindings"
	"reflect"
	"time"
)

var tt = reflect.TypeFor[time.Time]()

func fill(v reflect.Value, depth, variant int) {
	if !v.CanSet() || depth > 5 {
		return
	}
	if v.Type() == tt {
		v.Set(reflect.ValueOf(time.Date(2024, 3, 7, 12, 13, 14, 123456789, time.UTC)))
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("probe-é-\x00-x")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := int64(1)
		if variant == 2 && v.Type().PkgPath() == "" {
			n = -17
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.25)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fill(v.Elem(), depth+1, variant)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			fill(v.Field(i), depth+1, variant)
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			fill(v.Index(i), depth+1, variant)
		}
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 2, 2))
		for i := 0; i < 2; i++ {
			fill(v.Index(i), depth+1, variant)
		}
	case reflect.Map:
		k := reflect.New(v.Type().Key()).Elem()
		fill(k, depth+1, variant)
		x := reflect.New(v.Type().Elem()).Elem()
		fill(x, depth+1, variant)
		v.Set(reflect.MakeMap(v.Type()))
		v.SetMapIndex(k, x)
	}
}
func safe(f func()) (err any) { defer func() { err = recover() }(); f(); return }
func main() {
	nmbindings.InitNativeKVOBindings()
	cases, failures, rejected := 0, 0, 0
	for _, m := range nm.Schema.Models() {
		for _, typ := range m.Type().(*kvo.EntityType).NativeTypes() {
			p := kvo.Project(typ, m, nil)
			src := reflect.New(typ)
			fill(src.Elem(), 0, 1)
			var raw []byte
			if err := safe(func() { raw = msgpack.Encode(nil, src); msgpack.Decode(raw, src.Elem()) }); err != nil {
				rejected++
				continue
			}
			for _, f := range m.NativeStruct(typ).Fields() {
				for _, mode := range []string{"delete", "set-zero"} {
					ft := typ.FieldByIndex(f.FieldPath()).Type
					if mode == "set-zero" && (ft.Kind() < reflect.Bool || ft.Kind() > reflect.String || ft.Kind() == reflect.Array || ft.Kind() == reflect.Interface || ft.Kind() == reflect.Map || ft.Kind() == reflect.Pointer || ft.Kind() == reflect.Slice || ft.Kind() == reflect.Struct) {
						continue
					}
					cases++
					err := safe(func() {
						want := reflect.New(typ)
						msgpack.Decode(raw, want.Elem())
						want.Elem().FieldByIndex(f.FieldPath()).SetZero()
						expected := msgpack.Encode(nil, want)
						e := p.MessagePack(raw).Edit()
						if mode == "delete" {
							e.Delete(f.Prop)
						} else {
							e.SetValue(f.Prop.Code(), p.Record(want).GetValue(f.Prop.Code()))
						}
						encoded := e.MessagePack()
						if encoded == nil {
							encoded = msgpack.Encode(nil, reflect.ValueOf(e.Record().Native()))
						}
						got := reflect.New(typ)
						msgpack.Decode(encoded, got.Elem())
						actual := msgpack.Encode(nil, got)
						if !bytes.Equal(expected, actual) {
							panic(fmt.Sprintf("full native bytes differ expected=%x got=%x", expected, actual))
						}
					})
					if err != nil {
						failures++
						fmt.Printf("FAIL %s %s.%s: %v\n", mode, m.Name(), typ.FieldByIndex(f.FieldPath()).Name, err)
					}
				}
			}
		}
	}
	fmt.Printf("clear audit: comparisons=%d failures=%d rejected-model-fixtures=%d\n", cases, failures, rejected)
}
