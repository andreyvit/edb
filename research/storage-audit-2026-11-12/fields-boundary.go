//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/prairiegroupinc/fire/nm"
	"github.com/prairiegroupinc/fire/nm/nmbindings"
	"math"
	"reflect"
	"time"
)

var tt = reflect.TypeFor[time.Time]()

func fill(v reflect.Value, depth, variant int) {
	if !v.CanSet() || depth > 5 {
		return
	}
	if v.Type().ConvertibleTo(tt) && v.Kind() == reflect.Struct {
		times := []time.Time{time.Time{}, time.Date(2024, 3, 7, 12, 13, 14, 123456789, time.UTC), time.Unix(-1, 999999999).UTC(), time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), time.Date(1677, 9, 21, 0, 12, 43, 0, time.UTC), time.Date(2262, 4, 11, 23, 47, 16, 999999999, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(1960, 1, 1, 4, 0, 0, 37, time.FixedZone("audit", 18000))}
		v.Set(reflect.ValueOf(times[variant]).Convert(v.Type()))
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		if variant != 3 {
			v.SetString("probe-é-\x00-x")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := int64(variant)
		if v.Type().PkgPath() == "" {
			switch variant {
			case 2:
				n = -17
			case 3:
				n = 0
			case 4:
				n = int64(uint64(1)<<(v.Type().Bits()-1)) - 1
			case 5:
				n = -int64(uint64(1) << (v.Type().Bits() - 1))
			}
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n := uint64(variant)
		if v.Type().PkgPath() == "" {
			if variant == 4 {
				n = ^uint64(0) >> (64 - v.Type().Bits())
			}
			if variant == 3 {
				n = 0
			}
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		n := 1.25
		switch variant {
		case 2:
			n = -1.25
		case 3:
			n = math.Copysign(0, -1)
		case 4:
			n = math.SmallestNonzeroFloat64
			if v.Kind() == reflect.Float32 {
				n = math.SmallestNonzeroFloat32
			}
		case 5:
			n = math.MaxFloat64
			if v.Kind() == reflect.Float32 {
				n = math.MaxFloat32
			}
		}
		v.SetFloat(n)
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
	models, fields, checks, failures, invalid := 0, 0, 0, 0, 0
	for _, m := range nm.Schema.Models() {
		for _, typ := range m.Type().(*kvo.EntityType).NativeTypes() {
			models++
			p := kvo.Project(typ, m, nil)
			for _, f := range m.NativeStruct(typ).Fields() {
				fields++
				for variant := 0; variant < 8; variant++ {
					src := reflect.New(typ)
					field := src.Elem().FieldByIndex(f.FieldPath())
					if variant > 0 {
						fill(field, 0, variant)
					}
					var raw []byte
					if err := safe(func() { raw = msgpack.Encode(nil, src) }); err != nil {
						invalid++
						fmt.Printf("INVALID %s.%s variant=%d: %v\n", m.Name(), typ.FieldByIndex(f.FieldPath()).Name, variant, err)
						continue
					}
					var canonical reflect.Value
					if err := safe(func() { canonical = reflect.New(typ); msgpack.Decode(raw, canonical.Elem()) }); err != nil {
						invalid++
						fmt.Printf("INVALID-DECODE %s.%s variant=%d: %v\n", m.Name(), f.Prop.Name(), variant, err)
						continue
					}
					for _, op := range []string{"decode-property", "snapshot", "edit"} {
						checks++
						err := safe(func() {
							nr, er := p.Record(canonical), p.MessagePack(raw)
							switch op {
							case "decode-property":
								a := reflect.New(field.Type()).Elem()
								b := reflect.New(field.Type()).Elem()
								if err := p.TryDecodeProperty(a, nr, f.Prop.Code()); err != nil {
									panic(fmt.Sprint("native: ", err))
								}
								if err := p.TryDecodeProperty(b, er, f.Prop.Code()); err != nil {
									panic(fmt.Sprint("encoded: ", err))
								}
								if !reflect.DeepEqual(a.Interface(), b.Interface()) {
									panic(fmt.Sprintf("native=%#v encoded=%#v", a.Interface(), b.Interface()))
								}
								if field.Kind() != reflect.Struct && field.Kind() != reflect.Pointer && field.Kind() != reflect.Map && field.Kind() != reflect.Slice && field.Kind() != reflect.Array && field.Kind() != reflect.Interface {
									want := canonical.Elem().FieldByIndex(f.FieldPath())
									if !reflect.DeepEqual(b.Interface(), want.Interface()) {
										panic(fmt.Sprintf("scalar changed: decoded=%#v persisted=%#v", b.Interface(), want.Interface()))
									}
								}
							case "snapshot":
								a, b := nr.Snapshot().Raw(), er.Snapshot().Raw()
								if !bytes.Equal(a, b) {
									// Timestamps can use equivalent word/span representations.
									da, db := reflect.New(field.Type()).Elem(), reflect.New(field.Type()).Elem()
									p.DecodeProperty(da, kvo.Encoded(row2.Trusted(a)).WithModel(m), f.Prop.Code())
									p.DecodeProperty(db, kvo.Encoded(row2.Trusted(b)).WithModel(m), f.Prop.Code())
									if !reflect.DeepEqual(da.Interface(), db.Interface()) {
										panic(fmt.Sprintf("snapshot value differs: native=%#v encoded=%#v", da.Interface(), db.Interface()))
									}
								}
							case "edit":
								if variant == 0 {
									return
								}
								empty := msgpack.Encode(nil, reflect.New(typ))
								e := p.MessagePack(empty).Edit()
								value := nr.GetValue(f.Prop.Code())
								e.SetValue(f.Prop.Code(), value)
								got := reflect.New(typ)
								edited := e.MessagePack()
								if native := e.Record().Native(); native != nil {
									// Custom root codecs use a native editor; MessagePack only
									// exposes bytes when the editor itself remains encoded.
									edited = msgpack.Encode(nil, reflect.ValueOf(native))
								}
								msgpack.Decode(edited, got.Elem())
								a, b := nr.GetValue(f.Prop.Code()), p.Record(got).GetValue(f.Prop.Code())
								if a.P == nil && b.P == nil {
									if a.R != b.R {
										panic(fmt.Sprintf("word differs: %v vs %v", a, b))
									}
								} else if !bytes.Equal(a.AsBytes(), b.AsBytes()) {
									panic("value span differs")
								}
							}
						})
						if err != nil {
							failures++
							fmt.Printf("FAIL %s %s.%s prop=%s type=%s variant=%d: %v\n", op, m.Name(), typ.FieldByIndex(f.FieldPath()).Name, f.Prop.FullName(), field.Type(), variant, err)
						}
					}
				}
			}
		}
	}
	fmt.Printf("models=%d fields=%d comparisons=%d failures=%d rejected-fixtures=%d\n", models, fields, checks, failures, invalid)
}
