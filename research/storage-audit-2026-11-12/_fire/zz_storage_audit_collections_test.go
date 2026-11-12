package fdb

import (
	"encoding/json"
	"fmt"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/prairiegroupinc/fire/nm"
	"math/rand/v2"
	"reflect"
	"testing"
)

func auditCollectionCanonical(v reflect.Value) string {
	if r, ok := v.Interface().(kvo.Record); ok {
		if r.Missing() {
			return "null"
		}
		raw, err := json.Marshal(r.ToJSON(r.Model(), kvo.JSONOptions{}))
		if err != nil {
			panic(err)
		}
		return string(raw)
	}
	if seq := v.MethodByName("Iter"); seq.IsValid() {
		return fmt.Sprint(auditCollectionItems(v))
	}
	return fmt.Sprintf("%T:%#v", v.Interface(), v.Interface())
}
func auditCollectionItems(v reflect.Value) map[string]string {
	iter := v.MethodByName("Iter").Call(nil)[0]
	out := map[string]string{}
	i := 0
	yield := reflect.MakeFunc(iter.Type().In(0), func(args []reflect.Value) []reflect.Value {
		key := fmt.Sprint(i)
		value := args[0]
		if len(args) == 2 {
			key = auditCollectionCanonical(args[0])
			value = args[1]
		}
		out[key] = auditCollectionCanonical(value)
		i++
		return []reflect.Value{reflect.ValueOf(true)}
	})
	iter.Call([]reflect.Value{yield})
	return out
}
func TestDeepCollectionAudit(t *testing.T) {
	props := map[uint64]any{}
	for _, v := range auditProperties {
		p := v.(interface{ Code() uint64 })
		props[p.Code()] = v
	}
	cases, failures, rejected := 0, 0, 0
	rng := rand.New(rand.NewPCG(73, 12))
	for _, m := range nm.Schema.Models() {
		for _, typ := range m.Type().(*kvo.EntityType).NativeTypes() {
			p := kvo.Project(typ, m, nil)
			for _, f := range m.NativeStruct(typ).Fields() {
				prop := props[f.Prop.Code()]
				if prop == nil {
					continue
				}
				get := reflect.ValueOf(prop).MethodByName("Get")
				if !get.IsValid() {
					continue
				}
				for scenario := 0; scenario < 12; scenario++ {
					src := reflect.New(typ)
					if scenario == 1 {
						auditFill(src.Elem().FieldByIndex(f.FieldPath()), 0, 1)
					} else if scenario > 1 {
						auditFillRandom(src.Elem().FieldByIndex(f.FieldPath()), 0, rng)
					}
					var raw []byte
					if auditSafe(func() { raw = msgpack.Encode(nil, src); src = reflect.New(typ); msgpack.Decode(raw, src.Elem()) }) != nil {
						rejected++
						continue
					}
					cases++
					problem := auditSafe(func() {
						a := get.Call([]reflect.Value{reflect.ValueOf(p.Record(src))})[0]
						b := get.Call([]reflect.Value{reflect.ValueOf(p.MessagePack(raw))})[0]
						if a.MethodByName("Iter").IsValid() {
							var av, bv map[string]string
							if err := auditSafe(func() { av = auditCollectionItems(a) }); err != nil {
								panic(fmt.Sprint("native: ", err))
							}
							if err := auditSafe(func() { bv = auditCollectionItems(b) }); err != nil {
								panic(fmt.Sprint("encoded: ", err))
							}
							if !reflect.DeepEqual(av, bv) {
								panic(fmt.Sprintf("native=%v encoded=%v", av, bv))
							}
							if n := b.MethodByName("Len").Call(nil)[0].Int(); int(n) != len(bv) {
								panic("Len disagrees with Iter")
							}
						}
					})
					if problem != nil {
						failures++
						fmt.Printf("FAIL-COLLECTION %s.%s scenario=%d: %v\n", m.Name(), typ.FieldByIndex(f.FieldPath()).Name, scenario, problem)
						if fmt.Sprint(problem) == "native: row2: invalid input" {
							fmt.Printf("FIXTURE raw=%x\n", raw)
						}
					}
				}
			}
		}
	}
	fmt.Printf("collection audit: props=%d cases=%d rejected=%d failures=%d\n", len(props), cases, rejected, failures)
	if failures > 0 {
		t.Fail()
	}
}
