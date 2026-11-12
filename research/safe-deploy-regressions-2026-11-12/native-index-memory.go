//go:build ignore

// Count lookup slices reachable from registered Fire native plans, not total RSS.
package main

import (
	"encoding/json"
	"github.com/prairiegroupinc/fire/fdb"
	"os"
	"reflect"
)

func main() {
	plans := map[uintptr]bool{}
	indexes := map[uintptr]int{}
	var walk func(reflect.Value)
	walk = func(p reflect.Value) {
		if !p.IsValid() || p.IsNil() || plans[p.Pointer()] {
			return
		}
		plans[p.Pointer()] = true
		v := p.Elem()
		index := v.FieldByName("index")
		if index.Len() != 0 {
			indexes[index.Pointer()] = index.Cap() * int(index.Type().Elem().Size())
		}
		walk(v.FieldByName("elem"))
		fields := v.FieldByName("fields")
		for i := 0; i < fields.Len(); i++ {
			walk(fields.Index(i).FieldByName("plan"))
		}
	}
	for _, t := range fdb.Schema.Tables() {
		v := reflect.ValueOf(t).Elem()
		for _, name := range []string{"projection", "binding"} {
			p := v.FieldByName(name)
			if p.IsValid() && !p.IsNil() {
				walk(p.Elem().FieldByName("root"))
			}
		}
	}
	n := 0
	for _, size := range indexes {
		n += size
	}
	json.NewEncoder(os.Stdout).Encode(map[string]int{"plans": len(plans), "index_arrays": len(indexes), "index_bytes": n})
}
