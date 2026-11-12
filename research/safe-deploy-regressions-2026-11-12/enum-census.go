//go:build ignore

// Static persisted-field census through the actual registered Fire schema.
// No database or tenant data is read. Build unchanged in old/new source trees.
package main

import (
	"encoding"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/prairiegroupinc/fire/fdb"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

var interfaces = []struct {
	name string
	t    reflect.Type
}{
	{"custom", reflect.TypeFor[msgpack.CustomEncoder]()},
	{"marshal", reflect.TypeFor[msgpack.Marshaler]()},
	{"binary", reflect.TypeFor[encoding.BinaryMarshaler]()},
	{"text", reflect.TypeFor[encoding.TextMarshaler]()},
}

func wire(t reflect.Type) (result map[string]any) {
	result = map[string]any{}
	for _, i := range interfaces {
		if t.Implements(i.t) || reflect.PointerTo(t).Implements(i.t) {
			result["encoder"] = i.name
			break
		}
	}
	var kinds []string
	for _, n := range []int64{0, 1, 2, 3, 4, 7, 16, 32} {
		func() {
			defer func() {
				if recover() != nil {
					kinds = append(kinds, "panic")
				}
			}()
			v := reflect.New(t).Elem()
			if v.Kind() >= reflect.Uint && v.Kind() <= reflect.Uint64 {
				v.SetUint(uint64(n))
			} else {
				v.SetInt(n)
			}
			raw, err := msgpack.Marshal(v.Addr().Interface())
			if err != nil {
				kinds = append(kinds, "error")
				return
			}
			kind := "other"
			if msgpcode.IsString(raw[0]) {
				kind = "string"
			} else if msgpcode.IsBin(raw[0]) {
				kind = "binary-text"
			} else if raw[0] <= 0x7f || raw[0] >= 0xe0 || raw[0] >= 0xcc && raw[0] <= 0xd3 {
				kind = "integer"
			}
			kinds = append(kinds, kind)
		}()
	}
	result["wire_kinds"] = kinds
	return
}

func main() {
	enc := json.NewEncoder(os.Stdout)
	for _, tbl := range fdb.Schema.Tables() {
		active := map[reflect.Type]bool{}
		var visit func(reflect.Type, string, int)
		visit = func(t reflect.Type, path string, depth int) {
			if depth > 32 || active[t] {
				return
			}
			active[t] = true
			defer delete(active, t)
			k := t.Kind()
			if t.Name() != "" && (k >= reflect.Int && k <= reflect.Int64 || k >= reflect.Uint && k <= reflect.Uint64) {
				r := wire(t)
				r["table"] = tbl.Name()
				r["row_type"] = tbl.RowType().String()
				r["path"] = path
				r["type"] = t.PkgPath() + "." + t.Name()
				if err := enc.Encode(r); err != nil {
					panic(err)
				}
			}
			switch k {
			case reflect.Pointer:
				visit(t.Elem(), path+"*", depth+1)
			case reflect.Array, reflect.Slice:
				visit(t.Elem(), path+"[]", depth+1)
			case reflect.Map:
				visit(t.Key(), path+"{key}", depth+1)
				visit(t.Elem(), path+"{value}", depth+1)
			case reflect.Struct:
				for i := range t.NumField() {
					f := t.Field(i)
					if !f.IsExported() || strings.Split(f.Tag.Get("msgpack"), ",")[0] == "-" {
						continue
					}
					visit(f.Type, path+"."+f.Name, depth+1)
				}
			}
		}
		visit(tbl.RowType(), "", 0)
	}
	fmt.Fprintln(os.Stderr, "census complete")
}
