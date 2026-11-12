package kvo

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/andreyvit/edb/values"
)

// JSONValue and DecodeJSON implement TJSON for reflective and generated bindings.
// Integer JSON tokens retain all 64 bits when decoded into an interface.
func JSONValue(v any) values.Value {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return values.Bytes(raw)
}
func DecodeJSON(v values.Value, dst any) {
	raw := v.AsBytes()
	if len(raw) == 0 {
		raw = []byte("null")
	}
	if out, ok := dst.(*any); ok {
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		var x any
		if err := d.Decode(&x); err != nil {
			panic(err)
		}
		*out = numberValues(x)
	} else if err := json.Unmarshal(raw, dst); err != nil {
		panic(err)
	}
}

func numberValues(v any) any {
	switch x := v.(type) {
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return n
		}
		if n, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return n
		}
		n, err := x.Float64()
		if err != nil {
			panic(err)
		}
		return n
	case []any:
		for i, v := range x {
			x[i] = numberValues(v)
		}
	case map[string]any:
		for k, v := range x {
			x[k] = numberValues(v)
		}
	}
	return v
}
