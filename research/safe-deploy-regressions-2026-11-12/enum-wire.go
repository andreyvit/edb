//go:build ignore

// Build unchanged from deployed and candidate Fire trees. Synthetic inputs only.
package main

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"

	"github.com/prairiegroupinc/fire/bm/msocmon"
	"github.com/prairiegroupinc/fire/bpub"
	"github.com/vmihailenco/msgpack/v5"
)

var types = map[string]reflect.Type{
	"ChangeType":       reflect.TypeFor[bpub.ChangeType](),
	"RedemptionStatus": reflect.TypeFor[bpub.RedemptionStatus](),
	"Platform":         reflect.TypeFor[msocmon.Platform](),
	"OriginCategory":   reflect.TypeFor[bpub.OriginCategory](),
}

type sample struct {
	Type        string
	Number      uint64
	Text, Bytes string
}

func main() {
	if len(os.Args) < 2 {
		panic("encode | decode FILE")
	}
	enc := json.NewEncoder(os.Stdout)
	if os.Args[1] == "encode" {
		var names []string
		for name := range types {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			for _, number := range []uint64{0, 1, 2, 3, 4, 7, 16, 32} {
				func() {
					defer func() {
						if p := recover(); p != nil {
							fmt.Fprintf(os.Stderr, "%s/%d: %T\n", name, number, p)
						}
					}()
					v := reflect.New(types[name]).Elem()
					if v.Kind() >= reflect.Uint && v.Kind() <= reflect.Uint64 {
						v.SetUint(number)
					} else {
						v.SetInt(int64(number))
					}
					text, _ := v.Interface().(encoding.TextMarshaler).MarshalText()
					if len(text) == 0 {
						return
					}
					raw, err := msgpack.Marshal(v.Interface())
					if err != nil {
						panic(err)
					}
					if err := enc.Encode(sample{name, number, string(text), base64.StdEncoding.EncodeToString(raw)}); err != nil {
						panic(err)
					}
				}()
			}
		}
		return
	}
	f, err := os.Open(os.Args[2])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	for dec.More() {
		var s sample
		if err := dec.Decode(&s); err != nil {
			panic(err)
		}
		raw, err := base64.StdEncoding.DecodeString(s.Bytes)
		if err != nil {
			panic(err)
		}
		v := reflect.New(types[s.Type])
		err = msgpack.Unmarshal(raw, v.Interface())
		out := map[string]any{"type": s.Type, "input": s.Number, "text": s.Text, "wire_first_byte": fmt.Sprintf("%02x", raw[0])}
		if err != nil {
			out["error"] = err.Error()
		} else {
			out["value"] = v.Elem().Interface()
		}
		if err := enc.Encode(out); err != nil {
			panic(err)
		}
	}
}
