package evm_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestScriptJSON(t *testing.T) {
	mod, err := evmcompiler.Compile("example.go", `func Result() any {
		return map[string]any{"rows": []any{false, int8(-4), nil, uint64(1<<63)+1, "\n"}, "empty": []int{}, "array":[2]float32{1.25,2}}
	}
	func Cycle() any { m:=map[string]any{}; m["m"]=m; return m }
	func Bad() any { return map[int]int{} }
	func Large() any { return "hello" }`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		limit int
		want  string
	}{
		{"Result", 1024, `{"rows":[false,-4,null,9223372036854775809,"\n"],"empty":[],"array":[1.25,2]}`},
		{"Cycle", 8192, "cyclic"}, {"Bad", 64, "keys must be strings"}, {"Large", 4, "byte limit"},
	} {
		var res [1]values.Value
		if err := evm.New(p).Run(context.Background(), tc.name, nil, res[:]); err != nil {
			t.Fatal(err)
		}
		raw, err := p.EncodeJSON(context.Background(), res[0], tc.limit)
		direct, directErr := p.JSONValue(context.Background(), res[0], tc.limit)
		if tc.name != "Result" {
			if err == nil || !strings.Contains(err.Error(), tc.want) || directErr == nil || !strings.Contains(directErr.Error(), tc.want) {
				t.Fatalf("%s: %s %v", tc.name, raw, err)
			}
			continue
		}
		decode := func(raw string) any {
			d := json.NewDecoder(strings.NewReader(raw))
			d.UseNumber()
			var v any
			if err := d.Decode(&v); err != nil {
				t.Fatal(err)
			}
			return v
		}
		if err != nil || !reflect.DeepEqual(decode(tc.want), decode(string(raw))) {
			t.Fatalf("%s: %s %v", tc.name, raw, err)
		}
		if directErr != nil || !reflect.DeepEqual(direct, decode(string(raw))) {
			t.Fatalf("direct JSON: %#v, %v", direct, directErr)
		}
	}
}

func TestJSONValueStringsAndLimits(t *testing.T) {
	m, err := evmcompiler.Compile("json.go", `func Keep(s string) any { return s }`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"", "hello", "\"\\\b\f\n\r\t\x00<>&\u2028\u2029", "😀\ufffd", "\xff\xfe\xe0\x80\x80"} {
		t.Run(fmt.Sprintf("%q", s), func(t *testing.T) {
			raw, _ := json.Marshal(s)
			v, err := p.PackInterface(values.StringType, values.String(s))
			if err != nil {
				t.Fatal(err)
			}
			var want any
			if err := json.Unmarshal(raw, &want); err != nil {
				t.Fatal(err)
			}
			for _, limit := range []int{len(raw) - 1, len(raw), len(raw) + 1} {
				out, err := p.JSONValue(context.Background(), v, limit)
				if limit < len(raw) {
					if err == nil {
						t.Fatal("size accepted", limit)
					}
					continue
				}
				if err != nil || out != want {
					t.Fatalf("%#v %v, want %#v (raw %s, limit %d)", out, err, want, raw, limit)
				}
			}
		})
	}
	data := []byte("borrowed")
	v, _ := p.PackInterface(values.StringType, values.Bytes(data))
	out, err := p.JSONValue(context.Background(), v, 64)
	if err != nil {
		t.Fatal(err)
	}
	clear(data)
	if out != "borrowed" {
		t.Fatal("retained string still borrows input")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.JSONValue(ctx, v, 64); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestJSONValueAllocationBudget(t *testing.T) {
	called := false
	pkg := evm.Package{Path: "host", Functions: []evm.ExternalFunction{{Name: "Copy", Signature: evm.Signature{Params: []*values.Type{values.AnyType}}, Call: func(c *evm.Call) error {
		called = true
		_, err := c.JSONValue(c.Argument(0), 1<<20)
		return err
	}}}}
	m, err := evmcompiler.Compile("json.go", `import "host"; func Copy(v any) { host.Copy(v) }; func Keep(s string) any { return s }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	v, _ := p.PackInterface(values.StringType, values.String("warm"))
	if err := vm.Run(context.Background(), "Copy", []values.Value{v}, nil); err != nil {
		t.Fatal(err)
	}
	v, _ = p.PackInterface(values.StringType, values.String(strings.Repeat("x", 1024)))
	called = false
	err = vm.RunWithBudget(context.Background(), "Copy", []values.Value{v}, nil, evm.Budget{AllocationBytes: 64})
	if !called || !errors.Is(err, evm.ErrAllocation) {
		t.Fatalf("called=%v: %v", called, err)
	}
}

func TestJSONInterfaceHeaders(t *testing.T) {
	m, err := evmcompiler.Compile("json.go", `
func Keep(b bool, i int64, u uint64, f float64, s string, a []any, m map[string]any) any {
 return []any{b, i, u, f, s, a, m}
}
func Check(v any) bool {
 a := v.([]any)
 return !a[0].(bool) && a[1].(int64) == -9223372036854775808 &&
 a[2].(uint64) == 18446744073709551615 && a[4].(string) == "retained" &&
 a[5] != nil && a[5].([]any) == nil && a[6] != nil && a[6].(map[string]any) == nil
}`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	args := []values.Value{values.Bool(false), values.Int(math.MinInt64), values.Uint(math.MaxUint64), values.Uint(0x7ff8000000000042), values.String(strings.Clone("retained")), {}, {}}
	var out [1]values.Value
	vm := evm.New(p)
	if err := vm.Run(context.Background(), "Keep", args, out[:]); err != nil {
		t.Fatal(err)
	}
	s, payload, err := p.UnpackInterface(out[0])
	if err != nil || s.Kind != values.SliceKind {
		t.Fatalf("slice: %v %v", s, err)
	}
	items := values.CellSlice(payload.P, uint64(payload.SliceLen()))
	for i, typ := range []*values.Type{values.Boolean, values.Int64, values.Uint64, values.Float64, values.StringType} {
		t.Run(typ.Name(), func(t *testing.T) {
			if n := testing.AllocsPerRun(100, func() {
				v, err := p.PackInterface(typ, args[i])
				if err != nil {
					t.Fatal(err)
				}
				s, x, err := p.UnpackInterface(v)
				if err != nil || s.Type != typ || x != args[i] {
					t.Fatalf("round trip: %v %v %v", s, x, err)
				}
				items[i] = v
			}); n != 0 {
				t.Fatalf("interface header allocated %g times", n)
			}
		})
	}
	for _, item := range append(items[5:], out[0]) {
		s, payload, err := p.UnpackInterface(item)
		if err != nil {
			t.Fatal(err)
		}
		if n := testing.AllocsPerRun(100, func() {
			v, err := p.PackInterface(s.Type, payload)
			if err != nil {
				t.Fatal(err)
			}
			typ, got, err := p.UnpackInterface(v)
			if err != nil || typ.Type != s.Type || got != payload {
				t.Fatalf("collection round trip: %v %v %v", typ, got, err)
			}
		}); n != 0 {
			t.Fatalf("collection interface header allocated %g times", n)
		}
	}
	runtime.GC()
	if err := vm.Run(context.Background(), "Check", out[:], out[:]); err != nil || out[0].R != 1 {
		t.Fatalf("retained JSON values: %v %v", out, err)
	}
}
