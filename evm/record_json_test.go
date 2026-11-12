package evm_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type jsonCustomWord uint64

func (n jsonCustomWord) MarshalJSON() ([]byte, error) {
	if n == 0 {
		return nil, errors.New("custom JSON failure")
	}
	return []byte(`{"id":18446744073709551615}`), nil
}

type jsonTextWord uint64

func (jsonTextWord) MarshalText() ([]byte, error)  { return []byte("custom text"), nil }
func (n *jsonTextWord) UnmarshalText([]byte) error { *n = 3; return nil }

type jsonScalarRow struct {
	Name  string         `kvo:"name"`
	Code  jsonCustomWord `kvo:"code"`
	Text  jsonTextWord   `kvo:"text"`
	Big   uint64         `kvo:"big"`
	Small uint64         `kvo:"small"`
	Float float64        `kvo:"float"`
	Data  []byte         `kvo:"data"`
	Extra any            `kvo:"extra"`
}

func TestRecordJSONConversion(t *testing.T) {
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "row", reflect.TypeFor[jsonScalarRow]())
	for i, p := range []struct {
		name string
		typ  kvo.AnyType
	}{
		{"name", kvo.TMachineString}, {"code", kvo.NewScalarSubtype[jsonCustomWord]("code", kvo.TUint64)},
		{"text", kvo.NewScalarSubtype[jsonTextWord]("text", kvo.TUint64)}, {"big", kvo.TUint64}, {"small", kvo.TUint64},
		{"float", kvo.TFloat64}, {"data", kvo.TBytes}, {"extra", kvo.TJSON},
	} {
		kvo.NewProp(s, uint64(i+1), p.name, p.typ, nil)
	}
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) {
		for i := uint64(1); i <= 8; i++ {
			b.Prop(s.MustPropByCode(i))
		}
	})
	bind := kvo.Bind(reflect.TypeFor[jsonScalarRow](), m)
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		t.Fatal(err)
	}
	mod, err := evmcompiler.Compile("json.go", `import "m"
func Export(r *m.Row) any { return r.ToJSON() }
func Kinds(r *m.Row) bool { v := r.ToJSON(); return v["small"].(int64)==7 && v["big"].(uint64)==18446744073709551615 && v["float"].(int64)==10000000000 }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	row := jsonScalarRow{"hello\xff\xfe", 9, 3, math.MaxUint64, 7, 1e10, []byte{0, 1, 255}, map[string]any{"n": uint64(math.MaxUint64), "f": 0.25, "empty": []any{}}}
	for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2"} {
		t.Run(format, func(t *testing.T) {
			var r kvo.Record
			var data []byte
			switch format {
			case "native":
				r = bind.Record(reflect.ValueOf(&row))
			case "MessagePack":
				data, err = msgpack.TryEncode(nil, reflect.ValueOf(row))
				if err != nil {
					t.Fatal(err)
				}
				r = bind.MessagePack(data)
			default:
				layout := row2.Compact2
				if format == "Fast2" {
					layout = row2.Fast2
				}
				data = bind.Encode(nil, reflect.ValueOf(row), layout)
				r = bind.Encoded(row2.Trusted(data))
			}
			// The old path is the semantic oracle, only retained in this test.
			raw, err := json.Marshal(r.ToJSON(m, kvo.JSONOptions{}))
			if err != nil {
				t.Fatal(err)
			}
			var want any
			d := json.NewDecoder(bytes.NewReader(raw))
			d.UseNumber()
			if err := d.Decode(&want); err != nil {
				t.Fatal(err)
			}
			vm := evm.New(p)
			args := []values.Value{r.Value()}
			var result [1]values.Value
			if err := vm.Run(context.Background(), "Kinds", args, result[:]); err != nil || result[0].R != 1 {
				t.Fatal("number kinds", result, err)
			}
			if err := vm.Run(context.Background(), "Export", args, result[:]); err != nil {
				t.Fatal(err)
			}
			clear(data)
			got, err := p.JSONValue(context.Background(), result[0], 1<<20)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("got %#v, %v; want %#v", got, err, want)
			}
		})
	}
	vm := evm.New(p)
	args := []values.Value{bind.Record(reflect.ValueOf(&row)).Value()}
	var out [1]values.Value
	if err := vm.Run(context.Background(), "Export", args, out[:]); err != nil {
		t.Fatal(err)
	}
	if err := vm.RunWithBudget(context.Background(), "Export", args, out[:], evm.Budget{AllocationBytes: 128}); !errors.Is(err, evm.ErrAllocation) {
		t.Fatal("budget", err)
	}
	row.Code = 0
	if err := evm.New(p).Run(context.Background(), "Export", args, out[:]); err == nil {
		t.Fatal("ignored custom marshaler error")
	}
	row.Code, row.Float = 9, math.NaN()
	if err := evm.New(p).Run(context.Background(), "Export", args, out[:]); err == nil {
		t.Fatal("accepted NaN")
	}
}

type jsonNullableChild struct {
	Name string `kvo:"name" json:"name"`
}
type jsonNullableRow struct {
	Items []*jsonNullableChild          `kvo:"items" json:"items"`
	Dict  map[string]*jsonNullableChild `kvo:"dict" json:"dict"`
}

func TestRecordJSONNullableCollections(t *testing.T) {
	s := kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	ct := kvo.NewEntityType(s, "child", reflect.TypeFor[jsonNullableChild]())
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name) })
	items := kvo.NewProp(s, 2, "items", kvo.ListOf(ct), nil)
	dict := kvo.NewProp(s, 3, "dict", kvo.MapOf(kvo.TMachineString, ct), nil)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "row", reflect.TypeFor[jsonNullableRow]()), func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(items)); b.Prop(s.MustPropByCode(dict)) })
	bind := kvo.Bind(reflect.TypeFor[jsonNullableRow](), m)
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		t.Fatal(err)
	}
	mod, err := evmcompiler.Compile("json.go", `import "m"
func Export(r *m.Row) any { return r.ToJSON() }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	src := &jsonNullableRow{[]*jsonNullableChild{{"kept"}, nil}, map[string]*jsonNullableChild{"nil": nil, "kept": {"value"}}}
	raw, err := json.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	var want any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	for _, r := range []kvo.Record{bind.Record(reflect.ValueOf(src)), bind.MessagePack(msgpack.Encode(nil, reflect.ValueOf(src)))} {
		vm := evm.New(p)
		var out [1]values.Value
		if err := vm.Run(context.Background(), "Export", []values.Value{r.Value()}, out[:]); err != nil {
			t.Fatal(err)
		}
		got, err := p.JSONValue(context.Background(), out[0], 1<<20)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, %v; want %#v", got, err, want)
		}
	}
}
