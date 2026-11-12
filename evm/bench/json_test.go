package bench_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/bench/internal/jsonwork"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type jsonFixture struct {
	p             *evm.Program
	vm            *evm.VM
	ctx           context.Context
	docs          [4]values.Value
	rows          [4]values.Value
	want, updated [4]int64
	raw           [4][]byte
}

func newJSONFixture(t testing.TB) *jsonFixture {
	t.Helper()
	schema := kvo.NewSchema()
	data := kvo.NewProp(schema, 1, "data", kvo.TJSON, nil)
	model := kvo.NewModel(schema, kvo.NewEntityType(schema, "document"), func(b *kvo.ModelBuilder) { b.Prop(schema.MustPropByCode(data)) })
	pkg := evm.Package{Path: "db", Records: []*evm.RecordType{evm.NewRecordType("Document", model, false)}}
	source := strings.Replace(jsonwork.Source, "package jsonwork", `package main; import "db"`, 1) + `
func Decode(row *db.Document) any { return row.data }
func DecodeRead(row *db.Document) int64 { return Read(row.data) }
`
	m, err := evmcompiler.Compile("json.go", source, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, scope := evm.WithRecords(context.Background())
	t.Cleanup(scope.Close)
	f := &jsonFixture{p: p, vm: evm.New(p), ctx: ctx}
	f.vm.Limits.AllocationBytes, f.vm.Limits.Allocations = ^uint64(0), ^uint64(0)
	for seed := range 4 {
		doc := jsonwork.Build(seed)
		f.want[seed] = jsonwork.Read(doc)
		f.raw[seed], err = json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		var row row2.Writer
		row.Begin(0, "", row2.Object, row2.Fast2)
		row.Put(data, "", values.Bytes(f.raw[seed]), row2.Data)
		row.End()
		f.rows[seed] = scope.Read(model, row2.Trusted(row.BytesWritten()))
		var out [1]values.Value
		if err := f.vm.Run(ctx, "Build", []values.Value{values.Int(int64(seed))}, out[:]); err != nil {
			t.Fatal(err)
		}
		f.docs[seed] = out[0]
		f.updated[seed] = jsonwork.Update(doc, seed)
	}
	return f
}

func (f *jsonFixture) run(name string, seed int) (int64, error) {
	var args [2]values.Value
	n := 1
	want := f.want[seed]
	switch name {
	case "BuildRead":
		args[0] = values.Int(int64(seed))
	case "Read":
		args[0] = f.docs[seed]
	case "Update":
		args[0], args[1], n, want = f.docs[seed], values.Int(int64(seed)), 2, f.updated[seed]
	case "DecodeRead":
		args[0] = f.rows[seed]
	case "Rebox":
		args[0], want = values.Int(1024), 1024*1023/2+8
	default:
		return 0, fmt.Errorf("unknown JSON workload %s", name)
	}
	var out [1]values.Value
	err := f.vm.RunWithBudget(f.ctx, name, args[:n], out[:], evm.Budget{AllocationBytes: 16 << 20, Allocations: 100000})
	if err == nil && out[0].AsInt() != want {
		err = fmt.Errorf("%s seed %d: got %d, want %d", name, seed, out[0].AsInt(), want)
	}
	return out[0].AsInt(), err
}

var jsonWorkloads = []string{"BuildRead", "Read", "Update", "DecodeRead", "Rebox"}

func TestJSONWorkload(t *testing.T) {
	for _, name := range jsonWorkloads {
		t.Run(name, func(t *testing.T) {
			f := newJSONFixture(t)
			for i := range 8 {
				if _, err := f.run(name, i%4); err != nil {
					t.Fatal(err)
				}
			}
			for seed := range 4 {
				value := f.docs[seed]
				if name == "DecodeRead" {
					var out [1]values.Value
					if err := f.vm.Run(f.ctx, "Decode", []values.Value{f.rows[seed]}, out[:]); err != nil {
						t.Fatal(err)
					}
					value = out[0]
				}
				raw, err := f.p.EncodeJSON(f.ctx, value, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
				want := jsonwork.Build(seed)
				if name == "Update" {
					jsonwork.Update(want, seed)
				}
				wantRaw, err := json.Marshal(want)
				if err != nil {
					t.Fatal(err)
				}
				decode := func(raw []byte) any {
					var v any
					d := json.NewDecoder(strings.NewReader(string(raw)))
					d.UseNumber()
					if err := d.Decode(&v); err != nil {
						t.Fatal(err)
					}
					return v
				}
				if !reflect.DeepEqual(decode(raw), decode(wantRaw)) {
					t.Fatalf("%s seed %d: JSON differs", name, seed)
				}
			}
		})
	}
}

func BenchmarkJSON(b *testing.B) {
	for _, name := range jsonWorkloads {
		b.Run(name, func(b *testing.B) {
			f := newJSONFixture(b)
			for i := range 8 {
				if _, err := f.run(name, i%4); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if _, err := f.run(name, i%4); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("Encode", func(b *testing.B) {
		f := newJSONFixture(b)
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			raw, err := f.p.EncodeJSON(f.ctx, f.docs[i%4], 1<<20)
			if err != nil || len(raw) != len(f.raw[i%4]) {
				b.Fatalf("JSON size %d: %v", len(raw), err)
			}
		}
	})
}
