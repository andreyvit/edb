package kvo_test

import (
	"context"
	"math"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/internal/valuebench"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

func TestFloat32UsesSharedRuntimeBits(t *testing.T) {
	type row struct {
		N float32 `edb:"n"`
	}
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "FloatRow", reflect.TypeFor[row]())
	kvo.NewProp(s, 1, "n", kvo.NewFloatType[float32]("float32", nil), nil)
	m := kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	b := kvo.Bind(reflect.TypeFor[row](), m)
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		t.Fatal(err)
	}
	module, err := evmcompiler.Compile("float.go", `import "m"; func Read(r *m.FloatRow) float32 { return r.n }; func Write(r *m.FloatRowEdit, n float32) float32 {r.n=n;return r.n}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	program, err := evm.Link(module, pkg)
	if err != nil {
		t.Fatal(err)
	}
	src := row{1.25}
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		raw := b.Encode(nil, reflect.ValueOf(&src).Elem(), layout)
		for _, r := range []kvo.Record{
			b.Encoded(row2.Trusted(raw)),
			kvo.Encoded(row2.Trusted(raw)).WithModel(m),
			kvo.Encoded(row2.Trusted(raw)).WithModel(m).Fastest(),
			b.Record(reflect.ValueOf(&src)),
		} {
			if v := r.GetValue(1); v.R != uint64(math.Float32bits(src.N)) || v.P != nil {
				t.Fatalf("unexpected float32: %+v", v)
			}
			ctx, scope := evm.WithRecords(context.Background())
			vm := evm.New(program)
			out := make([]values.Value, 1)
			if err := vm.Run(ctx, "Read", []values.Value{scope.ReadRecord(m, r)}, out); err != nil || out[0].AsFloat32() != src.N {
				t.Fatal(out, err)
			}
			e := r.Edit()
			if err := vm.Run(ctx, "Write", []values.Value{scope.EditRecord(m, e), values.Float32(-2.5)}, out); err != nil || out[0].AsFloat32() != -2.5 {
				t.Fatal(out, err)
			}
			scope.Close()
			if e.GetValue(1).AsFloat32() != -2.5 {
				t.Fatal("edit changed runtime representation")
			}
			var dst row
			if err := b.TryDecode(e.Snapshot().Raw(), reflect.ValueOf(&dst).Elem()); err != nil || dst.N != -2.5 {
				t.Fatal(dst, err)
			}
		}
		src.N = 1.25
		var w row2.Writer
		w.Begin(0, "", row2.Object, layout)
		w.Put(1, "", values.Float(math.MaxFloat64), row2.Word)
		w.End()
		if err := b.TryValidate(w.BytesWritten()); err == nil {
			t.Fatal("validation accepted float32 overflow")
		}
		var dst row
		if err := b.TryDecode(w.BytesWritten(), reflect.ValueOf(&dst).Elem()); err == nil {
			t.Fatal("decode accepted float32 overflow")
		}
	}
}

func TestSharedStringOwnership(t *testing.T) {
	schema, model := valuebench.Model()
	ref := kvo.Bind(reflect.TypeFor[valuebench.Row](), model)
	gen := ref.WithGenerated(&valuebench.Static, nativebind.StaticOnly)
	pkg, err := evm.ModelPackage("m", schema)
	if err != nil {
		t.Fatal(err)
	}
	module, err := evmcompiler.Compile("read.go", `import "m"; func Read(r *m.Row) string { return r.name }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	program, err := evm.Link(module, pkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Fast2", "Compact2", "MessagePack", "Generated", "Reflected", "Memory"} {
		t.Run(name, func(t *testing.T) {
			src := valuebench.Sample()
			want := src.Name
			var raw []byte
			var r kvo.Record
			switch name {
			case "Generated":
				r = gen.Record(reflect.ValueOf(src))
			case "Reflected":
				r = ref.Record(reflect.ValueOf(src))
			case "MessagePack":
				raw, err = msgpack.TryEncode(nil, reflect.ValueOf(src))
				if err != nil {
					t.Fatal(err)
				}
				r = gen.MessagePack(raw)
			default:
				layout := row2.Compact2
				if name != "Compact2" {
					layout = row2.Fast2
				}
				raw = gen.Encode(nil, reflect.ValueOf(src).Elem(), layout)
				r = gen.Encoded(row2.Trusted(raw))
				if name == "Memory" {
					r = kvo.Encoded(row2.Trusted(raw)).WithModel(model).Fastest()
				}
			}
			if v := r.GetValue(1); v.AsInt() != src.Count || v.P != nil {
				t.Fatal("integer is not normalized")
			}
			borrowed := r.GetValue(2)
			owned := r.GetOwnedString(2)
			if owned.AsString() != src.Name {
				t.Fatal("string mismatch")
			}
			if (owned.P == borrowed.P) != r.OwnsData() {
				t.Fatal("incorrect string ownership/copy")
			}
			ctx, scope := evm.WithRecords(context.Background())
			vm := evm.New(program)
			out := make([]values.Value, 1)
			if err := vm.Run(ctx, "Read", []values.Value{scope.ReadRecord(model, r)}, out); err != nil {
				t.Fatal(err)
			}
			scope.Close()
			if out[0].AsString() != borrowed.AsString() {
				t.Fatal("EVM string value changed")
			}
			clear(raw)
			if owned.AsString() != src.Name || out[0].AsString() != src.Name {
				t.Fatal("retained string borrowed destroyed transaction data")
			}
			if !r.OwnsData() {
				return
			}
			input := []byte("replacement string")
			if err := r.Edit().TrySetValue(2, values.Bytes(input)); err != nil {
				t.Fatal(err)
			}
			clear(input)
			if r.GetValue(2).AsString() != "replacement string" {
				t.Fatal("editor retained borrowed input")
			}
			if owned.AsString() != want || out[0].AsString() != want {
				t.Fatal("edit mutated previously returned string")
			}
		})
	}
}
