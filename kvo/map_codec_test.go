package kvo_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestRecordAndNestedMaps(t *testing.T) {
	type child struct {
		Name string `kvo:"name"`
	}
	type row struct {
		Children map[string]*child          `kvo:"children"`
		Counts   map[uint64]map[int32]int64 `kvo:"counts"`
	}
	s := kvo.NewSchema()
	ct := kvo.NewEntityType(s, "MapChild", reflect.TypeFor[child]())
	rt := kvo.NewEntityType(s, "MapRow", reflect.TypeFor[row]())
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	children := kvo.NewRecordMapProp(s, 2, "children", kvo.MapOf(kvo.TMachineString, ct), values.StringCodec[string]())
	inner := kvo.NewMapCodec(kvo.MapOf(kvo.TInt32, kvo.TInt64), values.SignedCodec[int32](), values.SignedCodec[int64]())
	counts := kvo.NewNestedMapCodec(kvo.TUint64, values.UnsignedCodec[uint64](), inner).Prop(s, 3, "counts")
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name) })
	m := kvo.NewModel(s, rt, func(b *kvo.ModelBuilder) { b.Prop(children); b.Prop(counts, kvo.KeepDictionaryZeroValues) })
	b := kvo.Bind(reflect.TypeFor[row](), m)
	src := &row{map[string]*child{"a": {"Alice"}}, map[uint64]map[int32]int64{7: {-1: 4}}}
	for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2"} {
		t.Run(format, func(t *testing.T) {
			var r kvo.Record
			switch format {
			case "native":
				r = b.Record(reflect.ValueOf(src))
			case "MessagePack":
				r = b.MessagePack(msgpack.Encode(nil, reflect.ValueOf(src)))
			default:
				layout := row2.Fast2
				if format == "Compact2" {
					layout = row2.Compact2
				}
				r = b.Encoded(row2.Open(b.Encode(nil, reflect.ValueOf(src).Elem(), layout)))
			}
			read := func() {
				if r.Get(children).Get("a").Get(name) != "Alice" || r.Get(counts).Get(7).Get(-1) != 4 {
					t.Fatal("map values")
				}
			}
			read()
			if format != "native" {
				if n := testing.AllocsPerRun(100, read); n != 0 {
					t.Fatal(n)
				}
			}
			e := r.Clone().Edit()
			outer := e.Map(counts)
			innerEdit := kvo.EditMap(outer, uint64(7))
			innerEdit.Set(-2, 8)
			kvo.NewMap(outer, uint64(8)).Set(1, 0)
			if e.Get(counts).Get(7).Get(-2) != 8 {
				t.Fatal("nested write missing")
			}
			first := e.Record().Clone()
			innerEdit.Set(-2, 9)
			var saved kvo.Record
			if format == "MessagePack" {
				saved = b.MessagePack(e.MessagePack())
			} else {
				saved = kvo.Encoded(e.Snapshot()).WithModel(m)
			}
			if saved.Get(counts).Get(7).Get(-2) != 9 || first.Get(counts).Get(7).Get(-2) != 8 {
				t.Fatal("nested snapshot")
			}
			if _, ok := saved.Get(counts).Get(8).Lookup(1); !ok {
				t.Fatal("nested zero lost")
			}
			outer.Delete(7)
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("stale nested editor accepted")
					}
				}()
				innerEdit.Set(1, 3)
			}()
		})
	}
}

func TestJSONDictionaryEntryEdits(t *testing.T) {
	type row struct {
		Extras map[string]any `kvo:"extras"`
	}
	s := kvo.NewSchema()
	prop := kvo.NewJSONMapProp(s, 1, "extras", kvo.TMachineString, values.StringCodec[string]())
	typ := kvo.NewEntityType(s, "DynamicMap", reflect.TypeFor[row]())
	m := kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) { b.Prop(prop, kvo.KeepDictionaryZeroValues) })
	b := kvo.Bind(reflect.TypeFor[row](), m)
	for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2"} {
		t.Run(format, func(t *testing.T) {
			src := &row{map[string]any{"keep": map[string]any{"big": uint64(1<<63 + 7)}, "zero": int64(0)}}
			var r kvo.Record
			switch format {
			case "native":
				r = b.Record(reflect.ValueOf(src))
			case "MessagePack":
				r = b.MessagePack(msgpack.Encode(nil, reflect.ValueOf(src)))
			default:
				layout := row2.Fast2
				if format == "Compact2" {
					layout = row2.Compact2
				}
				r = b.Encoded(row2.Open(b.Encode(nil, reflect.ValueOf(src).Elem(), layout)))
			}
			keysOnly := func() {
				m := r.Get(prop)
				if m.Len() != 2 {
					t.Fatal("dictionary length")
				}
				n := 0
				for range m.Keys() {
					n++
				}
				if n != 2 {
					t.Fatal("dictionary keys")
				}
			}
			if n := testing.AllocsPerRun(100, keysOnly); n != 0 {
				t.Fatal("key traversal materialized JSON values", n)
			}
			e := r.Edit()
			x := e.Map(prop)
			x.SetNull("null")
			x.Set("added", map[string]any{"value": "test"})
			x.Delete("zero")
			var saved kvo.Record
			if format == "MessagePack" {
				saved = b.MessagePack(e.MessagePack())
			} else {
				saved = kvo.Encoded(e.Snapshot()).WithModel(m)
			}
			if v, ok := saved.Get(prop).Lookup("null"); !ok || v != nil {
				t.Fatal("null lost", v, ok)
			}
			if _, ok := saved.Get(prop).Lookup("zero"); ok {
				t.Fatal("deletion lost")
			}
			if saved.Get(prop).Get("keep").(map[string]any)["big"] != uint64(1<<63+7) {
				t.Fatal("integer precision lost")
			}
			if saved.Get(prop).Get("added").(map[string]any)["value"] != "test" {
				t.Fatal("JSON write lost")
			}
		})
	}
}
