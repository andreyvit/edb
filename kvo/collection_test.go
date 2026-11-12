package kvo_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestSharedListCell(t *testing.T) {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "listCell", reflect.TypeFor[typedListRow]())
	tags := kvo.NewListProp(s, 1, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	model := kvo.NewModel(s, typ, func(m *kvo.ModelBuilder) { m.Prop(tags) })
	b := kvo.Bind(reflect.TypeFor[typedListRow](), model)
	dynamic := kvo.BindListAccess(model, 1)
	if dynamic != kvo.BindListAccess(model, 1) {
		t.Fatal("access metadata was not reused")
	}
	if unsafe.Sizeof(kvo.ListCell{}) != 16 || unsafe.Sizeof(kvo.List[string]{}) != 16 || unsafe.Sizeof(kvo.RecordList{}) != 16 {
		t.Fatal("cell size")
	}
	for _, items := range [][]string{nil, {}, {"one", "two", "three"}} {
		row := &typedListRow{Tags: items}
		fast := b.Encoded(row2.Trusted(b.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)))
		for _, r := range []kvo.Record{b.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row))), fast, b.Encoded(row2.Trusted(b.Encode(nil, reflect.ValueOf(row).Elem(), row2.Compact2))), b.Record(reflect.ValueOf(row)), kvo.Encoded(fast.Snapshot()).WithModel(model).Fastest()} {
			read := func() {
				fromGo := r.Get(tags).Value()
				cell, ok := dynamic.TryCell(fromGo)
				if !ok || cell.Len() != len(items) || cell.Value() != fromGo {
					t.Fatal("typed cell transport")
				}
				for _, v := range []values.Value{fromGo, dynamic.Get(r).Value()} {
					c := kvo.ListCellFromValue(v)
					it := c.Iter()
					for i, want := range items {
						got, ok := it.Next()
						if !ok || got.AsString() != want || c.At(i).AsString() != want {
							t.Fatal("element")
						}
					}
					if _, ok := it.Next(); ok {
						t.Fatal("end")
					}
					if _, ok := kvo.TryFromValue(v); ok {
						t.Fatal("list accepted as a record")
					}
				}
				if _, ok := kvo.TryListCell(r.Value()); ok {
					t.Fatal("record accepted as a list")
				}
			}
			read()
			if n := testing.AllocsPerRun(100, read); n != 0 {
				t.Fatal("allocations", n)
			}
		}
	}
	other := kvo.NewSchema()
	ot := kvo.NewEntityType(other, "numbers")
	kvo.NewProp(other, 1, "nums", kvo.ListOf(kvo.TInt64), nil)
	om := kvo.NewModel(other, ot, func(m *kvo.ModelBuilder) { m.Prop(other.MustPropByCode(1)) })
	text := dynamic.Get(b.Record(reflect.ValueOf(&typedListRow{Tags: []string{"text"}})))
	if _, ok := kvo.BindListAccess(om, 1).TryCell(text.Value()); ok {
		t.Fatal("wrong element type accepted")
	}
	if _, ok := kvo.TryListCell(values.String("text")); ok {
		t.Fatal("ordinary span accepted")
	}
	var zero kvo.ListCell
	if zero.Len() != 0 {
		t.Fatal("zero length")
	}
	it := zero.Iter()
	if _, ok := it.Next(); ok {
		t.Fatal("zero element")
	}
}
