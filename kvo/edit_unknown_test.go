package kvo_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
)

func TestMessagePackCollectionEditsPreserveUnexposedFields(t *testing.T) {
	type child struct {
		Name   string `kvo:"name"`
		Hidden string `kvo:"-"`
	}
	type row struct {
		Hidden   string           `kvo:"-"`
		Children map[string]child `kvo:"children"`
		Items    []child          `kvo:"items"`
	}
	s := kvo.NewSchema()
	ct := kvo.NewEntityType(s, "PartialChild", reflect.TypeFor[child]())
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	children := kvo.NewRecordMapProp(s, 2, "children", kvo.MapOf(kvo.TMachineString, ct), values.StringCodec[string]())
	items := kvo.NewRecordListProp(s, 3, "items", kvo.ListOf(ct))
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name) })
	m := kvo.NewModel(s, kvo.NewEntityType(s, "PartialParent", reflect.TypeFor[row]()), func(b *kvo.ModelBuilder) { b.Prop(children); b.Prop(items) })
	p := kvo.Project(reflect.TypeFor[row](), m, nil)
	src := &row{"parent", map[string]child{"a": {"Alice", "kept a"}, "b": {"Bob", "kept b"}}, []child{{"Carol", "kept c"}}}
	raw := msgpack.Encode(nil, reflect.ValueOf(src))
	e := p.MessagePack(raw).Edit()
	e.Map(children).Edit("a").Set(name, "Annie")
	e.List(items).Edit(0).Set(name, "Caroline")
	_ = e.Get(children)
	e.Map(children).Edit("b").Set(name, "Bobby")
	var got row
	msgpack.Decode(e.MessagePack(), reflect.ValueOf(&got).Elem())
	if got.Hidden != "parent" || got.Children["a"].Hidden != "kept a" || got.Children["b"].Hidden != "kept b" || got.Items[0].Hidden != "kept c" {
		t.Fatal(got)
	}
	if got.Children["a"].Name != "Annie" || got.Children["b"].Name != "Bobby" || got.Items[0].Name != "Caroline" {
		t.Fatal(got)
	}
}

func TestCollectionAssignmentTranslatesMessagePackBindings(t *testing.T) {
	type destination struct {
		Name string `kvo:"name" msgpack:"n"`
	}
	type source struct {
		Name string `kvo:"name" msgpack:"other"`
	}
	type row struct {
		Entries map[string]destination `kvo:"entries"`
		Items   []destination          `kvo:"items"`
	}
	s := kvo.NewSchema()
	ct := kvo.NewEntityType(s, "SharedChild", reflect.TypeFor[destination](), reflect.TypeFor[source]())
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	entries := kvo.NewRecordMapProp(s, 2, "entries", kvo.MapOf(kvo.TMachineString, ct), values.StringCodec[string]())
	items := kvo.NewRecordListProp(s, 3, "items", kvo.ListOf(ct))
	cm := kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name) })
	m := kvo.NewModel(s, kvo.NewEntityType(s, "BindingParent", reflect.TypeFor[row]()), func(b *kvo.ModelBuilder) { b.Prop(entries); b.Prop(items) })
	p := kvo.Project(reflect.TypeFor[row](), m, nil)
	from := kvo.Project(reflect.TypeFor[source](), cm, nil).MessagePack(msgpack.Encode(nil, reflect.ValueOf(&source{"correct"})))
	e := p.MessagePack(msgpack.Encode(nil, reflect.ValueOf(&row{}))).Edit()
	e.Map(entries).Set("one", from)
	e.List(items).Append(from)
	var got row
	msgpack.Decode(e.MessagePack(), reflect.ValueOf(&got).Elem())
	if got.Entries["one"].Name != "correct" || len(got.Items) != 1 || got.Items[0].Name != "correct" {
		t.Fatal(got)
	}
}
