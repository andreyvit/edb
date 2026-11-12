package kvo_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestAttachedCollectionEdits(t *testing.T) {
	type child struct {
		Name string `kvo:"name"`
	}
	type row struct {
		Name     string            `kvo:"name"`
		IDs      map[uint32]string `kvo:"ids"`
		Children map[string]child  `kvo:"children"`
		Items    []child           `kvo:"items"`
		Tags     []string          `kvo:"tags"`
	}
	s := kvo.NewSchema()
	ct := kvo.NewEntityType(s, "CollectionChild", reflect.TypeFor[child]())
	rt := kvo.NewEntityType(s, "CollectionRow", reflect.TypeFor[row]())
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	ids := kvo.NewMapProp(s, 2, "ids", kvo.MapOf(kvo.TUint32, kvo.TMachineString), values.UnsignedCodec[uint32](), values.StringCodec[string]())
	children := kvo.NewRecordMapProp(s, 3, "children", kvo.MapOf(kvo.TMachineString, ct), values.StringCodec[string]())
	tags := kvo.NewListProp(s, 4, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	items := kvo.NewRecordListProp(s, 5, "items", kvo.ListOf(ct))
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name) })
	m := kvo.NewModel(s, rt, func(b *kvo.ModelBuilder) {
		b.Prop(name)
		b.Prop(ids, kvo.KeepDictionaryZeroValues)
		b.Prop(children)
		b.Prop(tags)
		b.Prop(items)
	})
	b := kvo.Bind(reflect.TypeFor[row](), m)
	for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2", "memory"} {
		t.Run(format, func(t *testing.T) {
			src := &row{Name: "keep", IDs: map[uint32]string{1: "one"}, Children: map[string]child{"a": {"Alice"}}, Tags: []string{"vip", "old"}}
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
				r = kvo.Encoded(row2.Open(b.Encode(nil, reflect.ValueOf(src).Elem(), layout))).WithModel(m)
				if format == "memory" {
					r = r.Fastest()
				}
			}
			e := r.Edit()
			objects := e.List(items)
			firstObject := objects.New()
			firstObject.Set(name, "first")
			objects.New().Set(name, "second")
			if err := firstObject.TrySet(name, "stale"); err == nil {
				t.Fatal("structural list change did not detach child")
			}
			objects.Edit(0).Set(name, "edited")
			idsEdit := e.Map(ids)
			if idsEdit != e.Map(ids) {
				t.Fatal("not sharing edit buffer")
			}
			idsEdit.Set(2, "")
			idsEdit.Set(3, "first")
			idsEdit.Set(3, "last")
			idsEdit.Delete(1)
			if v, ok := idsEdit.Lookup(2); !ok || v != "" {
				t.Fatal("zero entry missing")
			}
			if e.Get(ids).Get(3) != "last" {
				t.Fatal("typed read misses pending write")
			}
			list := e.List(tags)
			if list.AddUnique("vip") || !list.AddUnique("new") || !list.Remove("old") {
				t.Fatal("list update")
			}
			ce := e.Map(children)
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("wrong map entry model accepted")
					}
				}()
				ce.Set("bad", r)
			}()
			if _, ok := ce.Lookup("bad"); ok {
				t.Fatal("failed assignment mutated dictionary")
			}
			c := ce.Edit("a")
			c.Set(name, "Annie")
			ce.New("b").Set(name, "Bob")
			first := e.Snapshot()
			objects.Edit(1).Set(name, "updated second")
			c.Set(name, "Anna")
			idsEdit.Set(3, "after snapshot")
			if kvo.Encoded(first).WithModel(m).Get(children).Get("a").Get(name) != "Annie" {
				t.Fatal("snapshot changed")
			}
			var saved kvo.Record
			if format == "MessagePack" {
				saved = b.MessagePack(e.MessagePack())
			} else {
				saved = kvo.Encoded(e.Snapshot()).WithModel(m)
			}
			if saved.Get(items).Slice()[0].Get(name) != "edited" || saved.Get(items).Slice()[1].Get(name) != "updated second" {
				t.Fatal("record list edits lost")
			}
			objects.RemoveAt(0)
			if objects.Get(0).Get(name) != "updated second" {
				t.Fatal("record list removal")
			}
			if saved.Get(name) != "keep" || saved.Get(ids).Len() != 2 || saved.Get(ids).Get(3) != "after snapshot" || saved.Get(children).Get("a").Get(name) != "Anna" || saved.Get(children).Get("b").Get(name) != "Bob" {
				t.Fatal("save lost edits")
			}
			if got := saved.Get(tags).Slice(); !reflect.DeepEqual(got, []string{"vip", "new"}) {
				t.Fatal(got)
			}
			if format == "native" && (src.Children["a"].Name != "Anna" || src.IDs[3] != "after snapshot" || len(src.Tags) != 2) {
				t.Fatal("native map entry not written through")
			}
			ce.Delete("a")
			if err := c.TrySet(name, "stale"); err == nil {
				t.Fatal("removed child still attached")
			}
			idsEdit.Clear()
			if !e.Has(ids) || e.Get(ids).Len() != 0 {
				t.Fatal("clear should retain empty dictionary")
			}
			e.Delete(ids)
			if e.Has(ids) {
				t.Fatal("deleted property remains")
			}
			if !e.Get(ids).Missing() {
				t.Fatal("deleted dictionary not missing")
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("stale map edit accepted")
					}
				}()
				idsEdit.Set(4, "stale")
			}()
			e.Set(tags, tags.FromSlice([]string{"replacement"}))
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("stale list edit accepted")
					}
				}()
				list.Append("stale")
			}()
			e.Set(ids, ids.FromMap(nil))
			e.Set(tags, tags.FromSlice(nil))
			if !e.Get(ids).Missing() || !e.Get(tags).Missing() {
				t.Fatal("nil collection assignment became empty")
			}
			e.Map(ids).Clear()
			e.List(tags).Clear()
			if e.Get(ids).Missing() || e.Get(tags).Missing() {
				t.Fatal("empty collection became absent")
			}
		})
	}
}
