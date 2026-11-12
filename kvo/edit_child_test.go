package kvo_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
)

func TestAttachedChildEdits(t *testing.T) {
	type child struct {
		Name  string `kvo:"name"`
		Count int64  `kvo:"count"`
	}
	type row struct {
		Child *child `kvo:"child"`
		Name  string `kvo:"name"`
	}
	s := kvo.NewSchema()
	ct := kvo.NewEntityType(s, "Child", reflect.TypeFor[child]())
	rt := kvo.NewEntityType(s, "Parent", reflect.TypeFor[row]())
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	count := kvo.NewSignedProp[int64](s, 2, "count", kvo.TInt64)
	cp := kvo.NewRecordProp(s, 3, "child", ct)
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name); b.Prop(count) })
	m := kvo.NewModel(s, rt, func(b *kvo.ModelBuilder) { b.Prop(name); b.Prop(cp) })
	b := kvo.Bind(reflect.TypeFor[row](), m)
	for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2", "memory"} {
		t.Run(format, func(t *testing.T) {
			src := &row{Name: "untouched"}
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
			if e.Edit(cp) != nil {
				t.Fatal("missing child was created")
			}
			c := e.New(cp)
			c.Set(name, "Alice")
			c.Set(count, 0)
			first := e.Snapshot()
			c.Set(count, 7)
			if e.Get(cp).Get(count) != 7 || e.Get(name) != "untouched" {
				t.Fatal("child detached")
			}
			if kvo.Encoded(first).WithModel(m).Get(cp).Get(count) != 0 {
				t.Fatal("snapshot changed")
			}
			if format == "native" && (src.Child == nil || src.Child.Count != 7) {
				t.Fatal("native edit did not write through")
			}
			e.Set(cp, kvo.Record{})
			if err := c.TrySet(count, 8); err == nil {
				t.Fatal("replaced child accepted mutation")
			}
			if e.Edit(cp) != nil {
				t.Fatal("cleared child remains")
			}
			fresh := e.EditOrCreate(cp)
			fresh.Set(count, 9)
			_ = e.Snapshot()
			fresh.Set(count, 10)
			if e.Get(cp).Get(count) != 10 {
				t.Fatal("snapshot detached live child")
			}
		})
	}
}
