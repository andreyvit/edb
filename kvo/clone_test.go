package kvo_test

import (
	"bytes"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestRecordCloneAndOwnedCollections(t *testing.T) {
	s := kvo.NewSchema()
	child := kvo.NewEntityType(s, "child", reflect.TypeFor[typedChild]())
	root := kvo.NewEntityType(s, "row", reflect.TypeFor[typedRow]())
	name := kvo.NewStringProp[string](s, 1, "child:name", kvo.TMachineString)
	points := kvo.NewSignedProp[int64](s, 2, "row:points", kvo.TInt64)
	tags := kvo.NewListProp(s, 3, "row:tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	childProp := kvo.NewRecordProp(s, 4, "row:child", child)
	children := kvo.NewRecordListProp(s, 5, "row:children", kvo.ListOf(child))
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) { b.Prop(name) })
	m := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) { b.Prop(points); b.Prop(tags); b.Prop(childProp); b.Prop(children) })
	binding := kvo.Bind(reflect.TypeFor[typedRow](), m)
	for _, format := range []string{"Fast2", "Compact2", "MessagePack", "Native", "Memory"} {
		t.Run(format, func(t *testing.T) {
			tagData := []byte("first")
			source := &typedRow{7, []string{values.Bytes(tagData).AsString(), "second"}, &typedChild{"child"}, []*typedChild{{"one"}, {"two"}, {"three"}}}
			if format == "Native" || format == "MessagePack" {
				source.Children[1] = nil
			}
			var raw []byte
			var r kvo.Record
			switch format {
			case "Native":
				r = binding.Record(reflect.ValueOf(source))
			case "MessagePack":
				raw = msgpack.Encode(nil, reflect.ValueOf(source))
				raw = msgpack.Trusted(raw).Set("unknown", []byte{0xa1, 'x'}).Raw()
				r = binding.MessagePack(raw)
			default:
				layout := row2.Compact2
				if format != "Compact2" {
					layout = row2.Fast2
				}
				raw = binding.Encode(nil, reflect.ValueOf(source).Elem(), layout)
				e := row2.Edit(row2.Trusted(raw))
				e.Set([]row2.Step{{Key: 999}}, values.Bytes([]byte("unknown")), row2.Data)
				raw = e.Snapshot().Raw()
				r = binding.Encoded(row2.Trusted(raw))
				if format == "Memory" {
					r = kvo.Encoded(r.Snapshot()).WithModel(m).Fastest()
				}
			}
			copy := r.Clone()
			if !copy.OwnsData() || copy.Model() != m || copy.Get(points) != 7 {
				t.Fatal("clone metadata/value")
			}
			ownedTags := r.Get(tags).OwnedSlice()
			objects := r.Get(children).OwnedSlice()
			clonedObjects := r.Get(children).CloneSlice()
			if len(objects) != 3 || objects[1].Missing() != (source.Children[1] == nil) || clonedObjects[1].Missing() != (source.Children[1] == nil) {
				t.Fatal("nil collection element lost")
			}
			// Cursor materialization starts at its current position, independently.
			cursor := r.Get(tags).Cursor()
			cursor.Next()
			if got := cursor.OwnedSlice(); !slices.Equal(got, []string{"second"}) || cursor.Len() != 1 {
				t.Fatal("cursor position", got)
			}
			if format == "Native" || format == "Memory" {
				r.Edit().Set(points, int64(99))
				r.Get(childProp).Edit().Set(name, "source changed")
				if copy.Get(points) != 7 || copy.Get(childProp).Get(name) != "child" {
					t.Fatal("clone aliases mutable source")
				}
			}
			source.Children[0].Name = "source changed"
			clear(tagData)
			clear(raw)
			if !slices.Equal(ownedTags, []string{"first", "second"}) || copy.Get(tags).Slice()[0] != "first" {
				t.Fatal("retained string borrows source")
			}
			if clonedObjects[0].Get(name) != "one" {
				t.Fatal("cloned collection aliases source")
			}
			if format != "Native" && objects[0].Get(name) != "one" {
				t.Fatal("owned record borrows encoded source")
			}
			if format == "Native" && objects[0].Get(name) != "source changed" {
				t.Fatal("Own unexpectedly cloned native identity")
			}
			if format == "MessagePack" {
				if string(msgpack.Trusted(copy.MessagePack()).Lookup("unknown").Bytes()) != "x" {
					t.Fatal("lost unknown msgpack field")
				}
			} else if format != "Native" {
				if string(copy.GetAs(999, row2.Data).AsBytes()) != "unknown" {
					t.Fatal("lost unknown field")
				}
			}
			edit := copy.Edit()
			edit.Set(points, int64(42))
			if edit.Get(points) != 42 {
				t.Fatal("clone is not editable")
			}
		})
	}
	if !(kvo.Record{}).Clone().Missing() {
		t.Fatal("missing clone")
	}
}

type cloneGraph struct {
	Name          string `kvo:"name"`
	Self          *cloneGraph
	Shared, Other *typedChild
	Data          []byte
	Map           map[string]any
	Private       []byte
	private       []byte
	When          time.Time
	Unsupported   chan int
}

func TestNativeClonePreservesDataGraph(t *testing.T) {
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "graph")
	kvo.NewProp(s, 1, "name", kvo.TMachineString, nil)
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	p := kvo.Project(reflect.TypeFor[cloneGraph](), m, kvo.FieldMap{reflect.TypeFor[cloneGraph](): {1: "Name"}})
	data := []byte{1, 2, 3, 4}
	g := &cloneGraph{Name: "before", Data: data[:2], private: []byte{5}, When: time.Now()}
	g.Self = g
	g.Shared = &typedChild{"shared"}
	g.Other = g.Shared
	g.Map = map[string]any{"child": g.Shared, "slice": []string{"original"}}
	g.Map["self"] = g.Map
	r := p.Record(reflect.ValueOf(g))
	if r.Own().Value() != r.Value() || r.Fastest().Value() != r.Value() {
		t.Fatal("retention/access changed native identity")
	}
	c := r.Clone().Native().(*cloneGraph)
	if c == g || c.Self != c || c.Shared != c.Other || c.Shared == g.Shared || c.Map["child"] != c.Shared || !c.When.Equal(g.When) {
		t.Fatal("clone type/graph")
	}
	c.Data = c.Data[:cap(c.Data)]
	c.Data[3] = 9
	c.Shared.Name = "clone"
	c.private[0] = 9
	c.Map["slice"].([]string)[0] = "clone"
	c.Map["self"].(map[string]any)["new"] = true
	if data[3] != 4 || g.Shared.Name != "shared" || g.private[0] != 5 || g.Map["slice"].([]string)[0] != "original" || g.Map["new"] != nil {
		t.Fatal("native clone shares mutable data")
	}
	if c.Map["new"] != true {
		t.Fatal("map cycle lost")
	}
	g.Unsupported = make(chan int)
	defer func() {
		if recover() == nil {
			t.Fatal("cloned a live channel")
		}
	}()
	r.Clone()
}

func TestOwnedByteCollection(t *testing.T) {
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "bytes")
	prop := kvo.NewListProp(s, 1, "bytes", kvo.ListOf(kvo.TBytes), values.BytesCodec[[]byte]())
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) { b.Prop(prop) })
	data := [][]byte{{1, 2}, nil, {3}}
	l := prop.FromSlice(data)
	r := kvo.Encoded(row2.Empty(row2.Fast2)).WithModel(m)
	edit := r.Edit()
	edit.Set(prop, l)
	raw := bytes.Clone(edit.Snapshot().Raw())
	l = kvo.Encoded(row2.Trusted(raw)).WithModel(m).Get(prop)
	owned := l.OwnedSlice()
	clear(raw)
	if !bytes.Equal(owned[0], []byte{1, 2}) || !bytes.Equal(owned[2], []byte{3}) {
		t.Fatal("byte elements borrow source")
	}
}

func BenchmarkRecordRetention(b *testing.B) {
	_, _, fixtures := recordHandleFixtures(b)
	for _, f := range fixtures {
		r := f.open(7)
		b.Run(f.name+"/Own", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				retainedRecord = r.Own()
			}
		})
		b.Run(f.name+"/Clone", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				retainedRecord = r.Clone()
			}
		})
	}
}
