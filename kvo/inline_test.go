package kvo_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type InlineValues struct {
	Points int64         `msgpack:"p"`
	Tags   []string      `msgpack:"tags"`
	Hidden countedString `msgpack:"hidden"`
}
type InlineRow struct {
	Tenant       uint64 `msgpack:"t"`
	InlineValues `msgpack:",inline"`
	Unmodeled    string `msgpack:"extra"`
}

func inlineProjection() *kvo.Projection {
	s := kvo.NewSchema()
	child := kvo.NewEntityType(s, "stats")
	root := kvo.NewEntityType(s, "row")
	kvo.NewProp(s, 1, "tenant", kvo.TUint64, nil)
	kvo.NewProp(s, 2, "stats", child, nil)
	kvo.NewProp(s, 3, "points", kvo.TInt64, nil)
	kvo.NewProp(s, 4, "tags", kvo.ListOf(kvo.TMachineString), nil)
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(3)); b.Prop(s.MustPropByCode(4)) })
	m := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)); b.Prop(s.MustPropByCode(2)) })
	return kvo.Project(reflect.TypeFor[InlineRow](), m, kvo.FieldMap{
		reflect.TypeFor[InlineRow]():    {1: "Tenant", 2: "InlineValues"},
		reflect.TypeFor[InlineValues](): {3: "Points", 4: "Tags"},
	})
}
func TestInlineMessagePackBorrowedAccess(t *testing.T) {
	p := inlineProjection()
	original := InlineRow{7, InlineValues{13, []string{"one", "two"}, "private"}, "keep"}
	raw := msgpack.Encode(nil, reflect.ValueOf(&original))
	frozen := bytes.Clone(raw)
	if !p.DirectMessagePack() {
		t.Fatal("inline struct unnecessarily decodes")
	}
	hiddenDecodes = 0
	read := func() {
		r := p.MessagePack(raw)
		c := r.Child(row2.Step{Key: 2})
		if r.GetValue(1).AsUint() != 7 || c.GetValue(3).AsInt() != 13 {
			t.Fatal("scalar access")
		}
		it := c.ListValue(4).Iter()
		for _, want := range []string{"one", "two"} {
			v, ok := it.Next()
			if !ok || v.AsString() != want {
				t.Fatal("list access")
			}
		}
		if _, ok := it.Next(); ok {
			t.Fatal("list end")
		}
	}
	read()
	if n := testing.AllocsPerRun(100, read); n != 0 {
		t.Fatalf("%g allocations", n)
	}
	if hiddenDecodes != 0 {
		t.Fatal("decoded unrequested fields")
	}
	r := p.MessagePack(raw)
	e := r.Edit()
	e.FastSetPathValue([]row2.Step{{Key: 2}, {Key: 3}}, values.Int(29))
	if hiddenDecodes != 0 {
		t.Fatal("edit decoded unrequested fields")
	}
	var got InlineRow
	msgpack.Decode(e.MessagePack(), reflect.ValueOf(&got))
	if got.Points != 29 || got.Tenant != 7 || got.Hidden != "private" || got.Unmodeled != "keep" || !reflect.DeepEqual(got.Tags, original.Tags) {
		t.Fatalf("%+v", got)
	}
	if !bytes.Equal(raw, frozen) {
		t.Fatal("mutated borrowed bytes")
	}
	// The normalized snapshot preserves the logical child, not the flat wire layout.
	snapshot := kvo.Encoded(r.Snapshot()).WithModel(r.Model())
	if snapshot.Child(row2.Step{Key: 2}).GetValue(3).AsInt() != 13 {
		t.Fatal("snapshot")
	}
	for _, format := range []kvo.Record{r, r.Fastest(), snapshot, snapshot.Fastest()} {
		list := format.Child(row2.Step{Key: 2}).ListValue(4)
		if list.Len() != 2 || list.At(1).AsString() != "two" {
			t.Fatal("list representation")
		}
		if n := testing.AllocsPerRun(100, func() {
			it := list.Iter()
			for {
				_, ok := it.Next()
				if !ok {
					break
				}
			}
		}); n != 0 {
			t.Fatalf("list iteration: %g allocations", n)
		}
	}
}

// The standard decoder accepts a nested embedding alias as well as flattened
// fields. Their wire order matters, so unusual payloads must retain its merge
// semantics instead of silently returning only the flattened value.
func TestInlineMessagePackNestedAlias(t *testing.T) {
	p := inlineProjection()
	original := InlineRow{Tenant: 7, InlineValues: InlineValues{Points: 13}}
	raw := msgpack.Encode(nil, reflect.ValueOf(&original))
	alias := msgpack.Encode(nil, reflect.ValueOf(map[string]any{"p": int64(29)}))
	raw = msgpack.Trusted(raw).Set("InlineValues", alias).Raw()
	var want InlineRow
	msgpack.Decode(raw, reflect.ValueOf(&want))
	r := p.MessagePack(raw)
	if r.Child(row2.Step{Key: 2}).GetValue(3).AsInt() != want.Points {
		t.Fatal("nested alias changed decoder semantics")
	}
}

type ImplicitValues struct {
	Count int64 `msgpack:"n,omitempty"`
}
type ImplicitRow struct {
	ImplicitValues `msgpack:"values"`
	Extra          uint64 `msgpack:"extra"`
}
type ShadowedRow struct {
	ImplicitValues `msgpack:",inline"`
	Count          int64 `msgpack:"n"`
}

func TestMessagePackImplicitInlineAndShadowing(t *testing.T) {
	s := kvo.NewSchema()
	child := kvo.NewEntityType(s, "child")
	root := kvo.NewEntityType(s, "root")
	kvo.NewProp(s, 1, "child", child, nil)
	kvo.NewProp(s, 2, "count", kvo.TInt64, nil)
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(2)) })
	m := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	fields := kvo.FieldMap{reflect.TypeFor[ImplicitRow](): {1: "ImplicitValues"}, reflect.TypeFor[ShadowedRow](): {1: "ImplicitValues"}, reflect.TypeFor[ImplicitValues](): {2: "Count"}}
	p := kvo.Project(reflect.TypeFor[ImplicitRow](), m, fields)
	if !p.DirectMessagePack() {
		t.Fatal("implicit inline unnecessarily decodes")
	}
	raw := msgpack.Encode(nil, reflect.ValueOf(&ImplicitRow{ImplicitValues{9}, 17}))
	r := p.MessagePack(raw)
	if r.Child(row2.Step{Key: 1}).GetValue(2).AsInt() != 9 {
		t.Fatal("implicit inline")
	}
	e := r.Edit()
	e.FastSetPathValue([]row2.Step{{Key: 1}, {Key: 2}}, values.Int(31))
	var got ImplicitRow
	msgpack.Decode(e.MessagePack(), reflect.ValueOf(&got))
	if got.Count != 31 || got.Extra != 17 {
		t.Fatal(got)
	}
	p = kvo.Project(reflect.TypeFor[ShadowedRow](), m, fields)
	if p.DirectMessagePack() {
		t.Fatal("shadowed embedding needs decoder semantics")
	}
}

func TestReplaceInlineChildPreservesSiblings(t *testing.T) {
	s := kvo.NewSchema()
	child := kvo.NewEntityType(s, "child")
	root := kvo.NewEntityType(s, "root")
	kvo.NewProp(s, 1, "child", child, nil)
	kvo.NewProp(s, 2, "count", kvo.TInt64, nil)
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(2)) })
	m := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	p := kvo.Project(reflect.TypeFor[ImplicitRow](), m, kvo.FieldMap{reflect.TypeFor[ImplicitRow](): {1: "ImplicitValues"}, reflect.TypeFor[ImplicitValues](): {2: "Count"}})
	raw := msgpack.Encode(nil, reflect.ValueOf(&ImplicitRow{ImplicitValues{9}, 17}))
	e := p.MessagePack(raw).Edit()
	var w row2.Writer
	w.Begin(0, "", row2.Object, row2.Compact2)
	w.Put(2, "", values.Int(33), row2.Signed)
	w.End()
	e.SetValue(1, values.Bytes(w.BytesWritten()))
	var got ImplicitRow
	msgpack.Decode(e.MessagePack(), reflect.ValueOf(&got))
	if got.Count != 33 || got.Extra != 17 {
		t.Fatal(got)
	}
	e.SetValue(1, values.Bytes(row2.Empty(row2.Compact2).Raw()))
	got = ImplicitRow{}
	msgpack.Decode(e.MessagePack(), reflect.ValueOf(&got))
	if got.Count != 0 || got.Extra != 17 || !msgpack.Trusted(e.MessagePack()).Lookup("n").Missing() {
		t.Fatal("replacement did not remove omitted field", got)
	}
	if !msgpack.Trusted(e.MessagePack()).Lookup("values").Missing() {
		t.Fatal("replacement introduced nested embedding")
	}
}
