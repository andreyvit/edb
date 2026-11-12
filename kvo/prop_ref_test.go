package kvo_test

import (
	"reflect"
	"slices"
	"sync"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type typedChild struct {
	Name string `kvo:"name" msgpack:"n"`
}
type typedRow struct {
	Points   int64         `kvo:"points" msgpack:"p"`
	Tags     []string      `kvo:"tags" msgpack:"t"`
	Child    *typedChild   `kvo:"child" msgpack:"c"`
	Children []*typedChild `kvo:"children" msgpack:"cs"`
}

func TestTypedPropertyAccess(t *testing.T) {
	s := kvo.NewSchema()
	child := kvo.NewEntityType(s, "child", reflect.TypeFor[typedChild]())
	root := kvo.NewEntityType(s, "row", reflect.TypeFor[typedRow]())
	name := kvo.NewStringProp[string](s, 1, "child:name", kvo.TMachineString)
	points := kvo.NewSignedProp[int64](s, 2, "row:points", kvo.TInt64)
	tags := kvo.NewListProp(s, 3, "row:tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	childProp := kvo.NewRecordProp(s, 4, "row:child", child)
	children := kvo.NewRecordListProp(s, 5, "row:children", kvo.ListOf(child))
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) { b.Prop(name) })
	m := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) {
		b.Prop(points)
		b.Prop(tags)
		b.Prop(childProp)
		b.Prop(children)
	})
	binding := kvo.Bind(reflect.TypeFor[typedRow](), m)
	row := &typedRow{-7, []string{"a", "b"}, &typedChild{"child"}, []*typedChild{{"one"}, {"two"}, {"three"}}}
	raw := msgpack.Encode(nil, reflect.ValueOf(row))
	fast := binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)))
	cases := []struct {
		name string
		r    kvo.Record
	}{{"messagepack", binding.MessagePack(raw)}, {"fast2", fast}, {"compact2", binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Compact2)))}, {"native", binding.Record(reflect.ValueOf(row))}, {"memory", kvo.Encoded(fast.Snapshot()).WithModel(m).Fastest()}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			read := func() {
				if points.Get(c.r) != -7 || c.r.Get(points) != -7 {
					t.Fatal("points")
				}
				it := c.r.Get(tags).Cursor()
				if it.Len() != 2 || c.r.Get(tags).Len() != 2 {
					t.Fatal("list length")
				}
				for _, want := range []string{"a", "b"} {
					got, ok := it.Next()
					if !ok || got != want {
						t.Fatal(got, ok)
					}
				}
				if it.Len() != 0 {
					t.Fatal("remaining length")
				}
				if _, ok := it.Next(); ok {
					t.Fatal("exhaustion")
				}
				if name.Get(c.r.Get(childProp)) != "child" {
					t.Fatal("child")
				}
				n := 0
				for tag := range c.r.Get(tags).Iter() {
					if tag != "a" && tag != "b" {
						t.Fatal(tag)
					}
					n++
				}
				if n != 2 {
					t.Fatal(n)
				}
				n = 0
				for object := range c.r.Get(children).Iter() {
					if name.Get(object) == "" {
						t.Fatal("empty object")
					}
					n++
				}
				if n != 3 {
					t.Fatal(n)
				}
				objects := c.r.Get(children).Cursor()
				for _, want := range []string{"one", "two", "three"} {
					obj, ok := objects.Next()
					if !ok || name.Get(obj) != want {
						t.Fatal("objects", obj, ok)
					}
				}
				if _, ok := objects.Next(); ok {
					t.Fatal("object exhaustion")
				}
			}
			read()
			if n := testing.AllocsPerRun(100, read); n != 0 {
				t.Fatalf("%g read allocations", n)
			}
			if got := c.r.Get(tags).Slice(); !slices.Equal(got, []string{"a", "b"}) {
				t.Fatal(got)
			}
			seq := c.r.Get(tags).Iter()
			for v := range seq {
				if v != "a" {
					t.Fatal(v)
				}
				break
			}
			for v := range seq {
				if v != "b" {
					t.Fatal(v)
				}
			}
			for range seq {
				t.Fatal("restarted sequence")
			}
			objs := c.r.Get(children).Slice()
			if len(objs) != 3 || name.Get(objs[2]) != "three" {
				t.Fatal("object slice")
			}
			edit := c.r.Edit()
			if err := edit.TrySet(childProp, c.r); err == nil {
				t.Fatal("wrong child model accepted")
			}
			if edit.Get(childProp).Get(name) != "child" {
				t.Fatal("failed assignment changed child")
			}

			edit.Set(points, int64(12))
			if edit.Get(points) != 12 {
				t.Fatal("typed edit")
			}
			points.Set(edit, -7)
			edit.Set(tags, tags.FromSlice([]string{"new", "list"}))
			got := edit.Get(tags).Cursor()
			if v, ok := got.Next(); !ok || v != "new" {
				t.Fatal("list write", v, ok)
			}
			tags.SetSlice(edit, []string{"a", "b"})
			if got := edit.Get(tags).Slice(); !slices.Equal(got, []string{"a", "b"}) {
				t.Fatal("slice assignment", got)
			}
		})
	}
	if unsafe.Sizeof(points) != 8 || unsafe.Sizeof(name) != 8 || unsafe.Sizeof(kvo.Record{}) != 16 {
		t.Fatal("handle size")
	}
	for _, f := range []func(){func() { kvo.BindStringProp[string](s, points.Code()) }, func() { kvo.BindSignedProp[int32](s, points.Code()) }, func() { kvo.NewSignedProp[int64](s, points.Code(), "duplicate", kvo.TInt64) }} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid property accepted")
				}
			}()
			f()
		}()
	}
}

func TestZeroTypedLists(t *testing.T) {
	var strings kvo.List[string]
	var objects kvo.RecordList
	if strings.Len() != 0 || objects.Len() != 0 || strings.Slice() != nil || objects.Slice() != nil {
		t.Fatal("zero list")
	}
	for range strings.Iter() {
		t.Fatal("zero scalar list element")
	}
	for range objects.Iter() {
		t.Fatal("zero object list element")
	}
}

func TestTypedListConcurrentRegistration(t *testing.T) {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "concurrentList", reflect.TypeFor[typedListRow]())
	tags := kvo.NewListProp(s, 1, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	model := kvo.NewModel(s, typ, func(m *kvo.ModelBuilder) { m.Prop(tags) })
	b := kvo.Bind(reflect.TypeFor[typedListRow](), model)
	row := &typedListRow{Tags: []string{"one", "two"}}
	encoded := b.Encoded(row2.Trusted(b.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)))
	rows := []kvo.Record{encoded, b.Record(reflect.ValueOf(row)), b.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row)))}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 100 {
				for _, r := range rows {
					l := r.Get(tags)
					if l.Len() != 2 {
						t.Error("length")
					}
					it := l.Cursor()
					for _, want := range row.Tags {
						if got, ok := it.Next(); !ok || got != want {
							t.Error("element")
						}
					}
				}
			}
		})
	}
	wg.Wait()
}
