package kvo_test

import (
	"bytes"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

var hiddenDecodes int

type countedString string

func (s *countedString) UnmarshalMsgpack(b []byte) error {
	hiddenDecodes++
	*s = countedString(msgpack.Trusted(b).Bytes())
	return nil
}

type directChild struct {
	Number int64         `msgpack:"n"`
	Hidden countedString `msgpack:"h"`
}
type directRow struct {
	ID     uint64        `msgpack:"-"`
	Name   string        `msgpack:"n"`
	Child  *directChild  `msgpack:"c,omitempty"`
	Hidden countedString `msgpack:"h"`
}

func directProjection() *kvo.Projection {
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "row")
	c := kvo.NewEntityType(s, "child")
	kvo.NewProp(s, 1, "name", kvo.TMachineString, nil)
	kvo.NewProp(s, 2, "child", c, nil)
	kvo.NewProp(s, 3, "number", kvo.TInt64, nil)
	kvo.NewProp(s, 4, "id", kvo.TUint64, nil)
	kvo.NewModel(s, c, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(3)) })
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(1))
		b.Prop(s.MustPropByCode(2))
		b.Prop(s.MustPropByCode(4))
	})
	return kvo.Project(reflect.TypeFor[directRow](), m, kvo.FieldMap{reflect.TypeFor[directRow](): {1: "Name", 2: "Child", 4: "ID"}, reflect.TypeFor[directChild](): {3: "Number"}})
}
func TestMessagePackDirectAndFastest(t *testing.T) {
	original := directRow{9, "before", &directChild{-129, "private-child"}, "private-root"}
	raw, err := msgpack.TryEncode(nil, reflect.ValueOf(&original))
	if err != nil {
		t.Fatal(err)
	}
	p := directProjection()
	hiddenDecodes = 0
	r := p.MessagePack(raw)
	if r.GetValue(4).AsUint() != 0 || string(r.GetValue(1).AsBytes()) != "before" || r.Child(row2.Step{Key: 2}).GetValue(3).AsInt() != -129 || hiddenDecodes != 0 {
		t.Fatal("direct access decoded unrelated data")
	}
	frozen := bytes.Clone(raw)
	e := r.Edit()
	if err := e.TryFastSetPathValue([]row2.Step{{Key: 2}, {Key: 3}}, values.Int(81)); err != nil {
		t.Fatal(err)
	}
	if hiddenDecodes != 0 || e.GetPathValue([]row2.Step{{Key: 2}, {Key: 3}}).AsInt() != 81 {
		t.Fatal("edit decoded unrelated data")
	}
	if !bytes.Equal(raw, frozen) || r.Child(row2.Step{Key: 2}).GetValue(3).AsInt() != -129 {
		t.Fatal("borrowed input changed")
	}
	snapshot := bytes.Clone(e.MessagePack())
	if err := e.TrySetValue(1, values.Bytes([]byte("after"))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msgpack.Trusted(e.MessagePack()).Lookup("h").Raw(), msgpack.Trusted(raw).Lookup("h").Raw()) {
		t.Fatal("hidden field rewritten")
	}
	var got directRow
	if err := msgpack.TryDecode(e.MessagePack(), reflect.ValueOf(&got)); err != nil {
		t.Fatal(err)
	}
	if got.Child.Number != 81 || got.Hidden != original.Hidden || got.Child.Hidden != original.Child.Hidden || got.Name != "after" {
		t.Fatal(got)
	}
	if string(msgpack.Trusted(snapshot).Lookup("n").Bytes()) != "before" {
		t.Fatal("snapshot changed")
	}
	hiddenDecodes = 0
	fast := r.Fastest()
	if hiddenDecodes != 2 || fast.GetValue(4).AsUint() != 0 {
		t.Fatal("full decode invented omitted primary key", hiddenDecodes)
	}
	for range 5 {
		fast = fast.Fastest()
		if fast.Child(row2.Step{Key: 2}).GetValue(3).AsInt() != -129 {
			t.Fatal("decoded view")
		}
	}
	if hiddenDecodes != 2 {
		t.Fatal("decoded twice")
	}
	// Full decoding detaches all fields from the source bytes.
	clear(raw)
	if string(fast.GetValue(1).AsBytes()) != "before" {
		t.Fatal("decoded view borrowed strings")
	}
}
func TestFastestAllRowFormats(t *testing.T) {
	_, m := statrow.Model()
	b := kvo.Bind(reflect.TypeFor[statrow.State](), m)
	src := statrow.Sample(13)
	mp, err := msgpack.TryEncode(nil, reflect.ValueOf(src))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []kvo.Record{b.MessagePack(mp), b.Encoded(row2.Trusted(b.Encode(nil, reflect.ValueOf(src).Elem(), row2.Fast2))), b.Encoded(row2.Trusted(b.Encode(nil, reflect.ValueOf(src).Elem(), row2.Compact2)))} {
		fast := r.Fastest()
		if fast.Child(row2.Step{Key: 9}).GetValue(5).AsUint() != src.Profile.Balance || fast.GetValue(19).AsInt() != src.Earned {
			t.Fatal("decoded values")
		}
		a, err := r.TryToJSON(m, kvo.JSONOptions{})
		if err != nil {
			t.Fatal(err)
		}
		z, err := fast.TryToJSON(m, kvo.JSONOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, z) {
			t.Fatalf("decoded semantics differ: %#v / %#v", a, z)
		}
	}
	bare := kvo.Encoded(row2.Empty(row2.Fast2))
	if bare.Fastest().Missing() {
		t.Fatal("best effort changed record")
	}

}

type optionRow struct {
	Child kvo.Optional[directChild] `msgpack:"c"`
}

func TestMessagePackOptionalChild(t *testing.T) {
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "row")
	c := kvo.NewEntityType(s, "child")
	kvo.NewProp(s, 2, "child", c, nil)
	kvo.NewProp(s, 3, "number", kvo.TInt64, nil)
	kvo.NewModel(s, c, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(3)) })
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(2), kvo.PresenceIsSignificant) })
	p := kvo.Project(reflect.TypeFor[optionRow](), m, kvo.FieldMap{reflect.TypeFor[optionRow](): {2: "Child"}, reflect.TypeFor[directChild](): {3: "Number"}})
	src := optionRow{kvo.Optional[directChild]{Value: directChild{5, "private"}, Present: true}}
	raw, err := msgpack.TryEncode(nil, reflect.ValueOf(src))
	if err != nil {
		t.Fatal(err)
	}
	hiddenDecodes = 0
	r := p.MessagePack(raw)
	if !r.Has(2) || r.Child(row2.Step{Key: 2}).GetValue(3).AsInt() != 5 {
		t.Fatal("optional access")
	}
	ed := r.Edit()
	if err := ed.TryFastSetPathValue([]row2.Step{{Key: 2}, {Key: 3}}, values.Int(8)); err != nil {
		t.Fatal(err)
	}
	if hiddenDecodes != 0 {
		t.Fatal("decoded optional child")
	}
	var got optionRow
	if err := msgpack.TryDecode(ed.MessagePack(), reflect.ValueOf(&got)); err != nil {
		t.Fatal(err)
	}
	if !got.Child.Present || got.Child.Value.Number != 8 || got.Child.Value.Hidden != "private" {
		t.Fatal(got)
	}
	empty, _ := msgpack.TryEncode(nil, reflect.ValueOf(optionRow{}))
	ed = p.MessagePack(empty).Edit()
	if err := ed.TryFastSetPathValue([]row2.Step{{Key: 2}, {Key: 3}}, values.Int(9)); err != nil {
		t.Fatal(err)
	}
	if ed.GetPathValue([]row2.Step{{Key: 2}, {Key: 3}}).AsInt() != 9 {
		t.Fatal("created absent optional")
	}
}

// These values distinguish Go presence from encoded nonzero values, and cover
// explicit nil decoding through the per-property compatibility path.
func TestMessagePackDirectMatchesNative(t *testing.T) {
	type row struct {
		JSON      any          `msgpack:"j"`
		Array     [2]int64     `msgpack:"a"`
		Bytes     []byte       `msgpack:"b"`
		ByteArray [2]byte      `msgpack:"ba"`
		Float     float64      `msgpack:"f"`
		Narrow    float32      `msgpack:"n"`
		Time      time.Time    `msgpack:"t"`
		Count     uint64       `msgpack:"c"`
		Pointer   *directChild `msgpack:"p"`
	}
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "row")
	c := kvo.NewEntityType(s, "child")
	props := []struct {
		name, field string
		typ         kvo.AnyType
	}{
		{"json", "JSON", kvo.TJSON},
		{"array", "Array", kvo.ListOf(kvo.TInt64)},
		{"bytes", "Bytes", kvo.ListOf(kvo.NewIntType[uint8]("byte", nil))},
		{"byte_array", "ByteArray", kvo.ListOf(kvo.NewIntType[uint8]("array_byte", nil))},
		{"float", "Float", kvo.NewFloatType[float64]("float64", nil)},
		{"narrow", "Narrow", kvo.NewFloatType[float32]("float32", nil)},
		{"time", "Time", kvo.TTime},
		{"count", "Count", kvo.TUint64},
		{"pointer", "Pointer", c},
	}
	fields := make(map[kvo.PropCode]string)
	for i, prop := range props {
		key := uint64(i + 1)
		kvo.NewProp(s, key, prop.name, prop.typ, nil)
		fields[key] = prop.field
	}
	kvo.NewProp(s, 20, "number", kvo.TInt64, nil)
	kvo.NewModel(s, c, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(20)) })
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) {
		for i := range props {
			b.Prop(s.MustPropByCode(uint64(i + 1)))
		}
	})
	p := kvo.Project(reflect.TypeFor[row](), m, kvo.FieldMap{reflect.TypeFor[row](): fields, reflect.TypeFor[directChild](): {20: "Number"}})
	for _, src := range []row{{}, {JSON: false, Array: [2]int64{-3, 7}, Bytes: []byte{0, 255}, ByteArray: [2]byte{128, 0}, Float: math.Copysign(0, -1), Narrow: 1.25, Time: time.UnixMicro(987654).UTC(), Count: math.MaxUint64, Pointer: &directChild{Number: 31}}} {
		raw, err := msgpack.TryEncode(nil, reflect.ValueOf(&src))
		if err != nil {
			t.Fatal(err)
		}
		// Compare the full-decoder's semantics, not the pre-encoding Go value.
		native := p.MessagePack(raw).Fastest()
		direct := p.MessagePack(raw)
		for i := range props {
			key := uint64(i + 1)
			if direct.Has(key) != native.Has(key) {
				t.Fatalf("Has(%s)", props[i].name)
			}
		}
		a, err := direct.TryToJSON(m, kvo.JSONOptions{})
		if err != nil {
			t.Fatal(err)
		}
		b, err := native.TryToJSON(m, kvo.JSONOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("direct %#v, native %#v", a, b)
		}
		if direct.GetValue(5).AsUint() != native.GetValue(5).AsUint() {
			t.Fatal("lost signed zero")
		}
	}
	// Missing fixed arrays still have their statically declared length.
	empty := p.MessagePack([]byte{0x80})
	if row2.Trusted(empty.GetValue(2).AsBytes()).Len() != 2 || row2.Trusted(empty.GetValue(4).AsBytes()).Len() != 2 {
		t.Fatal("missing array length")
	}
	if empty.Child(row2.Step{Key: 9}).Fastest().Missing() != true {
		t.Fatal("absent child lost binding")
	}
	// Explicit nil must use an addressable destination with the pooled decoder.
	nilJSON := p.MessagePack([]byte{0x81, 0xa1, 'j', 0xc0})
	if nilJSON.Has(1) || string(nilJSON.GetValue(1).AsBytes()) != "null" {
		t.Fatal("nil JSON")
	}
}

type customMessagePackChild struct{ N, Hidden int64 }

var customMessagePackDecodes int

func (v customMessagePackChild) MarshalMsgpack() ([]byte, error) {
	return msgpack.TryEncode(nil, reflect.ValueOf([2]int64{v.N, v.Hidden}))
}
func (v *customMessagePackChild) UnmarshalMsgpack(raw []byte) error {
	customMessagePackDecodes++
	var fields [2]int64
	if err := msgpack.TryDecode(raw, reflect.ValueOf(&fields)); err != nil {
		return err
	}
	v.N, v.Hidden = fields[0], fields[1]
	return nil
}

func TestMessagePackCustomCodecFallback(t *testing.T) {
	type root struct {
		Child  *customMessagePackChild `msgpack:"c"`
		Hidden countedString           `msgpack:"h"`
	}
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "root")
	c := kvo.NewEntityType(s, "custom")
	kvo.NewProp(s, 1, "child", c, nil)
	kvo.NewProp(s, 2, "n", kvo.TInt64, nil)
	cm := kvo.NewModel(s, c, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(2)) })
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	fields := kvo.FieldMap{reflect.TypeFor[root](): {1: "Child"}, reflect.TypeFor[customMessagePackChild](): {2: "N"}}
	p := kvo.Project(reflect.TypeFor[root](), m, fields)
	src := root{&customMessagePackChild{4, 51}, "untouched"}
	raw, err := msgpack.TryEncode(nil, reflect.ValueOf(&src))
	if err != nil {
		t.Fatal(err)
	}
	customMessagePackDecodes, hiddenDecodes = 0, 0
	r := p.MessagePack(raw)
	if customMessagePackDecodes != 0 || hiddenDecodes != 0 {
		t.Fatal("eager root decode")
	}
	if r.Child(row2.Step{Key: 1}).GetValue(2).AsInt() != 4 || customMessagePackDecodes != 1 || hiddenDecodes != 0 {
		t.Fatal("local child fallback")
	}
	ed := r.Edit()
	if err := ed.TryFastSetPathValue([]row2.Step{{Key: 1}, {Key: 2}}, values.Int(9)); err != nil {
		t.Fatal(err)
	}
	if hiddenDecodes != 0 {
		t.Fatal("edit decoded siblings")
	}
	var got root
	if err := msgpack.TryDecode(ed.MessagePack(), reflect.ValueOf(&got)); err != nil {
		t.Fatal(err)
	}
	if got.Child.N != 9 || got.Child.Hidden != 51 || got.Hidden != src.Hidden {
		t.Fatal(got)
	}
	cp := kvo.Project(reflect.TypeFor[customMessagePackChild](), cm, fields)
	if cp.DirectMessagePack() {
		t.Fatal("custom root must retain decoder")
	}
	childBytes, _ := src.Child.MarshalMsgpack()
	if cp.MessagePack(childBytes).GetValue(2).AsInt() != 4 {
		t.Fatal("custom root fallback")
	}
	nilChild := p.MessagePack([]byte{0x81, 0xa1, 'c', 0xc0}).Child(row2.Step{Key: 1})
	if !nilChild.Fastest().Missing() {
		t.Fatal("missing custom child")
	}
}

func TestFastKeepsFast2(t *testing.T) {
	_, m := statrow.Model()
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), m)
	src := statrow.Sample(13)
	raw := binding.Encode(nil, reflect.ValueOf(src).Elem(), row2.Fast2)
	for _, r := range []kvo.Record{kvo.Encoded(row2.Trusted(raw)), binding.Encoded(row2.Trusted(raw))} {
		hot := r.Fast()
		snapshot := hot.Snapshot().Raw()
		if &snapshot[0] != &raw[0] {
			t.Fatal("Fast decoded Fast2")
		}
		if testing.AllocsPerRun(10, func() { _ = r.Fast() }) != 0 {
			t.Fatal("Fast allocated for Fast2")
		}
	}
	compact := binding.Encode(nil, reflect.ValueOf(src).Elem(), row2.Compact2)
	mp, err := msgpack.TryEncode(nil, reflect.ValueOf(src))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []kvo.Record{binding.Encoded(row2.Trusted(compact)), binding.MessagePack(mp)} {
		hot := r.Fast()
		got := hot.Native().(*statrow.State)
		got.ID = 500
		if hot.GetValue(1).AsUint() != 500 || r.GetValue(1).AsUint() != 13 {
			t.Fatal("Fast did not materialize independent native object")
		}
		if hot.Fast().Native() != got {
			t.Fatal("Fast copied decoded object")
		}
	}
	generic := kvo.Encoded(row2.Trusted(compact)).WithModel(m).Fast()
	clear(compact)
	if generic.GetValue(1).AsUint() != 13 || generic.Native() != nil {
		t.Fatal("Fast did not materialize generic memory")
	}
	native := binding.Record(reflect.ValueOf(src))
	if native.Fast().Native() != src {
		t.Fatal("Fast copied original Go object")
	}
	if !kvo.Encoded(row2.View{}).Fast().Missing() {
		t.Fatal("Fast changed missing record")
	}
}

func TestMaterializationDoesNotInventPrimaryKey(t *testing.T) {
	_, model := statrow.Model()
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	src := statrow.Sample(13)
	raw := binding.Encode(nil, reflect.ValueOf(src).Elem(), row2.Fast2)
	r := binding.Encoded(row2.Trusted(raw))
	if r.Fast().GetValue(1).AsUint() != 13 {
		t.Fatal("stored ID lost")
	}
	hot := r.Fastest()
	if hot.Native().(*statrow.State).Key != 0 {
		t.Fatal("materialization invented an omitted database key")
	}
	if hot.Fastest().Native() != hot.Native() {
		t.Fatal("redecoded native object")
	}
}
