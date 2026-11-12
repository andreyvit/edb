package kvo_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestDecodedMemory(t *testing.T) {
	_, model := statrow.Model()
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		source := statrow.Sample(13)
		source.Spent = -257
		raw := binding.Encode(nil, reflect.ValueOf(source).Elem(), layout)
		encoded := kvo.Encoded(row2.Trusted(raw))
		r := encoded.WithModel(model).Fastest()
		a, err := encoded.TryToJSON(model, kvo.JSONOptions{})
		if err != nil {
			t.Fatal(err)
		}
		z, err := r.TryToJSON(model, kvo.JSONOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, z) {
			t.Fatalf("decoded semantics: %#v / %#v", a, z)
		}
		if r.Native() != nil || r.NativeType() != nil {
			t.Fatal("invented Go binding")
		}
		if testing.AllocsPerRun(10, func() { _ = r.Fastest() }) != 0 {
			t.Fatal("redecoded owned object")
		}
		cloned := r.Snapshot().Raw()
		if !bytes.Equal(raw, cloned) {
			t.Fatal("snapshot changed wire values")
		}
		clear(raw)
		if r.GetValue(20).AsInt() != -257 || r.Child(row2.Step{Key: 9}).GetValue(5).AsUint() != source.Profile.Balance {
			t.Fatal("borrowed source")
		}
		editor := r.Edit()
		if err := editor.TryFastSetPathValue([]row2.Step{{Key: 9}, {Key: 5}}, values.Uint(900)); err != nil {
			t.Fatal(err)
		}
		if r.Child(row2.Step{Key: 9}).GetValue(5).AsUint() != 900 {
			t.Fatal("decoded edit not live")
		}
		if row2.Trusted(cloned).Child(row2.Step{Key: 9}).Get(5, row2.Word).AsUint() != source.Profile.Balance {
			t.Fatal("snapshot changed")
		}
		var decoded statrow.State
		if err := binding.TryDecode(editor.Snapshot().Raw(), reflect.ValueOf(&decoded).Elem()); err != nil {
			t.Fatal(err)
		}
		if decoded.Profile.Balance != 900 || decoded.Spent != -257 {
			t.Fatal("bad edited values")
		}
	}
}

func TestDecodedMemoryUnknownAndPresence(t *testing.T) {
	s := kvo.NewSchema()
	root := kvo.NewEntityType(s, "root")
	child := kvo.NewEntityType(s, "child")
	kvo.NewProp(s, 1, "child", child, nil)
	kvo.NewProp(s, 2, "number", kvo.TInt64, nil)
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(2), kvo.PresenceIsSignificant) })
	model := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1), kvo.PresenceIsSignificant) })
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		var w row2.Writer
		w.Begin(0, "", row2.Object, layout)
		w.PutPresent(1, "", values.Uint(0), row2.Word)
		w.Put(999999999999, "", values.Bytes([]byte("opaque")), row2.Data)
		w.End()
		r := kvo.Encoded(row2.Trusted(w.BytesWritten())).WithModel(model).Fastest()
		if !r.Has(1) || r.Child(row2.Step{Key: 1, Options: row2.Present}).Missing() {
			t.Fatal("lost explicit empty object")
		}
		if string(r.GetAs(999999999999, row2.Data).AsBytes()) != "opaque" {
			t.Fatal("lost unknown field")
		}
		e := r.Edit()
		if err := e.TryFastSetPathValue([]row2.Step{{Key: 1, Options: row2.Present}, {Key: 2, Options: row2.Present}}, values.Int(0)); err != nil {
			t.Fatal(err)
		}
		if !r.Child(row2.Step{Key: 1, Options: row2.Present}).Has(2) {
			t.Fatal("lost present zero")
		}
		if err := e.TryFastSetPathValue([]row2.Step{{Key: 1, Options: row2.Present}, {Key: 2}}, values.Int(0)); err != nil {
			t.Fatal(err)
		}
		if !r.Has(1) || r.Child(row2.Step{Key: 1, Options: row2.Present}).Has(2) {
			t.Fatal("wrong deletion")
		}
		if err := e.TryFastSetPathValue([]row2.Step{{Key: 1}}, values.Uint(0)); err != nil {
			t.Fatal(err)
		}
		if r.Has(1) {
			t.Fatal("not deleted")
		}
		if string(e.Snapshot().Get(999999999999, row2.Data).AsBytes()) != "opaque" {
			t.Fatal("unknown field edited")
		}
	}
}

func TestDecodedMemoryWithoutModel(t *testing.T) {
	var w row2.Writer
	w.Begin(0, "", row2.Object, row2.Compact2)
	w.Begin(8, "", row2.Object, row2.Compact2)
	w.Put(3, "", values.Int(-900), row2.Signed)
	w.End()
	w.End()
	r := kvo.Encoded(row2.Trusted(w.BytesWritten())).Fastest()
	if r.Child(row2.Step{Key: 8}).GetAs(3, row2.Signed).AsInt() != -900 {
		t.Fatal("schema-free child")
	}
	if err := r.Edit().TrySetAs([]row2.Step{{Key: 8}, {Key: 3}}, values.Int(900), row2.Signed); err != nil {
		t.Fatal(err)
	}
	if r.Child(row2.Step{Key: 8}).GetAs(3, row2.Signed).AsInt() != 900 {
		t.Fatal("schema-free edit")
	}
	if !kvo.Encoded(row2.View{}).Fastest().Missing() {
		t.Fatal("missing became present")
	}
}
