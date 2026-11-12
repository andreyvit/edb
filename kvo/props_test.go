package kvo_test

import (
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestTypedDeclarationsAndModelBuilder(t *testing.T) {
	type ID uint64
	s := kvo.NewSchema()
	id := kvo.NewUnsignedProp[ID](s, 1, "customer:id", kvo.NewScalarSubtype[ID]("id", kvo.TUint64))
	name := kvo.NewStringProp[string](s, 2, "customer:name", kvo.TMachineString)
	legacy := kvo.NewProp(s, 3, "customer:legacy", kvo.TUint64, nil)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "customer"), func(b *kvo.ModelBuilder) {
		b.Prop(id, kvo.PrimaryKey)
		b.Prop(name, kvo.PropName("label"))
		b.Prop(s.MustPropByCode(legacy))
	})
	if m.PropByName("label").Code() != name.Code() || m.PropByName("id").Code() != id.Code() {
		t.Fatal("typed model property registration lost names or codes")
	}
	if reflect.TypeOf(id).PkgPath() != "github.com/andreyvit/edb/kvo" || unsafe.Sizeof(id) != 8 {
		t.Fatal("property declaration has the wrong package or size")
	}
	for _, invalid := range []any{-1, "name", nil} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("accepted invalid property %v", invalid)
				}
			}()
			kvo.CodeOf(invalid)
		}()
	}
}

func TestLegacyPropertyStorageContracts(t *testing.T) {
	s := kvo.NewSchema()
	at := kvo.NewLegacyTimeProp(s, 1, "at")
	counts := kvo.NewMapCodec(kvo.MapOf(kvo.TUint64, kvo.TUint64), values.UnsignedCodec[uint64](), values.UnsignedCodec[uint64]()).Prop(s, 2, "counts", kvo.PropFlag(0))
	m := kvo.NewModel(s, kvo.NewEntityType(s, "legacy"), func(b *kvo.ModelBuilder) { b.Prop(at); b.Prop(counts) })
	if m.PropFlags(counts.Code()) != 0 {
		t.Fatal("changed legacy dictionary policy")
	}
	codec := kvo.LegacyTimeCodec()
	for _, want := range []time.Time{{}, time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)} {
		v := codec.Encode(want)
		if v.AsUint() != kvo.TimeToUint64(want) || !codec.Decode(v).Equal(want) {
			t.Fatal("changed legacy time words")
		}
		for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
			var w row2.Writer
			w.Begin(0, "", row2.Object, layout)
			w.End()
			e := kvo.Encoded(row2.Open(w.BytesWritten())).WithModel(m).Edit()
			e.Set(at, want)
			r := e.Record()
			if !r.Get(at).Equal(want) || r.GetValue(at.Code()).AsUint() != kvo.TimeToUint64(want) {
				t.Fatal("typed access changed legacy time storage")
			}
		}
	}
	if !kvo.SameType(kvo.MapOf(kvo.TUint64, kvo.ListOf(kvo.TTime)), kvo.MapOf(kvo.TUint64, kvo.ListOf(kvo.TTime))) || kvo.SameType(kvo.TInt64, kvo.TUint64) {
		t.Fatal("incorrect logical type identity")
	}
}
