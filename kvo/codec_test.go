package kvo_test

import (
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"reflect"
	"testing"
	"time"
)

type detail struct {
	N int64 `edb:"n"`
}
type sample struct {
	Label   kvo.Optional[string] `edb:"label"`
	Details map[string]detail    `edb:"details"`
	T       time.Time            `edb:"t"`
}

func TestExplicitEmptyAndTypedPayloads(t *testing.T) {
	schema := kvo.NewSchema()
	obj := kvo.NewEntityType(schema, "sample", reflect.TypeFor[sample]())
	item := kvo.NewEntityType(schema, "detail", reflect.TypeFor[detail]())
	kvo.NewProp(schema, 1, "n", kvo.TInt64, nil)
	kvo.NewModel(schema, item, func(b *kvo.ModelBuilder) { b.Prop(schema.MustPropByCode(1)) })
	kvo.NewProp(schema, 2, "details", kvo.MapOf(kvo.TMachineString, item), nil)
	kvo.NewProp(schema, 3, "label", kvo.TMachineString, nil)
	kvo.NewProp(schema, 4, "t", kvo.TTime, nil)
	m := kvo.NewModel(schema, obj, func(b *kvo.ModelBuilder) {
		b.Prop(schema.MustPropByCode(2), kvo.KeepDictionaryZeroValues)
		b.Prop(schema.MustPropByCode(3), kvo.PresenceIsSignificant)
		b.Prop(schema.MustPropByCode(4))
	})
	b := kvo.Bind(reflect.TypeFor[sample](), m)
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		for _, present := range []bool{true, false} {
			want := sample{Label: kvo.Optional[string]{Present: present}, Details: map[string]detail{"": {}, "x": {-129}}, T: time.UnixMicro(1234567).UTC()}
			raw := b.Encode(nil, reflect.ValueOf(want), layout)
			var got sample
			if err := b.TryDecode(raw, reflect.ValueOf(&got).Elem()); err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal(got, want, err)
			}
		}
	}
	bad := sample{T: time.Unix(1, 1)}
	if _, err := b.TryEncode(nil, reflect.ValueOf(bad), row2.Fast2); err == nil {
		t.Fatal("accepted loss of timestamp precision")
	}
}

type jsonRow struct {
	Extras map[string]any  `edb:"extras"`
	Bins   map[int32]int64 `edb:"bins"`
	At     time.Time       `edb:"at"`
}

func TestJSONDictionaryAndSignedKeys(t *testing.T) {
	s := kvo.NewSchema()
	entity := kvo.NewEntityType(s, "sample", reflect.TypeFor[jsonRow]())
	extras := kvo.NewProp(s, 1<<62|44, "extras", kvo.MapOf(kvo.TMachineString, kvo.TJSON), nil)
	key := kvo.NewIntType[int32]("int32", nil)
	bins := kvo.NewProp(s, 2, "bins", kvo.MapOf(key, kvo.TInt64), nil)
	at := kvo.NewProp(s, 3, "at", kvo.TTimestamp, nil)
	model := kvo.NewModel(s, entity, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(extras), kvo.KeepDictionaryZeroValues)
		b.Prop(s.MustPropByCode(bins), kvo.KeepDictionaryZeroValues)
		b.Prop(s.MustPropByCode(at))
	})
	binding := kvo.Bind(reflect.TypeFor[jsonRow](), model)
	row := jsonRow{map[string]any{"null": nil, "nested": []any{int64(-4), uint64(1<<63) + 1, "x", true, map[string]any{"z": false}}}, map[int32]int64{-7: 0, 9: 12}, time.Unix(1, 912345678).UTC()}
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		raw, err := binding.TryEncode(nil, reflect.ValueOf(row), layout)
		if err != nil {
			t.Fatal(err)
		}
		if err = binding.TryValidate(raw); err != nil {
			t.Fatal(err)
		}
		var got jsonRow
		if err = binding.TryDecode(raw, reflect.ValueOf(&got).Elem()); err != nil || !reflect.DeepEqual(row, got) {
			t.Fatalf("got %#v, %v", got, err)
		}
	}
}
