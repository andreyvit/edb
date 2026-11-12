package kvo_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestProjectionAliasesNativeFields(t *testing.T) {
	type state struct {
		Number *int64
		Other  chan int
	}
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "state")
	kvo.NewProp(s, 9, "n", kvo.TInt64, nil)
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(9), kvo.PresenceIsSignificant) })
	p := kvo.Project(reflect.TypeFor[state](), m, kvo.FieldMap{reflect.TypeFor[state](): {9: "Number"}})
	var obj state
	r := p.Record(reflect.ValueOf(&obj))
	if r.Has(9) {
		t.Fatal("nil field present")
	}
	ed := r.Edit()
	if err := ed.TrySetValue(9, values.Int(0)); err != nil {
		t.Fatal(err)
	}
	if obj.Number == nil || !r.Has(9) {
		t.Fatal("explicit zero missing")
	}
	*obj.Number = -129
	if n := r.GetValue(9).AsInt(); n != -129 {
		t.Fatal(n)
	}
	if err := ed.TryFastSetPathValue([]row2.Step{{Key: 9}}, values.Value{}); err != nil || obj.Number != nil {
		t.Fatal(obj, err)
	}
}
