package kvo

import "testing"

func TestSchemaAndModelLookups(t *testing.T) {
	prop := DumpSchema.PropByCode(KFoo)
	if prop == nil || prop.Name() != "foo" {
		t.Fatalf("PropByCode(KFoo) = %v, wanted foo", prop)
	}
	_ = prop.ValueKind()
	_ = prop.TypeModel()
	eq(t, DumpSchema.MustPropByCode(KFoo).Name(), "foo")

	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	_ = DumpSchema.MustPropByCode(999999)
}

func TestModelProps(t *testing.T) {
	_ = MDoodad.Name()
	_ = MDoodad.Type()

	if MDoodad.PropByName("foo") == nil {
		t.Fatalf("MDoodad.PropByName(foo) = nil")
	}
	if MDoodad.PropByCode(KFoo) == nil {
		t.Fatalf("MDoodad.PropByCode(KFoo) = nil")
	}
	if MDoodad.MustPropByCode(KFoo).Name() != "foo" {
		t.Fatalf("MDoodad.MustPropByCode(KFoo) returned unexpected prop")
	}
	if len(MDoodad.AllProps()) == 0 {
		t.Fatalf("MDoodad.AllProps() is empty")
	}

	pi := (&PropInstance{}).PropInstance()
	if pi == nil {
		t.Fatalf("PropInstance.PropInstance returned nil")
	}

	var opt PropOption = PropDense
	opt.markerIsPropOption()
}
