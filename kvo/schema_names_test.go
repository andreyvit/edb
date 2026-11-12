package kvo

import (
	"reflect"
	"slices"
	"testing"
)

func TestModelSelectors(t *testing.T) {
	for _, tt := range []struct {
		full, local string
		want        []string
	}{
		{"points", "points", []string{"points"}},
		{"points", "balance", []string{"balance", "points"}},
		{"customer:points", "balance", []string{"balance", "customer_points", "points"}},
		{"org:customer:points", "balance", []string{"balance", "org_customer_points", "points"}},
		{"customer:line-item", "item", []string{"item"}},
		{"customer:_", "item", []string{"customer__", "item"}},
	} {
		t.Run(tt.full+"/"+tt.local, func(t *testing.T) {
			s := NewSchema()
			code := NewProp(s, 1, tt.full, TInt64, nil)
			m := NewModel(s, NewEntityType(s, "customer"), func(b *ModelBuilder) { b.Prop(s.MustPropByCode(code), PropName(tt.local)) })
			var names []string
			for name, prop := range m.Selectors() {
				names = append(names, name)
				if prop.Code() != code || prop.Name() != tt.local || m.PropBySelector(name) != prop || m.PropByName(name) != prop {
					t.Fatalf("selector %q does not resolve to the model's property", name)
				}
			}
			if !slices.Equal(names, tt.want) {
				t.Fatalf("selectors = %v, want %v", names, tt.want)
			}
			if m.PropByName(tt.full) == nil {
				t.Fatal("full property name was lost")
			}
			for _, name := range []string{"missing", "_", "for", "customer:points", "line-item"} {
				if m.PropBySelector(name) != nil {
					t.Fatalf("unexpected selector %q", name)
				}
			}
			visited := 0
			for range m.Selectors() {
				visited++
				break
			}
			if visited != 1 {
				t.Fatal("selector iteration did not stop")
			}
		})
	}
}

func TestInlineModelProperties(t *testing.T) {
	s := NewSchema()
	code := NewProp(s, 1, "stats:values", MapOf(TMachineString, TInt64), nil)
	flags := Compact | KeepDictionaryZeroValues | PropDense
	child := NewModel(s, NewEntityType(s, "child"), func(b *ModelBuilder) {
		b.Prop(s.MustPropByCode(code), flags, PropName("totals"), LocalID(7))
	})
	parent := NewModel(s, NewEntityType(s, "parent"), func(b *ModelBuilder) { b.Inline(child) })
	if parent.PropFlags(code) != flags || parent.propsByCode[code].LocalID != 7 {
		t.Fatal("inline lost property policy")
	}
	for _, name := range []string{"totals", "values", "stats:values", "stats_values"} {
		if p := parent.PropByName(name); p == nil || p.Code() != code {
			t.Fatal("inline lost alias", name)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("inline accepted a duplicate property")
		}
	}()
	NewModel(s, NewEntityType(s, "collision"), func(b *ModelBuilder) { b.Prop(s.MustPropByCode(code)); b.Inline(child) })
}

func TestNamespacedProperties(t *testing.T) {
	type customer struct {
		Name    string `kvo:"full_name"`
		Tenant  uint64 `kvo:"tenant:id"`
		Ignored bool   `kvo:"-"`
	}
	s := NewSchema()
	name := NewProp(s, 1, "customer:full_name", TMachineString, nil)
	tenant := NewProp(s, 2, "tenant:id", TUint64, nil)
	productName := NewProp(s, 3, "product:full_name", TMachineString, nil)
	m := NewModel(s, NewEntityType(s, "customer", reflect.TypeFor[customer]()), func(b *ModelBuilder) { b.Prop(s.MustPropByCode(name)); b.Prop(s.MustPropByCode(tenant)) })
	for _, alias := range []string{"full_name", "customer:full_name", "customer_full_name"} {
		p := m.PropByName(alias)
		if p == nil || p.Code() != name || p.Name() != "full_name" || p.FullName() != "customer:full_name" {
			t.Fatalf("%s: %v", alias, p)
		}
	}
	if len(m.AllProps()) != 2 || s.PropByCode(name).Name() != "customer:full_name" {
		t.Fatal("aliases changed declarations")
	}
	fields := m.NativeStruct(reflect.TypeFor[customer]()).Fields()
	if len(fields) != 2 || fields[0].Prop.Code() != name || fields[1].Prop.Code() != tenant {
		t.Fatal(fields)
	}
	for _, build := range []func(){
		func() { NewProp(s, name, "different", TInt64, nil) },
		func() { NewProp(s, 4, "customer:full_name", TInt64, nil) },
		func() {
			NewModel(s, NewEntityType(s, "duplicate_code"), func(b *ModelBuilder) {
				b.Prop(s.MustPropByCode(name))
				b.Prop(s.MustPropByCode(name), PropName("other"))
			})
		},
		func() {
			NewModel(s, NewEntityType(s, "duplicate_local"), func(b *ModelBuilder) { b.Prop(s.MustPropByCode(name)); b.Prop(s.MustPropByCode(productName)) })
		},
		func() {
			NewModel(s, NewEntityType(s, "duplicate_local_alias"), func(b *ModelBuilder) {
				b.Prop(s.MustPropByCode(name))
				b.Prop(s.MustPropByCode(productName), PropName("other"))
			})
		},
		func() { NewProp(s, 4, "customer:", TInt64, nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("expected declaration panic")
				}
			}()
			build()
		}()
	}
}

func TestRejectedModelKeepsRegisteredSelectors(t *testing.T) {
	s := NewSchema()
	key := NewProp(s, 1, "customer:points", TInt64, nil)
	typ := NewEntityType(s, "customer")
	original := NewModel(s, typ, func(b *ModelBuilder) { b.Prop(s.MustPropByCode(key), PropName("balance")) })
	for _, candidate := range []*EntityType{typ, NewEntityType(s, "customer")} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("duplicate model accepted")
				}
			}()
			NewModel(s, candidate, func(b *ModelBuilder) { b.Prop(s.MustPropByCode(key), PropName("rejected")) })
		}()
		if typ.Model() != original || len(s.Models()) != 1 || s.Models()[0] != original {
			t.Fatal("rejected model changed the published model identity")
		}
		for _, name := range []string{"balance", "points", "customer_points"} {
			if prop := typ.Model().PropBySelector(name); prop == nil || prop.Code() != key {
				t.Fatal("rejected model changed selector", name)
			}
		}
		if candidate != typ && candidate.model != nil {
			t.Fatal("rejected entity type acquired a model")
		}
	}
}
