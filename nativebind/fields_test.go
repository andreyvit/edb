package nativebind_test

import (
	"math"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/nativebind/internal/fixture"
	"github.com/andreyvit/edb/values"
)

func TestNativeFields(t *testing.T) {
	for _, generated := range []bool{false, true} {
		name := "reflected"
		var g *nativebind.GeneratedFields
		if generated {
			name = "generated"
			g = &fixture.Fields
		}
		t.Run(name, func(t *testing.T) {
			b := nativebind.BindFields(reflect.TypeFor[fixture.Row](), g, nativebind.Automatic)
			if b.Generated != generated {
				t.Fatal("wrong selection")
			}
			owner := (*fixture.Row)(b.New())
			p := unsafe.Pointer(owner)
			next := &fixture.Row{Text: "next"}
			items := []int{3, 4}
			labels := map[string]int{"x": 8}
			child := fixture.Child{Number: 19}
			for _, x := range []struct {
				name string
				v    values.Value
			}{
				{"Count", values.Int(-129)}, {"Flag", values.Bool(true)}, {"Text", values.String("native")},
				{"Fraction", values.Float32(-1.25)}, {"Ratio", values.Float(math.Inf(1))},
				{"Next", values.Value{P: unsafe.Pointer(next)}}, {"Items", values.Value{P: unsafe.Pointer(&items)}},
				{"Labels", values.Value{P: unsafe.Pointer(&labels)}}, {"Child", values.Value{P: unsafe.Pointer(&child)}},
			} {
				b.Field(x.name).Set(p, x.v)
			}
			if owner.Count != -129 || !bool(owner.Flag) || owner.Text != "native" || owner.Fraction != -1.25 || !math.IsInf(owner.Ratio, 1) || owner.Next != next || owner.Child.Number != 19 {
				t.Fatal(owner)
			}
			if b.Field("Count").Get(p).AsInt() != -129 || b.Field("Text").Get(p).AsString() != "native" || b.Field("Next").Get(p).P != unsafe.Pointer(next) {
				t.Fatal("read mismatch")
			}
			if b.Field("Child").Get(p).P != unsafe.Pointer(&owner.Child) {
				t.Fatal("nested struct detached")
			}
			(*fixture.Child)(b.Field("Child").Address(p)).Number = 23
			owner.Items[0] = 11
			owner.Labels["x"] = 12
			if owner.Child.Number != 23 || items[0] != 11 || labels["x"] != 12 {
				t.Fatal("aliases lost")
			}
			b.Field("Next").Set(p, values.Value{})
			b.Field("Labels").Set(p, values.Value{})
			b.Field("Items").Set(p, values.Value{})
			if owner.Next != nil || owner.Labels != nil || owner.Items != nil {
				t.Fatal("nil assignment")
			}
			runtime.KeepAlive(owner)
		})
	}
}
func TestNativeFieldSelection(t *testing.T) {
	typ := reflect.TypeFor[fixture.Row]()
	stale := fixture.Fields
	stale.Fields = stale.Fields[:1]
	if nativebind.BindFields(typ, &stale, nativebind.Automatic).Generated {
		t.Fatal("stale fields accepted")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("StaticOnly accepted stale fields")
		}
	}()
	nativebind.BindFields(typ, &stale, nativebind.StaticOnly)
}
func TestHiddenNativeField(t *testing.T) {
	b := nativebind.BindFields(reflect.TypeFor[fixture.Row](), nil, nativebind.Automatic)
	defer func() {
		if recover() == nil {
			t.Fatal("private field exposed")
		}
	}()
	b.Field("hidden")
}
