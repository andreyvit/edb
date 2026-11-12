package kvo

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

func TestModelGeneratedNativeFields(t *testing.T) {
	type Counts struct {
		Count int64 `kvo:"count" msgpack:"c"`
	}
	type Row struct {
		Name   string `kvo:"name" msgpack:"n"`
		Counts `kvo:",inline" msgpack:",inline"`
	}
	newModel := func() *Model {
		s := NewSchema()
		name := NewStringProp[string](s, 1, "name", TMachineString)
		count := NewSignedProp[int64](s, 2, "count", TInt64)
		return NewModel(s, NewEntityType(s, "row", reflect.TypeFor[Row]()), func(b *ModelBuilder) { b.Prop(name); b.Prop(count) })
	}
	var reads, writes int
	fields := &nativebind.GeneratedFields{Type: reflect.TypeFor[Counts](), New: func() unsafe.Pointer { return unsafe.Pointer(new(Counts)) }, Fields: []nativebind.GeneratedField{
		{Name: "Count", Get: func(p unsafe.Pointer) values.Value { reads++; return values.Int((*Counts)(p).Count) }, Set: func(p unsafe.Pointer, v values.Value) { writes++; (*Counts)(p).Count = v.AsInt() }},
	}}
	for _, full := range []bool{false, true} {
		m := newModel()
		m.RegisterNativeFields(fields)
		row := &Row{Name: "Ada", Counts: Counts{Count: 12}}
		var r Record
		if full {
			r = Bind(reflect.TypeFor[Row](), m).Record(reflect.ValueOf(row))
		} else {
			r = Project(reflect.TypeFor[Row](), m, nil).Record(reflect.ValueOf(row))
		}
		before := reads
		if r.GetValue(2).AsInt() != 12 || reads != before+1 || r.GetValue(1).AsString() != "Ada" {
			t.Fatal("generated inline read or reflected fallback")
		}
		before = writes
		r.Edit().SetValue(2, values.Int(19))
		if row.Count != 19 || writes != before+1 {
			t.Fatal("generated write did not mutate original embedding")
		}
		assertPanics := func(f func()) {
			t.Helper()
			defer func() {
				if recover() == nil {
					t.Fatal("expected registration panic")
				}
			}()
			f()
		}
		assertPanics(func() { m.RegisterNativeFields(fields) })
	}
	m := newModel()
	stale := *fields
	stale.Fields = nil
	mustPanic := func(f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatal("expected registration panic")
			}
		}()
		f()
	}
	mustPanic(func() { m.RegisterNativeFields(&stale) })
	m.RegisterNativeFields(fields)
	mustPanic(func() { m.RegisterNativeFields(fields) })
	mustPanic(func() { newModel().RegisterNativeFields(nil) })
}
