package kvolab

import (
	"fmt"
	"testing"
	"unsafe"
)

// A generated wrapper knows its schema. It needs no runtime descriptor pointer.
type Widget struct{ Data View }

func (w Widget) Count() (uint64, bool) { v, ok := w.Data.Get(1); return v.U, ok }
func (w Widget) Properties() View      { v, _ := w.Data.Get(2); return v.Object() }

type Property struct {
	Name string
	Code uint64
	Kind Kind
}
type Model struct {
	Name       string
	Properties []Property
}

func (m *Model) Bind(name string) (Property, bool) {
	for _, p := range m.Properties {
		if p.Name == name {
			return p, true
		}
	}
	return Property{}, false
}

type DynamicObject struct {
	Data  View
	Model *Model
}

func (o DynamicObject) Read(p Property) (Cell, bool) { return o.Data.Get(p.Code) }
func ExampleWidget() {
	data := EncodeMixed(Obj(Field{1, U(12)}, Field{2, Map(Entry{"region", S("west")})}), Indexed, Compact)
	w := Widget{data}
	n, _ := w.Count()
	v, _ := w.Properties().GetString("region")
	fmt.Println(n, string(v.Bytes()))
	var scratch [8]Edit
	e := Editor{Base: data, Edits: scratch[:0]}
	e.Set(1, Cell{Kind: Uint, U: 13})
	updated := Widget{e.AppendCompact(nil)}
	n, _ = updated.Count()
	fmt.Println(n)
	// Output:
	// 12 west
	// 13
}
func TestTypedAndDynamic(t *testing.T) {
	model := &Model{Name: "widget", Properties: []Property{{"count", 1, Uint}, {"properties", 2, Dict}}}
	p, ok := model.Bind("count")
	if !ok {
		t.Fatal("bind")
	}
	raw := Encode(Obj(Field{1, U(0)}), Compact)
	typed := Widget{raw}
	dynamic := DynamicObject{raw, model}
	a, yes := typed.Count()
	c, ok := dynamic.Read(p)
	if !yes || !ok || a != c.U {
		t.Fatal("presence")
	}
	t.Logf("typed-wrapper=%d dynamic-wrapper=%d", unsafe.Sizeof(typed), unsafe.Sizeof(dynamic))
}
