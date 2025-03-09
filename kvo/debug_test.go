package kvo

import (
	"testing"
)

var DumpSchema = NewSchema()
var TDoodad = NewEntityType(DumpSchema, "doodad")
var TGizmo = NewEntityType(DumpSchema, "gizmo")
var KFoo = NewProp(DumpSchema, 100, "foo", TUint64Hex, nil)
var KBar = NewProp(DumpSchema, 101, "bar", Map(TUint64, TUint64), nil)
var KQux = NewProp(DumpSchema, 102, "qux", TGizmo, nil)
var KWaldo = NewProp(DumpSchema, 108, "waldo", Map(TUint64, TGizmo), nil)

// var KXyzzy = NewProp(DumpSchema, 111, "xyzzy", Uint64, nil)
var KWibble = NewProp(DumpSchema, 113, "wibble", TUint64, nil)
var KWobble = NewProp(DumpSchema, 114, "wobble", TGizmo, nil)

// var KWubble = NewProp(DumpSchema, 115, "wubble", Uint64, nil)

var MDoodad = NewModel(DumpSchema, TDoodad, func(b *ModelBuilder) {
	b.Prop(KFoo)
	b.Prop(KBar)
	b.Prop(KQux)
	b.Prop(KWaldo)
})

var MGizmo = NewModel(DumpSchema, TGizmo, func(b *ModelBuilder) {
	b.Prop(KWibble)
	b.Prop(KWobble)
	// b.Prop(KWubble)
})

// 20 metasyntactic variables
var mockNames = []string{
	"foo", "bar", "qux", "baz", "quux",
	"corge", "grault", "garply", "waldo", "fred",
	"plugh", "xyzzy", "thud", "wibble", "wobble",
	"wubble", "flob", "flobble", "flooble", "floober",
}

var fakeTypeNames = []string{
	"widget", "gadget", "doohickey", "frobernator",
	"doodad", "dinglehopper", "thingamabob", "thingamajig",
	"gizmo",
}

func TestDump(t *testing.T) {
	m := NewRecord(TDoodad)
	m.Set(KFoo, 0x42)
	fillMap(m.UpdateMap(KBar))
	fillGizmo(m.UpdateMap(KQux), 1000, 2)

	w := m.UpdateMap(KWaldo)
	fillGizmo(w.UpdateMap(222), 2000, 0)
	fillGizmo(w.UpdateMap(444), 4000, 1)

	a := m.rec.PackedRoot().Dump()
	e := "{foo: 0x42, bar: {100: 42, 200: 84}, qux: {wibble: 1000, wobble: {wibble: 1001, wobble: {wibble: 1002}}}, waldo: {222: {wibble: 2000}, 444: {wibble: 4000, wobble: {wibble: 4001}}}}"
	if a != e {
		t.Fatalf("** got:\n%v\n\nwanted:\n%v", a, e)
	}
}

func fillMap(m MutableMap) {
	m.Set(100, 42)
	m.Set(200, 84)
}

func fillGizmo(m MutableMap, n uint64, depth int) {
	m.Set(KWibble, n)
	if depth > 0 {
		fillGizmo(m.UpdateMap(KWobble), n+1, depth-1)
	}
}
