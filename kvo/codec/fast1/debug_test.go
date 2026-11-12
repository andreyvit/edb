package fast1

import (
	"github.com/andreyvit/edb/kvo"
	"testing"
)

var DumpSchema = kvo.NewSchema()
var TDoodad = kvo.NewEntityType(DumpSchema, "doodad")
var TGizmo = kvo.NewEntityType(DumpSchema, "gizmo")
var KFoo = kvo.NewProp(DumpSchema, 100, "foo", kvo.TUint64Hex, nil)
var KBar = kvo.NewProp(DumpSchema, 101, "bar", kvo.MapOf(kvo.TUint64, kvo.TUint64), nil)
var KQux = kvo.NewProp(DumpSchema, 102, "qux", TGizmo, nil)
var KWaldo = kvo.NewProp(DumpSchema, 108, "waldo", kvo.MapOf(kvo.TUint64, TGizmo), nil)

var KWibble = kvo.NewProp(DumpSchema, 113, "wibble", kvo.TUint64, nil)
var KWobble = kvo.NewProp(DumpSchema, 114, "wobble", TGizmo, nil)

var MDoodad = kvo.NewModel(DumpSchema, TDoodad, func(b *kvo.ModelBuilder) {
	b.Prop(DumpSchema.MustPropByCode(KFoo))
	b.Prop(DumpSchema.MustPropByCode(KBar))
	b.Prop(DumpSchema.MustPropByCode(KQux))
	b.Prop(DumpSchema.MustPropByCode(KWaldo))
})

var MGizmo = kvo.NewModel(DumpSchema, TGizmo, func(b *kvo.ModelBuilder) {
	b.Prop(DumpSchema.MustPropByCode(KWibble))
	b.Prop(DumpSchema.MustPropByCode(KWobble))
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

func TestInvalidKey(t *testing.T) {
	assertPanics(t, "reportCannotAccessKey", func() { reportCannotAccessKey(kvo.TMachineString, 1) })
}
