package kvo

var DumpSchema = NewSchema()
var TDoodad = NewEntityType(DumpSchema, "doodad")
var TGizmo = NewEntityType(DumpSchema, "gizmo")
var KFoo = NewProp(DumpSchema, 100, "foo", TUint64Hex, nil)
var KBar = NewProp(DumpSchema, 101, "bar", MapOf(TUint64, TUint64), nil)
var KQux = NewProp(DumpSchema, 102, "qux", TGizmo, nil)
var KWaldo = NewProp(DumpSchema, 108, "waldo", MapOf(TUint64, TGizmo), nil)

var KWibble = NewProp(DumpSchema, 113, "wibble", TUint64, nil)
var KWobble = NewProp(DumpSchema, 114, "wobble", TGizmo, nil)

var MDoodad = NewModel(DumpSchema, TDoodad, func(b *ModelBuilder) {
	b.Prop(DumpSchema.MustPropByCode(KFoo))
	b.Prop(DumpSchema.MustPropByCode(KBar))
	b.Prop(DumpSchema.MustPropByCode(KQux))
	b.Prop(DumpSchema.MustPropByCode(KWaldo))
})

var MGizmo = NewModel(DumpSchema, TGizmo, func(b *ModelBuilder) {
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
