package inlinerow

import (
	"github.com/andreyvit/edb/kvo"
	"reflect"
)

//go:generate go run ./gen
type Counts struct {
	Earned int64 `kvo:"earned" msgpack:"e"`
	Spent  int64 `kvo:"spent" msgpack:"s"`
}
type Group struct {
	Padding uint64 `kvo:"padding" msgpack:"p"`
	Counts  `kvo:",inline" msgpack:",inline"`
}
type Row struct {
	Marker uint64 `kvo:"marker" msgpack:"m"`
	Group  `kvo:",inline" msgpack:",inline"`
}

var Schema = kvo.NewSchema()
var Earned = kvo.NewSignedProp[int64](Schema, 1, "counts:earned", kvo.TInt64)
var Spent = kvo.NewSignedProp[int64](Schema, 2, "counts:spent", kvo.TInt64)
var Padding = kvo.NewUnsignedProp[uint64](Schema, 3, "padding", kvo.TUint64)
var Marker = kvo.NewUnsignedProp[uint64](Schema, 4, "marker", kvo.TUint64)
var CountsModel = kvo.NewModel(Schema, kvo.NewEntityType(Schema, "counts", reflect.TypeFor[Counts]()), func(b *kvo.ModelBuilder) {
	b.Prop(Earned)
	b.Prop(Spent)
})
var GroupModel = kvo.NewModel(Schema, kvo.NewEntityType(Schema, "group", reflect.TypeFor[Group]()), func(b *kvo.ModelBuilder) {
	b.Prop(Padding)
	b.Inline(CountsModel)
})
var Model = kvo.NewModel(Schema, kvo.NewEntityType(Schema, "row", reflect.TypeFor[Row]()), func(b *kvo.ModelBuilder) {
	b.Prop(Marker)
	b.Inline(GroupModel)
})
