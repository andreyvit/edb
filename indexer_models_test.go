package edb

import (
	"reflect"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

var (
	indexModels      = kvo.NewSchema()
	indexName        = kvo.NewStringProp[string](indexModels, 1, "Name", kvo.TMachineString)
	indexEmail       = kvo.NewStringProp[string](indexModels, 2, "Email", kvo.TMachineString)
	indexGroup       = kvo.NewStringProp[string](indexModels, 3, "Group", kvo.TMachineString)
	indexGroupNumber = kvo.NewUnsignedProp[uint64](indexModels, 4, "GroupNumber", kvo.TUint64)
	indexSort        = kvo.NewSignedProp[int](indexModels, 5, "Sort", kvo.TInt)
	indexCode        = kvo.NewStringProp[string](indexModels, 6, "Code", kvo.TMachineString)
	indexEntries     = kvo.NewListProp(indexModels, 7, "Entries", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
)

// Test projections describe only indexed fields; ordinary row payloads stay unchanged.
func testIndexAccess[Row any](props ...kvo.PropertyCode) *kvo.Model {
	schema := kvo.NewSchema()
	typ := reflect.TypeFor[Row]()
	fields := map[uint64]string{}
	for _, p := range props {
		prop := indexModels.MustPropByCode(p.Code())
		kvo.NewProp(schema, p.Code(), prop.Name(), prop.AnyType(), nil)
		name := prop.Name()
		if p.Code() == indexGroupNumber.Code() {
			name = "Group"
		}
		fields[p.Code()] = name
	}
	model := kvo.NewModel(schema, kvo.NewEntityType(schema, typ.Name()), func(b *kvo.ModelBuilder) {
		for _, p := range props {
			b.Prop(p)
		}
	})
	model.RegisterNativeType(typ, fields)
	return model
}
