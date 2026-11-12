// Package valuebench supplies the same fixture before and after value unification.
package valuebench

import (
	"github.com/andreyvit/edb/kvo"
	"reflect"
	"strings"
)

//go:generate go run ./gen
type Row struct {
	Count int64    `edb:"count"`
	Name  string   `edb:"name"`
	Data  []byte   `edb:"data"`
	Tags  []uint64 `edb:"tags"`
}

func Model() (*kvo.Schema, *kvo.Model) {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "Row", reflect.TypeFor[Row]())
	kvo.NewProp(s, 1, "count", kvo.TInt64, nil)
	kvo.NewProp(s, 2, "name", kvo.TMachineString, nil)
	kvo.NewProp(s, 3, "data", kvo.TBytes, nil)
	kvo.NewProp(s, 4, "tags", kvo.ListOf(kvo.TUint64), nil)
	return s, kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(1))
		b.Prop(s.MustPropByCode(2))
		b.Prop(s.MustPropByCode(3))
		b.Prop(s.MustPropByCode(4))
	})
}
func Sample() *Row {
	return &Row{-129, strings.Repeat("string payload! ", 4), []byte(strings.Repeat("binary payload! ", 16)), []uint64{1, 128, 65536, 1 << 40}}
}
