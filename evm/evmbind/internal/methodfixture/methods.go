package methodfixture

import (
	"reflect"
	"strconv"

	"github.com/andreyvit/edb/evm/evmbind"
	"github.com/andreyvit/edb/kvo"
)

//go:generate go run ./gen

type Row struct {
	Name     string `msgpack:"n,omitempty" kvo:"name"`
	Earned   int64  `msgpack:"e" kvo:"earned"`
	Redeemed int64  `msgpack:"r" kvo:"redeemed"`
	Expired  int64  `msgpack:"x" kvo:"expired"`
}

var Schema = kvo.NewSchema()
var Name = kvo.NewStringProp[string](Schema, 4, "name", kvo.TMachineString)
var Earned = kvo.NewSignedProp[int64](Schema, 1, "earned", kvo.TInt64)
var Redeemed = kvo.NewSignedProp[int64](Schema, 2, "redeemed", kvo.TInt64)
var Expired = kvo.NewSignedProp[int64](Schema, 3, "expired", kvo.TInt64)
var Model = kvo.NewModel(Schema, kvo.NewEntityType(Schema, "Customer", reflect.TypeFor[Row]()), func(b *kvo.ModelBuilder) {
	b.Prop(Earned)
	b.Prop(Redeemed)
	b.Prop(Expired)
	b.Prop(Name)
})
var OtherModel = kvo.NewModel(Schema, kvo.NewEntityType(Schema, "Other"), func(b *kvo.ModelBuilder) {
	b.Prop(Earned)
	b.Prop(Redeemed)
	b.Prop(Expired)
	b.Prop(Name)
})

func Customer_Name(r kvo.Record) string   { return r.Get(Name) }
func Customer_Balance(r kvo.Record) int64 { return r.Get(Earned) - r.Get(Redeemed) - r.Get(Expired) }

type Delta int64

func Customer_Add(r kvo.Record, n Delta) int64 { return Customer_Balance(r) + int64(n) }
func Customer_WithFlag(r kvo.Record, flag kvo.PropFlag) int64 {
	return Customer_Balance(r) + int64(flag)
}
func Customer_Label(r kvo.Record, prefix string) string {
	return prefix + strconv.FormatInt(Customer_Balance(r), 10)
}
func Customer_Summary(r kvo.Record, n int8, f float32) (int64, float32) {
	return Customer_Balance(r) + int64(n), f * 2
}
func Customer_Check(r kvo.Record, enabled bool) (bool, uint16) {
	return enabled && Customer_Balance(r) > 0, 65535
}
func Customer_Observe(r kvo.Record) {
	if Customer_Balance(r) != 73 {
		panic("unexpected balance")
	}
}
func Customer_Fail(r kvo.Record) { panic("helper panic") }

func Methods() []evmbind.Method {
	return []evmbind.Method{
		evmbind.RecordMethod(Model, "Name", Customer_Name, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "Balance", Customer_Balance, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "Add", Customer_Add, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "WithFlag", Customer_WithFlag, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "Label", Customer_Label, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "Summary", Customer_Summary, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "Check", Customer_Check, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "Observe", Customer_Observe, evmbind.BorrowOnly),
		evmbind.RecordMethod(Model, "Fail", Customer_Fail, evmbind.BorrowOnly),
	}
}
