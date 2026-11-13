package main

import (
	"fmt"
	"reflect"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

type Customer struct {
	ID             uint64   `msgpack:"-" kvo:"-"`
	FirstName      string   `msgpack:"first_name" kvo:"first_name"`
	Email          string   `msgpack:"email" kvo:"email"`
	Tags           []string `msgpack:"tags" kvo:"tags"`
	PointsEarned   int64    `msgpack:"points_earned" kvo:"points_earned"`
	PointsRedeemed int64    `msgpack:"points_redeemed" kvo:"points_redeemed"`
}

var (
	Models       = kvo.NewSchema()
	CustomerType = kvo.NewEntityType(Models, "customer", reflect.TypeFor[Customer]())

	Customer_FirstName = kvo.NewStringProp[string](Models, 1, "customer:first_name", kvo.TMachineString)
	Customer_Email     = kvo.NewStringProp[string](Models, 2, "customer:email", kvo.TMachineString)
	Customer_Tags      = kvo.NewListProp(Models, 3, "customer:tags",
		kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	Customer_PointsEarned   = kvo.NewSignedProp[int64](Models, 4, "customer:points_earned", kvo.TInt64)
	Customer_PointsRedeemed = kvo.NewSignedProp[int64](Models, 5, "customer:points_redeemed", kvo.TInt64)

	CustomerModel = kvo.NewModel(Models, CustomerType, func(b *kvo.ModelBuilder) {
		b.Prop(Customer_FirstName)
		b.Prop(Customer_Email)
		b.Prop(Customer_Tags)
		b.Prop(Customer_PointsEarned)
		b.Prop(Customer_PointsRedeemed)
	})

	DBSchema              = &edb.Schema{}
	customersByEmailIndex *edb.Index[string]
	customersByTagIndex   *edb.Index[string]
	CustomersTable        = edb.DefineTable(DBSchema, "customers", CustomerModel, func(b *edb.TableBuilder[uint64]) {
		b.SetRecordStorage(edb.RecordStorage{Format: edb.Fast2})
		customersByEmailIndex = b.AddIndex[string]("by_email", func(_ *edb.Tx, _ values.Value, r kvo.Record, yield func(string)) {
			if email := r.Get(Customer_Email); email != "" {
				yield(email)
			}
		}, edb.IndexOptDeprecatedUnique)
		customersByTagIndex = b.AddIndex[string]("by_tag", func(_ *edb.Tx, _ values.Value, r kvo.Record, yield func(string)) {
			for tag := range r.Get(Customer_Tags).Iter() {
				yield(tag)
			}
		})

	})
	Customers    = CustomersTable
	CustomerRows = CustomersTable.Rows[Customer]()
)

func PointBalance(r kvo.Record) int64 {
	return r.Get(Customer_PointsEarned) - r.Get(Customer_PointsRedeemed)
}

// Both functions use the same secondary index and sum the same customer balances.
func balanceFromStructs(tx *edb.Tx, tag string) int64 {
	var total int64
	for customer := range CustomerRows.ExactIndexScan(tx, customersByTagIndex, tag).Rows() {
		total += customer.PointsEarned - customer.PointsRedeemed
	}
	return total
}

func balanceFromRecords(tx *edb.Tx, tag string) int64 {
	var total int64
	for customer := range Customers.ExactIndexScan(tx, customersByTagIndex, tag).Records() {
		total += PointBalance(customer)
	}
	return total
}

func redeem(db *edb.DB, id uint64, points int64) error {
	return db.Tx(edb.TxMetadata{}, true, func(tx *edb.Tx) error {
		edit := Customers.Edit(tx, id)
		if edit == nil {
			return fmt.Errorf("customer %d not found", id)
		}
		balance := edit.Get(Customer_PointsEarned) - edit.Get(Customer_PointsRedeemed)
		if points <= 0 || points > balance {
			return fmt.Errorf("cannot redeem %d points from a balance of %d", points, balance)
		}
		edit.Set(Customer_PointsRedeemed, edit.Get(Customer_PointsRedeemed)+points)
		Customers.Save(tx, edit)
		return nil
	})
}
