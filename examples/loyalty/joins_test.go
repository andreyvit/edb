package main

import (
	"fmt"
	"reflect"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
)

func Example_recordsAndCompositeKeys() {
	type StatsKey struct {
		CustomerID uint64
		Day        string // YYYY-MM-DD sorts chronologically.
	}
	type DailyStats struct {
		Key    StatsKey `msgpack:"-" kvo:"-"`
		Orders int64    `msgpack:"orders" kvo:"orders"`
	}
	models := kvo.NewSchema()
	orders := kvo.NewSignedProp[int64](models, 1, "daily:orders", kvo.TInt64)
	model := kvo.NewModel(models, kvo.NewEntityType(models, "daily", reflect.TypeFor[DailyStats]()), func(b *kvo.ModelBuilder) {
		b.Prop(orders)
	})
	schema := &edb.Schema{}
	schema.Include(DBSchema)
	statsTable := edb.DefineTable(schema, "daily_stats", model, func(b *edb.TableBuilder[StatsKey]) {
		b.SetRecordStorage(edb.RecordStorage{Format: edb.Fast2})
	}).TableDef()
	statsRows := statsTable.Rows[DailyStats, StatsKey]()
	stats := statsTable.Typed[StatsKey]()
	db, err := edb.Open(edb.InMemory, schema, edb.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
		CustomerRows.Put(tx, &Customer{ID: 101, FirstName: "Alice", Tags: []string{"vip"}})
		CustomerRows.Put(tx, &Customer{ID: 102, FirstName: "Sam", Tags: []string{"vip"}})
		statsRows.Put(tx, &DailyStats{StatsKey{101, "2026-11-09"}, 2})
		statsRows.Put(tx, &DailyStats{StatsKey{101, "2026-11-10"}, 3})
		statsRows.Put(tx, &DailyStats{StatsKey{102, "2026-11-10"}, 4})
	})
	db.Read(func(tx *edb.Tx) {
		var total int64
		for id, customer := range Customers.ExactIndexScan(tx, customersByTagIndex, "vip").Entries() {
			day := stats.Get(tx, StatsKey{id, "2026-11-10"})
			if !day.Missing() {
				fmt.Println(customer.Get(Customer_FirstName), day.Get(orders))
				total += day.Get(orders)
			}
		}
		fmt.Println("VIP orders:", total)
		// Prefix matches the first tuple component, not a prefix of Day's text.
		for key := range stats.Prefix(tx, StatsKey{CustomerID: 101}, 1).Keys() {
			fmt.Println("Alice:", key.Day)
		}
	})
	// Each chunk owns a read transaction; joins use that exact transaction.
	var chunkedTotal int64
	for chunk := range edb.ReadChunks(db, 1, func(tx *edb.Tx) edb.RecordScan[uint64] {
		return Customers.ExactIndexScan(tx, customersByTagIndex, "vip")
	}) {
		for id := range chunk.Keys() {
			day := stats.Get(chunk, StatsKey{id, "2026-11-10"})
			if !day.Missing() {
				chunkedTotal += day.Get(orders)
			}
		}
	}
	fmt.Println("Chunked VIP orders:", chunkedTotal)
	// Output:
	// Alice 3
	// Sam 4
	// VIP orders: 7
	// Alice: 2026-11-09
	// Alice: 2026-11-10
	// Chunked VIP orders: 7
}
