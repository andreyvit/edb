package main

import (
	"fmt"
	"slices"

	"github.com/andreyvit/edb"
)

func Example_keySetsAndIndexUpdates() {
	db, err := edb.Open(edb.InMemory, DBSchema, edb.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.Write(func(tx *edb.Tx) {
		CustomerRows.Put(tx, &Customer{ID: 101, FirstName: "Alice", Tags: []string{"vip", "newsletter"}})
		CustomerRows.Put(tx, &Customer{ID: 102, FirstName: "Sam", Tags: []string{"staff", "newsletter"}})
		CustomerRows.Put(tx, &Customer{ID: 103, FirstName: "Maria", Tags: []string{"vip"}})
	})
	db.Read(func(tx *edb.Tx) {
		vip := Customers.ExactIndexScan(tx, customersByTagIndex, "vip").KeyCursor()
		staff := Customers.ExactIndexScan(tx, customersByTagIndex, "staff").KeyCursor()
		subscribed := Customers.ExactIndexScan(tx, customersByTagIndex, "newsletter").KeyCursor()
		for id := range vip.Union(staff).Intersect(subscribed).Keys() {
			// No customer row is fetched until after both set operations.
			fmt.Println("Send to:", Customers.Get(tx, id).Get(Customer_FirstName))
		}
	})
	db.Write(func(tx *edb.Tx) {
		// Saving changes by_tag. Finish scanning and own the keys before any write.
		ids := slices.Collect(Customers.ExactIndexScan(tx, customersByTagIndex, "vip").Keys())
		for _, id := range ids {
			edit := Customers.Edit(tx, id)
			if edit == nil {
				continue
			}
			edit.List(Customer_Tags).Remove("vip")
			Customers.Save(tx, edit) // Use the application's domain save helper when it has one.
		}
		fmt.Println("Remaining VIPs:", len(slices.Collect(Customers.ExactIndexScan(tx, customersByTagIndex, "vip").Keys())))
	})
	// Output:
	// Send to: Alice
	// Send to: Sam
	// Remaining VIPs: 0
}

func Example_scanBookmark() {
	db, err := edb.Open(edb.InMemory, DBSchema, edb.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.Write(func(tx *edb.Tx) {
		CustomerRows.Put(tx, &Customer{ID: 101, FirstName: "Alice", Tags: []string{"vip"}})
		CustomerRows.Put(tx, &Customer{ID: 102, FirstName: "Sam", Tags: []string{"vip"}})
	})
	// Manual transaction boundaries can include output flushing, time limits, or
	// other application work between pages. Capture the bookmark while reading.
	var after edb.ScanBookmark
	for {
		more := false
		db.Read(func(tx *edb.Tx) {
			page := Customers.ExactIndexScan(tx, customersByTagIndex, "vip").Resume(after).Chunk(1)
			for id, customer := range page.Entries() {
				fmt.Println(id, customer.Get(Customer_FirstName))
			}
			more = page.More()
			after = page.Bookmark()
		})
		if !more {
			break
		}
	}
	// Output:
	// 101 Alice
	// 102 Sam
}
