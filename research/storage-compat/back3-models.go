//go:build ignore

// Run from back3: go run /path/to/edb/research/storage-compat/back3-models.go DISPOSABLE_DB
// Opens with back3's full schema. Never use an active or original database.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
	"github.com/prairiegroupinc/fire/fdb"
)

func main() {
	if len(os.Args) != 2 {
		panic("supply a disposable snapshot path")
	}
	db, err := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true, NoPersistentFreeList: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	type sample struct {
		table *edb.TableDef
		key   any
		raw   []byte
	}
	var edits []sample
	db.Read(func(tx *edb.Tx) {
		for _, table := range []*edb.TableDef{fdb.CustomersTable, fdb.CustomerStatsTable, fdb.ProductsTable, fdb.OrdersTable} {
			checked, seen := 0, 0
			for c := tx.TableScan(table, edb.FullScan()); c.Next(); {
				seen++
				if seen > 100 && seen%257 != 0 {
					continue
				}
				row, _ := c.RowVal()
				if checked < 10 {
					edits = append(edits, sample{table, c.Key(), bytes.Clone(c.RawRow())})
				}
				binding := kvo.Project(row.Elem().Type(), table.Model(), nil)
				native, encoded := binding.Record(row), c.Record()
				want := native.ToJSON(table.Model(), kvo.JSONOptions{})
				got := encoded.ToJSON(table.Model(), kvo.JSONOptions{})
				if len(got) != len(want) {
					panic(table.Name() + ": modeled field counts differ")
				}
				// Marshal only for the oracle: custom scalar JSON methods and
				// exact integer values must agree, independent of Go concrete types.
				for key, v := range want {
					actual, found := got[key]
					if !found {
						panic(table.Name() + ": encoded record omits field " + key)
					}
					a, err := json.Marshal(v)
					if err != nil {
						panic(err)
					}
					b, err := json.Marshal(actual)
					if err != nil {
						panic(err)
					}
					if !bytes.Equal(a, b) {
						// Report field/type only; do not print private customer data.
						panic(fmt.Sprintf("%s sample %d field %s differs (%v/%v)", table.Name(), checked, key, reflect.TypeOf(v), reflect.TypeOf(got[key])))
					}
				}
				checked++
				if checked == 1000 {
					break
				}
			}
			if checked == 0 {
				panic(table.Name() + ": no rows available to verify")
			}
			fmt.Printf("%s: %d complete modeled records agree after traversing %d rows\n", table.Name(), checked, seen)
		}
	})
	for _, sample := range edits {
		func() {
			tx := db.BeginUpdate()
			defer tx.Close() // deliberately never commit the fixture edit
			native, before := tx.Get(sample.table, sample.key)
			want := reflect.ValueOf(native)
			tenant := want.Elem().FieldByName("TenantID")
			next := tenant.Uint() ^ 1
			tenant.SetUint(next)
			edit := tx.OpenRecordEdit(sample.table, sample.key)
			edit.Editor().SetValue(sample.table.Model().PropByName("tenant_id").Code(), values.Uint(next))
			after, err := tx.TrySaveRecord(edit)
			if err != nil {
				panic(err)
			}
			got, _ := tx.Get(sample.table, sample.key)
			if after.ModCount != before.ModCount+1 || !reflect.DeepEqual(got, want.Interface()) {
				panic(sample.table.Name() + ": real row edit changed other native fields")
			}
		}()
		db.Read(func(tx *edb.Tx) {
			c := tx.TableScan(sample.table, edb.ExactScan(sample.key))
			if !c.Next() || !bytes.Equal(c.RawRow(), sample.raw) {
				panic(sample.table.Name() + ": rollback did not restore original bytes")
			}
		})
	}
	fmt.Printf("%d real-row indexed edits preserved all other native fields and rolled back byte-for-byte\n", len(edits))
}
