//go:build ignore

// Candidate-only integration probe for Fire's transaction-releasing exports.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/fdb/cursors"
	"go.etcd.io/bbolt"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func tenantOf(row any) (bm.TenantID, bool) {
	v := reflect.ValueOf(row)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return 0, false
	}
	t := v.FieldByName("TenantID")
	if !t.IsValid() {
		for _, name := range []string{"Key", "ID"} {
			k := v.FieldByName(name)
			if k.IsValid() && k.Kind() == reflect.Struct {
				t = k.FieldByName("TenantID")
				if t.IsValid() {
					break
				}
			}
		}
	}
	if t.IsValid() && t.CanUint() {
		return bm.TenantID(t.Uint()), true
	}
	return 0, false
}
func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private disposable database required")
	}
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true, ReuseTxBuffers: true})
	must(e)
	defer db.Close()
	wrapped := map[string]bool{}
	for _, t := range []*edb.TableDef{fdb.QuestionsTable.TableDef(), fdb.JobsTable.TableDef(), fdb.BatchCouponsTable.TableDef(), fdb.BatchCouponUses.TableDef(), fdb.IntegrationStatesTable.TableDef(), fdb.DiscountStacksTable.TableDef()} {
		wrapped[t.Name()] = true
	}
	for _, ss := range fdb.AllStatSchemas {
		wrapped[ss.StatSchemaDailyTable().Name()] = true
	}
	for _, tbl := range fdb.Schema.Tables() {
		if tbl.IndexNamed("by_tenant") == nil && !wrapped[tbl.Name()] {
			continue
		}
		groups := map[bm.TenantID]map[string]bool{}
		skipped := 0
		var unreadableKeys [][]byte
		db.Read(func(tx *edb.Tx) {
			c := tx.TableScan(tbl, edb.FullScan())
			for c.Next() {
				row, _, err := c.TryRow()
				if err != nil {
					skipped++
					unreadableKeys = append(unreadableKeys, bytes.Clone(c.RawKey()))
					continue
				}
				id, ok := tenantOf(row)
				if !ok {
					skipped++
					continue
				}
				if groups[id] == nil {
					groups[id] = map[string]bool{}
				}
				groups[id][string(bytes.Clone(c.RawKey()))] = true
			}
		})
		// The broad replay separately records historical decode failures. Remove
		// these only from this disposable continuation fixture, so they cannot
		// abort every later tenant's scan before pagination can be exercised.
		if len(unreadableKeys) > 0 {
			if tbl.Name() != "integration-states" || len(tbl.Indices()) != 0 {
				panic("unclassified historical decode failure")
			}
			must(db.Bolt().Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket([]byte(tbl.Name())).Bucket([]byte("data"))
				for _, key := range unreadableKeys {
					must(b.Delete(key))
				}
				return nil
			}))
		}
		ids := make([]bm.TenantID, 0, len(groups))
		for id := range groups {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		checks, rows, pages := 0, 0, 0
		for _, id := range ids {
			var want []string
			db.Read(func(tx *edb.Tx) {
				c := cursors.ByTenant[any](tx, tbl, id)
				for c.Next() {
					key := string(bytes.Clone(c.RawKey()))
					if !groups[id][key] {
						row, _, err := c.TryRow()
						tenant, ok := tenantOf(row)
						panic(fmt.Sprintf("unexpected row in %s: error_type=%T tenant_decoded=%v tenant_matches=%v", tbl.Name(), err, ok, tenant == id))
					}
					want = append(want, key)
				}
			})
			rows += len(want)
			// A full table oracle covers unconditional by_tenant indexes. Wrapped
			// indexes may intentionally exclude inactive rows (e.g. questions).
			if tbl.IndexNamed("by_tenant") != nil && len(want) != len(groups[id]) {
				panic(fmt.Sprintf("tenant index omitted rows in %s", tbl.Name()))
			}
			for _, size := range []int{1, 2, 3, 17, 127, 1024} {
				var after, indexAfter any
				var got []string
				for {
					n := 0
					db.Read(func(tx *edb.Tx) {
						c := cursors.ByTenant[any](tx, tbl, id, cursors.ScanAfter(after, indexAfter))
						for n < size && c.Next() {
							got = append(got, string(bytes.Clone(c.RawKey())))
							after = c.Key()
							if ic, ok := c.RawCursor.(*edb.RawIndexCursor); ok {
								indexAfter = ic.IndexKey()
							}
							n++
						}
					})
					pages++
					if len(got) > len(want) {
						panic(fmt.Sprintf("pagination duplicated rows in %s", tbl.Name()))
					}
					if n < size {
						break
					}
				}
				if !slices.Equal(got, want) {
					panic(fmt.Sprintf("pagination mismatch in %s at batch size %d", tbl.Name(), size))
				}
				checks++
			}
		}
		must(json.NewEncoder(os.Stdout).Encode(map[string]any{"table": tbl.Name(), "tenants": len(ids), "rows": rows, "checks": checks, "transactions": pages, "skipped_unreadable_or_no_tenant": skipped, "errors": 0}))
	}
}
