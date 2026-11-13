//go:build ignore

// Probe the actual Fire notification payload choices on sampled existing rows.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/nm"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func digest(v any) string {
	b, e := json.Marshal(v)
	must(e)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private disposable database required")
	}
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true, ReuseTxBuffers: true})
	must(e)
	defer db.Close()
	normal := edb.ChangeFlagNotify | edb.ChangeFlagIncludeKey
	options := map[*edb.TableDef]edb.ChangeFlags{
		fdb.TenantsTable.TableDef():        normal | edb.ChangeFlagIncludeNewGoStruct | edb.ChangeFlagIncludeOldGoStruct,
		fdb.TenantSettingsTable.TableDef(): normal | edb.ChangeFlagIncludeNewGoStruct,
		fdb.TenantThemesTable.TableDef():   normal,
		fdb.QuestionsTable.TableDef():      normal | edb.ChangeFlagIncludeNewRecord | edb.ChangeFlagIncludeOldRecord,
		fdb.UsersTable.TableDef():          normal | edb.ChangeFlagIncludeNewGoStruct,
		fdb.JobsTable.TableDef():           normal,
	}
	retainedNative := []any{}
	retainedHashes := []string{}
	retainedRecords := []kvo.Record{}
	retainedTenants := []bm.TenantID{}
	snapshotID := bm.ID(0xffe0000000000000)
	for _, tbl := range fdb.Schema.Tables() {
		flags, ok := options[tbl]
		if !ok {
			continue
		}
		var keys [][]byte
		db.Read(func(tx *edb.Tx) {
			c := tx.TableScan(tbl, edb.FullScan())
			for c.Next() && len(keys) < 256 {
				keys = append(keys, bytes.Clone(c.RawKey()))
			}
		})
		count, skipped, snapshots := 0, 0, 0
		for _, key := range keys {
			var original any
			db.Read(func(tx *edb.Tx) { original, _ = tx.GetByKeyRaw(tbl, key) })
			v := reflect.ValueOf(original)
			field := -1
			for i := 0; i < v.Elem().NumField(); i++ {
				f := v.Elem().Type().Field(i)
				if f.IsExported() && v.Elem().Field(i).Type() == reflect.TypeFor[string]() && strings.Split(f.Tag.Get("msgpack"), ",")[0] != "-" {
					field = i
					break
				}
			}
			if field < 0 {
				skipped++
				continue
			}
			originalHash := digest(original)
			var updatedHash string
			run := func(deleting, commit bool) {
				tx := db.BeginUpdate(edb.TxMetadata{})
				defer tx.Close()
				before, _ := tx.GetByKeyRaw(tbl, key)
				notified := 0
				tx.OnChange(options, func(tx *edb.Tx, chg *edb.Change) {
					if chg.Table() != tbl {
						panic("unexpected nested notification")
					}
					notified++
					if !bytes.Equal(chg.RawKey(), key) || !reflect.DeepEqual(chg.Key(), tbl.DecodeKeyVal(key).Interface()) {
						panic("notification key mismatch")
					}
					now, _ := tx.GetByKeyRaw(tbl, key)
					if deleting {
						if chg.Op() != edb.OpDelete || now != nil || chg.HasNewGoStruct() || chg.HasNewRecord() {
							panic("delete notification state mismatch")
						}
					} else if chg.Op() != edb.OpPut || now == nil {
						panic("put notification state mismatch")
					}
					if flags.Contains(edb.ChangeFlagIncludeOldGoStruct) {
						if !reflect.DeepEqual(chg.OldGoStruct(), before) {
							panic("old payload mismatch")
						}
						retainedNative = append(retainedNative, chg.OldGoStruct())
						retainedHashes = append(retainedHashes, digest(before))
					}
					if !deleting && flags.Contains(edb.ChangeFlagIncludeNewGoStruct) {
						if !reflect.DeepEqual(chg.NewGoStruct(), now) {
							panic("new payload mismatch")
						}
						retainedNative = append(retainedNative, chg.NewGoStruct())
						retainedHashes = append(retainedHashes, digest(now))
					}
					if flags.Contains(edb.ChangeFlagIncludeOldRecord) {
						tenant := reflect.ValueOf(before).Elem().FieldByName("TenantID").Interface().(bm.TenantID)
						if !chg.HasOldRecord() || chg.OldRecord().Get(nm.Tenant_ID) != tenant {
							panic("old record tenant mismatch")
						}
						retainedRecords = append(retainedRecords, chg.OldRecord().Own())
						retainedTenants = append(retainedTenants, tenant)
					}
					if !deleting && flags.Contains(edb.ChangeFlagIncludeNewRecord) {
						tenant := reflect.ValueOf(now).Elem().FieldByName("TenantID").Interface().(bm.TenantID)
						if !chg.HasNewRecord() || chg.NewRecord().Get(nm.Tenant_ID) != tenant {
							panic("new record tenant mismatch")
						}
						retainedRecords = append(retainedRecords, chg.NewRecord().Own())
						retainedTenants = append(retainedTenants, tenant)
					}
					if !deleting && tbl == fdb.UsersTable.TableDef() {
						u := chg.NewGoStruct().(*bm.User)
						snapshotID++
						edb.Put(tx, &bm.UserSnapshot{ID: snapshotID, UserID: u.ID, User: *u})
						snap := edb.Get[bm.UserSnapshot](tx, snapshotID)
						// The nested User.ID is msgpack:"-"; UserID is the
						// snapshot's persisted identity in both deployed schemas.
						wantUser := *u
						wantUser.ID = 0
						if snap.UserID != u.ID || !reflect.DeepEqual(snap.User, wantUser) {
							var fields []string
							a, b := reflect.ValueOf(snap.User), reflect.ValueOf(wantUser)
							for i := 0; i < a.NumField(); i++ {
								if a.Type().Field(i).IsExported() && !reflect.DeepEqual(a.Field(i).Interface(), b.Field(i).Interface()) {
									fields = append(fields, a.Type().Field(i).Name)
								}
							}
							panic(fmt.Sprintf("reentrant user snapshot changed fields: %v", fields))
						}
						snapshots++
					}
				})
				if deleting {
					tx.DeleteByKeyRaw(tbl, key)
				} else {
					row, _ := tx.GetByKeyRaw(tbl, key)
					f := reflect.ValueOf(row).Elem().Field(field)
					f.SetString(f.String() + "\u2063audit")
					tx.Put(tbl, row)
					updatedHash = digest(row)
				}
				if notified != 1 {
					panic("notification count mismatch")
				}
				if commit {
					must(tx.Commit())
				}
				count++
			}
			run(false, false)
			db.Read(func(tx *edb.Tx) {
				row, _ := tx.GetByKeyRaw(tbl, key)
				if digest(row) != originalHash {
					panic("rollback persisted notification write")
				}
			})
			run(false, true)
			run(true, false)
			db.Read(func(tx *edb.Tx) {
				row, _ := tx.GetByKeyRaw(tbl, key)
				if digest(row) != updatedHash {
					panic("rolled-back delete changed row")
				}
			})
			run(true, true)
		}
		must(json.NewEncoder(os.Stdout).Encode(map[string]any{"table": tbl.Name(), "rows": len(keys), "mutations": count, "skipped_no_string_field": skipped, "user_snapshot_writes": snapshots, "errors": 0}))
	}
	for i, v := range retainedNative {
		if digest(v) != retainedHashes[i] {
			panic("retained notification struct changed")
		}
	}
	for i, r := range retainedRecords {
		if r.Get(nm.Tenant_ID) != retainedTenants[i] {
			panic("owned notification record changed")
		}
	}
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"retained_native_payloads": len(retainedNative), "retained_records": len(retainedRecords), "errors": 0}))
}
