package bench_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

const stateCount = 4096

func accountStateRows(b *testing.B) []*statrow.State {
	b.Helper()
	var rows []*statrow.State
	if name := os.Getenv("EDB_ACCOUNT_STATE_JSON"); name != "" {
		raw, err := os.ReadFile(name)
		if err != nil || json.Unmarshal(raw, &rows) != nil || len(rows) == 0 {
			b.Fatal("invalid account-state replay")
		}
	} else {
		for id := uint64(1); id <= stateCount; id++ {
			rows = append(rows, statrow.Sample(id))
		}
	}
	for i, r := range rows {
		if r == nil || r.ID != uint64(i+1) || r.Key != r.ID {
			b.Fatal("expected consecutive surrogate keys starting at 1")
		}
	}
	return rows
}

func BenchmarkAccountState(b *testing.B) {
	rows := accountStateRows(b)
	type scenario struct{ entry, indexer string }
	for _, mode := range []string{"KVORecord", "StructReflect", "StructGenerated", "EVM", "MessagePackRecord", "MessagePackStruct", "MessagePackEVM"} {
		entries := []scenario{
			{"ReadCustomers", "SetIndexer"}, {"WriteCustomers", "SetIndexer"},
		}
		for _, cfg := range entries {
			entry := cfg.entry
			b.Run(mode+"/"+entry+"/"+cfg.indexer, func(b *testing.B) {
				models, model := statrow.Model()
				schema, table, idx := stateTable(mode, cfg.indexer, model)
				db, err := edb.Open(filepath.Join(b.TempDir(), "state.db"), schema, edb.Options{IsTesting: true})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				var wants [2]uint64
				db.Write(func(tx *edb.Tx) {
					for _, r := range rows {
						id := r.ID
						edb.Put(tx, r)
						if !r.Active {
							continue
						}
						for i := range wants {
							if entry == "ReadCustomers" {
								wants[i] += r.Profile.Balance*r.Profile.Rate + id
							} else {
								wants[i] += id*r.Profile.Rate + uint64(i+1)
							}
						}
					}
				})
				h := recordHost{db: db, table: table}
				pkg := h.guestPackage(b, models)
				module, err := evmcompiler.Compile("state.go", customerScript, pkg)
				if err != nil {
					b.Fatal(err)
				}
				prog, err := evm.Link(module, pkg)
				if err != nil {
					b.Fatal(err)
				}
				vm := evm.New(prog)
				var row statrow.State
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					rev := uint64(1 + i%2)
					var sum uint64
					if mode == "EVM" || mode == "MessagePackEVM" {
						sum, err = h.run(vm, entry, rev)
						if err != nil {
							b.Fatal(err)
						}
					} else {
						run := db.Read
						if entry == "WriteCustomers" {
							run = db.Write
						}
						run(func(tx *edb.Tx) {
							for c := edb.FullTableScan[statrow.State](tx); c.Next(); {
								if mode == "MessagePackRecord" || mode == "KVORecord" {
									v, _, err := c.TryRecord()
									if err != nil {
										b.Fatal(err)
									}
									if !v.GetValue(activeKey).AsBool() {
										continue
									}
									id := v.GetValue(idKey).AsUint()
									profile := row2.Step{Key: profileKey}
									if entry == "ReadCustomers" {
										p := v.Child(profile)
										sum += p.GetValue(balanceKey).AsUint()*p.GetValue(rateKey).AsUint() + id
										continue
									}
									edit, err := tx.TryOpenRecordEdit(table, id)
									if err != nil || edit == nil {
										b.Fatalf("open %d: %v", id, err)
									}
									e := edit.Editor()
									rate := e.GetPathValue([]row2.Step{profile, {Key: rateKey}}).AsUint()
									if err = e.TrySetValue(revisionKey, values.Uint(rev)); err != nil {
										b.Fatal(err)
									}
									balance := id*rate + rev
									if err = e.TryFastSetPathValue([]row2.Step{profile, {Key: balanceKey}}, values.Uint(balance)); err != nil {
										b.Fatal(err)
									}
									sum += balance
									_, err = tx.TrySaveRecord(edit)
									if err != nil {
										b.Fatal(err)
									}
								} else {
									if _, err := c.RowInto(&row); err != nil {
										b.Fatal(err)
									}
									if !row.Active {
										continue
									}
									if entry == "ReadCustomers" {
										sum += row.Profile.Balance*row.Profile.Rate + row.ID
										continue
									}
									row.Profile.Balance = row.ID*row.Profile.Rate + rev
									row.Revision = rev
									edb.Put(tx, &row)
									sum += row.Profile.Balance
								}
							}
						})
					}
					if sum != wants[i%2] {
						b.Fatalf("checksum %d, want %d", sum, wants[i%2])
					}
				}
				b.StopTimer()
				db.Read(func(tx *edb.Tx) {
					n := 0
					for c := edb.FullTableScan[statrow.State](tx); c.Next(); {
						r := c.Row()
						r.Created = r.Created.UTC()
						r.LastActivity = r.LastActivity.UTC()
						wantRow := *rows[r.Key-1]
						want := &wantRow
						if entry == "WriteCustomers" && want.Active {
							want.Revision = uint64(1 + (b.N-1)%2)
							want.Profile.Balance = want.ID*want.Profile.Rate + want.Revision
						}
						if !reflect.DeepEqual(r, want) {
							b.Fatalf("stored row %d differs", r.Key)
						}
						n++
					}
					if n != len(rows) {
						b.Fatal(n)
					}
					revision := uint64(0)
					if entry == "WriteCustomers" {
						revision = uint64(1 + (b.N-1)%2)
					}
					checkBalanceIndex(b, tx, idx, rows, revision)
				})
			})
		}
	}
}

func stateTable(mode, indexer string, model *kvo.Model) (*edb.Schema, *edb.TableDef, *edb.IndexDef) {
	schema := new(edb.Schema)
	var idx *edb.IndexDef
	table := edb.DefineTable[uint64](schema, "state", model, func(b *edb.TableBuilder[uint64]) {
		idx = b.AddIndex[uint64]("balance", nil).IndexDef()
		storage := edb.RecordStorage{Format: edb.Fast2}
		if mode != "StructReflect" {
			storage.Generated = &statrow.Static
			storage.Mode = nativebind.StaticOnly
		}
		switch indexer {
		case "SetIndexer":
			b.SetIndexer(func(_ uint64, r kvo.Record, ib *edb.IndexBuilder) {
				ib.Add(idx, r.Child(row2.Step{Key: profileKey}).GetValue(balanceKey).AsUint())
			})
		}
		if !strings.HasPrefix(mode, "MessagePack") {
			b.SetRecordStorage(storage)
		}
	}).TableDef()
	return schema, table, idx
}

// Each rebuild must clear and repopulate the entire balance index.
func BenchmarkAccountStateReindex(b *testing.B) {
	rows := accountStateRows(b)
	for _, indexer := range []string{"SetIndexer"} {
		b.Run(indexer, func(b *testing.B) {
			_, model := statrow.Model()
			schema, table, idx := stateTable("KVO", indexer, model)
			db, err := edb.Open(filepath.Join(b.TempDir(), "state.db"), schema, edb.Options{IsTesting: true})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.Write(func(tx *edb.Tx) {
				for _, r := range rows {
					edb.Put(tx, r)
				}
			})
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				db.Write(func(tx *edb.Tx) { tx.Reindex(table, idx) })
			}
			b.StopTimer()
			db.Read(func(tx *edb.Tx) {
				checkBalanceIndex(b, tx, idx, rows, 0)
			})
		})
	}
}

func checkBalanceIndex(b *testing.B, tx *edb.Tx, idx *edb.IndexDef, rows []*statrow.State, revision uint64) {
	b.Helper()
	seen := make(map[uint64]bool, len(rows))
	for c := tx.IndexScan(idx, edb.FullScan()); c.Next(); {
		key := c.Key().(uint64)
		r := rows[key-1]
		want := r.Profile.Balance
		if revision != 0 && r.Active {
			want = key*r.Profile.Rate + revision
		}
		if seen[key] || c.IndexKey().(uint64) != want {
			b.Fatal("invalid balance index")
		}
		seen[key] = true
	}
	if len(seen) != len(rows) {
		b.Fatal("incomplete balance index")
	}
}
