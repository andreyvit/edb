package edb

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
)

func TestRecordMigrationReadSaveAndReindex(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		for _, changesData := range []bool{false, true} {
			t.Run(format.String()+map[bool]string{false: "/version-only", true: "/data"}[changesData], func(t *testing.T) {
				schema, table := changeRecordSchema(format)
				var idx *IndexDef
				b := TableBuilder[string]{tbl: table}
				idx = b.AddIndex[string]("name", nil).IndexDef()
				b.SetIndexer(func(_ string, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.GetValue(1).AsString()) })
				calls := 0
				b.AddMigrator(2, func(_ *Tx, key string, e *kvo.Editor) {
					calls++
					if key != "first" && key != "second" {
						t.Fatalf("key=%q", key)
					}
					if changesData {
						e.SetValue(1, values.String(e.GetValue(1).AsString()+"!"))
					}
				})
				db := setup(t, schema)
				db.Write(TxMetadata{}, func(tx *Tx) {
					Put(tx, &changeRecordRow{ID: "first", Name: "old", Tags: []string{"untouched"}}, &changeRecordRow{ID: "second", Name: "old", Tags: []string{"untouched"}})
				})
				table.latestSchemaVer = 2
				want := "old"
				if changesData {
					want += "!"
				}
				db.Read(func(tx *Tx) {
					rawBefore := bytes.Clone(table.dataBucketIn(tx).Get(table.EncodeKey("first")))
					native := Get[changeRecordRow](tx, "first")
					if native.Name != want || native.ID != "first" || native.Tags[0] != "untouched" {
						t.Fatal(native)
					}
					r, meta, err := tx.TryGetRecord(table, "first")
					if err != nil || meta.SchemaVer != 1 || r.GetValue(1).AsString() != want {
						t.Fatal(meta, err)
					}
					if format == MessagePack && r.MessagePack() == nil {
						t.Fatal("decoded MessagePack migration")
					}

					if !bytes.Equal(rawBefore, table.dataBucketIn(tx).Get(table.EncodeKey("first"))) {
						t.Fatal("read persisted migration")
					}
				})
				if calls != 2 {
					t.Fatal("migration ran more than once per read", calls)
				}
				db.Write(TxMetadata{}, func(tx *Tx) {
					first := tx.OpenRecordEdit(table, "first")
					stale := tx.OpenRecordEdit(table, "first")
					tx.SaveRecord(first) // Persist migration even without explicit edits.
					after := calls
					tx.SaveRecord(first)
					if calls != after {
						t.Fatal("save reran migration")
					}
					if _, err := tx.TrySaveRecord(stale); err == nil {
						t.Fatal("accepted stale pre-migration edit")
					}
					_, meta, err := tx.TryGetRecord(table, "first")
					if err != nil || meta.SchemaVer != 2 {
						t.Fatal(meta, err)
					}
					if !tx.IndexScan(idx, ExactScan(want)).Next() {
						t.Fatal("save did not rebuild index")
					}
					tx.Reindex(table, nil)
					if Get[changeRecordRow](tx, "second").Name != want {
						t.Fatal("reindex did not migrate")
					}
				})
				db.Read(func(tx *Tx) {
					for _, key := range []string{"first", "second"} {
						r, meta, err := tx.TryGetRecord(table, key)
						if err != nil || meta.SchemaVer != 2 || r.GetValue(1).AsString() != want {
							t.Fatal(key, meta, err)
						}
					}
				})
			})
		}
	}
}

func TestRecordMigrationProjectionPreservesUnmodeledFields(t *testing.T) {
	s := kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "projection"), func(b *kvo.ModelBuilder) { b.Prop(name) })
	schema := new(Schema)
	m.RegisterNativeType(reflect.TypeFor[changeProjectedRow](), map[kvo.PropCode]string{1: "Name"})
	table := DefineTable(schema, "projection", m, func(b *TableBuilder[uint64]) {
		b.SetSchemaVersion(2)
		b.AddMigrator(2, func(_ *Tx, key uint64, e *kvo.Editor) {
			if key != 9 {
				t.Fatal(key)
			}
			e.Set(name, e.Get(name)+"!")
		})
	}).TableDef()
	table.latestSchemaVer = 1
	db := setup(t, schema)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &changeProjectedRow{9, "old", "opaque"}) })
	table.latestSchemaVer = 2
	changeOpaqueDecodes = 0
	db.Write(TxMetadata{}, func(tx *Tx) {
		edit := tx.OpenRecordEdit(table, uint64(9))
		tx.SaveRecord(edit)
		if changeOpaqueDecodes != 0 {
			t.Fatal("encoded migration decoded a Go struct", changeOpaqueDecodes)
		}
		native := Get[changeProjectedRow](tx, uint64(9))
		if native.Name != "old!" || native.Opaque != "opaque" || native.ID != 9 {
			t.Fatal(native)
		}
	})
}

func TestRecordMigrationFailureRollsBackReindex(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			schema, table := changeRecordSchema(format)
			var idx *IndexDef
			b := TableBuilder[string]{tbl: table}
			idx = b.AddIndex[string]("name", nil).IndexDef()
			b.SetIndexer(func(_ string, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.GetValue(1).AsString()) })
			calls := 0
			b.AddMigrator(2, func(_ *Tx, key string, e *kvo.Editor) {
				calls++
				e.SetValue(1, values.String("new"))
				if key == "second" {
					panic("migration failed")
				}
			})
			db := setup(t, schema)
			db.Write(TxMetadata{}, func(tx *Tx) {
				Put(tx, &changeRecordRow{ID: "first", Name: "old"}, &changeRecordRow{ID: "second", Name: "old"})
			})
			before := make(map[string][]byte)
			db.Read(func(tx *Tx) {
				for _, key := range []string{"first", "second"} {
					before[key] = bytes.Clone(table.dataBucketIn(tx).Get(table.EncodeKey(key)))
				}
			})
			table.latestSchemaVer = 2
			assertRegistrationPanic(t, "migration failed", func() { db.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(table, nil) }) })
			if calls != 2 {
				t.Fatalf("expected a failure after the first migrated write, got %d calls", calls)
			}
			db.Read(func(tx *Tx) {
				for key, raw := range before {
					if !bytes.Equal(raw, table.dataBucketIn(tx).Get(table.EncodeKey(key))) {
						t.Fatalf("failed migration persisted %s", key)
					}
				}
				if tx.IndexScan(idx, ExactScan("new")).Next() {
					t.Fatal("failed migration persisted new index entries")
				}
				c := tx.IndexScan(idx, ExactScan("old"))
				count := 0
				for c.Next() {
					count++
				}
				if count != 2 {
					t.Fatal("failed migration lost old index entries", count)
				}
			})
		})
	}
}

type MigrationAliasValues struct {
	Name string `kvo:"name" msgpack:"name"`
}
type migrationAliasRow struct {
	ID                   uint64 `kvo:"-" msgpack:"-"`
	MigrationAliasValues `kvo:",inline"`
	Hidden               string `kvo:"-" msgpack:"hidden"`
}

func TestMessagePackAliasMigrationSaveAndReindex(t *testing.T) {
	for _, migrate := range []bool{false, true} {
		for _, reindex := range []bool{false, true} {
			name := map[bool]string{false: "current", true: "upgrade"}[migrate] + "/" + map[bool]string{false: "save", true: "reindex"}[reindex]
			t.Run(name, func(t *testing.T) {
				s := kvo.NewSchema()
				prop := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
				model := kvo.NewModel(s, kvo.NewEntityType(s, "alias", reflect.TypeFor[migrationAliasRow]()), func(b *kvo.ModelBuilder) { b.Prop(prop) })
				schema := new(Schema)
				var idx *IndexDef
				table := DefineTable(schema, "alias", model, func(b *TableBuilder[uint64]) {
					b.SetSchemaVersion(2)
					idx = b.AddIndex[string]("name", nil).IndexDef()
					b.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.Get(prop)) })
					b.AddMigrator(2, func(_ *Tx, key uint64, e *kvo.Editor) {
						if key != 7 {
							t.Fatal(key)
						}
						e.Set(prop, e.Get(prop)+"!")
					})
				}).TableDef()
				table.latestSchemaVer = 1
				db := setup(t, schema)
				db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &migrationAliasRow{7, MigrationAliasValues{"flat"}, "keep"}) })
				db.Write(TxMetadata{}, func(tx *Tx) {
					bucket := table.dataBucketIn(tx)
					key := table.EncodeKey(uint64(7))
					var v value
					ensure(v.decode(bucket.Get(key), false))
					alias := msgpack.Encode(nil, reflect.ValueOf(map[string]string{"name": "nested"}))
					v.Data = msgpack.Trusted(v.Data).Set("MigrationAliasValues", alias).Raw()
					out := append(make([]byte, maxValueHeaderSize), v.Data...)
					off := len(out)
					out = append(out, v.Index...)
					ensure(bucket.Put(key, putValueHeader(out, v.Flags, v.SchemaVer, v.ModCount, off)))
				})
				want := "nested"
				if migrate {
					table.latestSchemaVer = 2
					want += "!"
				}
				db.Read(func(tx *Tx) {
					row := tx.GetRecord(table, uint64(7)).Native().(*migrationAliasRow)
					if row.ID != 7 || row.Name != want || row.Hidden != "keep" {
						t.Fatalf("fallback read: %#v", row)
					}
				})
				db.Write(TxMetadata{}, func(tx *Tx) {
					if reindex {
						tx.Reindex(table, nil)
					} else {
						edit := tx.OpenRecordEdit(table, uint64(7))
						if !migrate {
							want += "?"
							edit.Set(prop, want)
						}
						tx.SaveRecord(edit)
					}
				})
				db.Read(func(tx *Tx) {
					row := Get[migrationAliasRow](tx, uint64(7))
					if row.ID != 7 || row.Name != want || row.Hidden != "keep" || tx.GetMeta(table, uint64(7)).SchemaVer != table.latestSchemaVer {
						t.Fatalf("fallback save: %#v", row)
					}
					if !tx.IndexScan(idx, ExactScan(want)).Next() {
						t.Fatal("missing migrated index")
					}
					if Get[migrationAliasRow](tx, uint64(0)) != nil {
						t.Fatal("fallback saved with zero primary key")
					}
				})
			})
		}
	}
}

func TestMigratorRegistration(t *testing.T) {
	for _, test := range []struct {
		name, message string
		configure     func(*TableBuilder[string])
	}{
		{"nil", "non-nil callback", func(b *TableBuilder[string]) { b.AddMigrator(2, nil) }},
		{"zero", "positive version", func(b *TableBuilder[string]) { b.AddMigrator(0, func(*Tx, string, *kvo.Editor) {}) }},
		{"future", "exceeds schema version", func(b *TableBuilder[string]) {
			b.AddMigrator(3, func(*Tx, string, *kvo.Editor) {})
			b.SetSchemaVersion(2)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertRegistrationPanic(t, test.message, func() { DefineTable(new(Schema), "rows", testTableModel[changeRecordRow](), test.configure).TableDef() })
		})
	}
}

func TestMigratorsRunInRegistrationOrder(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			schema, table := changeRecordSchema(format)
			b := TableBuilder[string]{tbl: table}
			var calls []uint64
			for _, version := range []uint64{3, 2, 3, 4} {
				b.AddMigrator(version, func(_ *Tx, key string, e *kvo.Editor) {
					if key != "key" {
						t.Fatal(key)
					}
					calls = append(calls, version)
					e.SetValue(1, values.String(e.GetValue(1).AsString()+fmt.Sprint(version)))
				})
			}
			db := setup(t, schema)
			for _, original := range []uint64{1, 2, 3, 4} {
				table.latestSchemaVer = original
				db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &changeRecordRow{ID: "key", Name: "v"}) })
				table.latestSchemaVer = 4
				var wantCalls []uint64
				want := "v"
				for _, v := range []uint64{3, 2, 3, 4} {
					if original < v {
						wantCalls = append(wantCalls, v)
						want += fmt.Sprint(v)
					}
				}
				db.Read(func(tx *Tx) {
					calls = nil
					if got := tx.GetRecord(table, "key").GetValue(1).AsString(); got != want {
						t.Fatal(got, want)
					}
					if !slices.Equal(calls, wantCalls) {
						t.Fatal(calls, wantCalls)
					}
					calls = nil
					if got := Get[changeRecordRow](tx, "key").Name; got != want {
						t.Fatal(got, want)
					}
					if !slices.Equal(calls, wantCalls) {
						t.Fatal(calls, wantCalls)
					}
				})
			}
		})
	}
}
