package edb

import (
	"bytes"
	"path/filepath"
	"slices"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func TestReplicationMergedMigrationPolicy_covers_native_records_and_reindex(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		for _, snapshot := range []bool{false, true} {
			mode := "apply"
			if snapshot {
				mode = "snapshot"
			}
			t.Run(format.String()+"/"+mode, func(t *testing.T) {
				sourceSchema, _ := changeRecordSchema(format)
				sink := &replicationTestSink{}
				source := openReplicationTest(t, sourceSchema, replicationCaptureOptions(sink))
				source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &changeRecordRow{ID: "first", Name: "source", Tags: []string{"keep"}}) })
				group := sink.snapshot()[0]
				defer releaseTestGroup(group)
				schema, table := changeRecordSchema(format)
				table.latestSchemaVer = 2
				var idx *IndexDef
				builder := TableBuilder[string]{tbl: table}
				idx = builder.AddIndex[string]("name", nil, IndexOptDeprecatedUnique).IndexDef()

				builder.SetIndexer(func(_ string, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.GetValue(1).AsString()) })
				migrations := 0
				builder.AddMigrator(2, func(_ *Tx, _ string, e *kvo.Editor) { migrations++; e.SetValue(1, values.String("migrated")) })
				path := filepath.Join(t.TempDir(), "db")
				if snapshot {
					source.Read(TxMetadata{}, func(tx *Tx) {
						if err := tx.btx.CopyFile(path, 0600); err != nil {
							t.Fatal(err)
						}
					})
				}
				db, err := Open(path, schema, Options{IsTesting: true, LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations, Replication: replicationApplyOptions()})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(db.Close)
				if !snapshot {
					changes := slices.Clone(group.Changes)
					changes[0].Object.Table = table
					if err := db.ApplyReplication(&ReplicationGroup{TransactionID: group.TransactionID, SchemaRev: group.SchemaRev, Changes: changes}); err != nil {
						t.Fatal(err)
					}
				}
				check := func(tx *Tx) {
					t.Helper()
					row := Get[changeRecordRow](tx, "first")
					if row == nil || row.ID != "first" || row.Name != "source" || len(row.Tags) != 1 || row.Tags[0] != "keep" {
						t.Fatalf("native row=%+v", row)
					}
					if r := tx.GetRecord(table, "first"); r.GetValue(1).AsString() != "source" {
						t.Fatal("record read ran migration")
					}
					cursor := tx.TableScan(table, FullScan())
					if !cursor.Next() || cursor.Record().GetValue(1).AsString() != "source" {
						t.Fatal("record cursor ran migration")
					}
					if Lookup[changeRecordRow](tx, idx, "source") == nil {
						t.Fatal("local index not built")
					}
					stored := rowReplicationValue(table.dataBucketIn(tx).Get(table.EncodeKey("first")))
					want := group.Changes[0].Value
					if stored.Flags != want.Flags || stored.SchemaVer != want.SchemaVer || stored.ModCount != want.ModCount || !bytes.Equal(stored.Data, want.Data) {
						t.Fatal("source representation changed")
					}
				}
				db.Read(TxMetadata{}, check)
				db.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(table, nil) })
				db.Read(TxMetadata{}, check)
				if migrations != 0 {
					t.Fatalf("migrations=%d", migrations)
				}
				requireReplicationID(t, db, group.TransactionID)
			})
		}
	}
}
