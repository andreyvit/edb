package edb

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func TestReplicationApply_main_index_callbacks_are_local_and_atomic(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			fail := false
			makeSchema := func(local bool) (*Schema, *TableDef, *IndexDef, *IndexDef) {
				sc := new(Schema)
				var name, email *IndexDef
				table := DefineTable(sc, "users", testIndexAccess[User](indexName, indexEmail), func(b *TableBuilder[ID]) {
					if format != MessagePack {
						b.SetRecordStorage(RecordStorage{Format: format})
					}
					if local {
						email = b.AddIndex[string]("email", func(tx *Tx, pk values.Value, r kvo.Record, yield func(string)) {
							if IndexPrimaryKey[ID](pk) != 1 || !tx.rawWrites {
								panic("invalid apply index context")
							}
							yield(r.Get(indexEmail))
						}, IndexOptDeprecatedUnique).IndexDef()
					}
					name = b.AddIndex[string]("name", func(tx *Tx, pk values.Value, r kvo.Record, yield func(string)) {
						if IndexPrimaryKey[ID](pk) != 1 {
							panic("index primary key lost")
						}
						n := r.Get(indexName)
						if local {
							if !tx.rawWrites {
								panic("local index work escaped raw mode")
							}
							if fail && n == "reject" {
								panic("local callback failed")
							}
							n = strings.ToUpper(n)
						}
						yield(n)
					}, IndexOptDeprecatedUnique).IndexDef()
				}).TableDef()
				return sc, table, name, email
			}
			sourceSchema, _, _, _ := makeSchema(false)
			sink := &replicationTestSink{}
			source := openReplicationTest(t, sourceSchema, replicationCaptureOptions(sink))
			targetSchema, targetTable, name, email := makeSchema(true)
			echo := &replicationTestSink{}
			opt := replicationCaptureOptions(echo)
			opt.Apply = replicationApplyOptions().Apply
			target := openReplicationTest(t, targetSchema, opt, Options{LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations})
			apply := func(index int) error {
				g := sink.snapshot()[index]
				changes := slices.Clone(g.Changes)
				changes[0].Object.Table = targetTable
				return target.ApplyReplication(&ReplicationGroup{TransactionID: g.TransactionID, SchemaRev: g.SchemaRev, Changes: changes})
			}
			source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "alice", Email: "a"}) })
			if err := apply(0); err != nil {
				t.Fatal(err)
			}
			target.Read(TxMetadata{}, func(tx *Tx) {
				if Lookup[User](tx, name, "ALICE") == nil || Lookup[User](tx, email, "a") == nil {
					t.Fatal("per-index callbacks were skipped")
				}
			})
			source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "bob", Email: "b"}) })
			if err := apply(1); err != nil {
				t.Fatal(err)
			}
			target.Read(TxMetadata{}, func(tx *Tx) {
				if Lookup[User](tx, name, "ALICE") != nil || Lookup[User](tx, email, "a") != nil || Lookup[User](tx, name, "BOB") == nil || Lookup[User](tx, email, "b") == nil {
					t.Fatal("callback index replacement failed")
				}
				stored := rowReplicationValue(targetTable.dataBucketIn(tx).Get(targetTable.EncodeKey(ID(1))))
				want := sink.snapshot()[1].Changes[0].Value
				if stored.Flags != want.Flags || stored.SchemaVer != want.SchemaVer || stored.ModCount != want.ModCount || !bytes.Equal(stored.Data, want.Data) {
					t.Fatal("callback indexing changed source representation")
				}
			})
			target.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(targetTable, nil) })
			source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "reject", Email: "c"}) })
			fail = true
			if err := apply(2); err == nil {
				t.Fatal("index callback failure accepted")
			}
			requireReplicationID(t, target, 2)
			target.Read(TxMetadata{}, func(tx *Tx) {
				if Lookup[User](tx, name, "BOB") == nil || Lookup[User](tx, email, "b") == nil || Lookup[User](tx, email, "c") != nil {
					t.Fatal("callback failure committed partial indexes")
				}
			})
			fail = false
			if err := apply(2); err != nil {
				t.Fatal(err)
			}
			source.Write(TxMetadata{}, func(tx *Tx) { tx.DeleteByKey(sourceSchema.TableNamed("users"), ID(1)) })
			if err := apply(3); err != nil {
				t.Fatal(err)
			}
			target.Read(TxMetadata{}, func(tx *Tx) {
				if Get[User](tx, ID(1)) != nil || Lookup[User](tx, name, "REJECT") != nil || Lookup[User](tx, email, "c") != nil {
					t.Fatal("callback index deletion failed")
				}
			})
			if len(echo.snapshot()) != 0 {
				t.Fatal("local callback index work recaptured")
			}
			requireReplicationID(t, target, 4)
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			replicationAssertClean(t, source)
			replicationAssertClean(t, target)
		})
	}
}
