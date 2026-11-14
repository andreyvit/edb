package edb

import (
	"testing"

	"github.com/andreyvit/edb/kvo"
)

func TestReindex_build_completion_and_rollback_do_not_depend_on_capture(t *testing.T) {
	for _, capture := range []bool{false, true} {
		name := "disabled"
		if capture {
			name = "capture"
		}
		t.Run(name, func(t *testing.T) {
			sc := new(Schema)
			var idx *IndexDef
			fail := false
			tbl := DefineTable(sc, "users", testIndexAccess[User](indexName), func(b *TableBuilder[ID]) {
				idx = b.AddIndex[string]("name", nil, IndexOptDeprecatedUnique).IndexDef()

				b.SetIndexer(func(_ ID, r kvo.Record, ib *IndexBuilder) {
					if fail {
						panic("build failed")
					}
					ib.Add(idx, r.Get(indexName))
				})
			}).TableDef()
			sink := &replicationTestSink{}
			var opt *ReplicationOptions
			if capture {
				opt = replicationCaptureOptions(sink)
			}
			db := openReplicationTest(t, sc, opt)
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "seed"}) })
			ts := db.tableState(tbl)
			db.Write(TxMetadata{}, func(tx *Tx) { ts.Indices[idx.name].Built = false; ts.save(tx) })
			fail = true
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { return safelyCall(func(tx *Tx) error { tx.Reindex(tbl, idx); return nil }, tx) })
			if err == nil {
				t.Fatal("failed build accepted")
			}
			if ts.Indices[idx.name].Built {
				t.Fatal("failed build marked complete")
			}
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Lookup[User](tx, idx, "seed") == nil {
					t.Fatal("failed build committed an empty index")
				}
			})
			fail = false
			db.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(tbl, idx) })
			if !ts.Indices[idx.name].Built {
				t.Fatal("successful build not marked complete")
			}
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Lookup[User](tx, idx, "seed") == nil {
					t.Fatal("successful build lost row")
				}
			})
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			if capture {
				requireReplicationID(t, db, 1)
				replicationAssertClean(t, db)
			}
		})
	}
}
