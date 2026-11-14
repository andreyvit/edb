package edb

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

func TestLocalWritePolicyFilteredReplica(t *testing.T) {
	policy := LocalWritePolicy{Mode: RejectLocalWrites, Include: func(obj ReplicationObject) bool { return obj.Table == usersTable }}
	db := openReplicationTest(t, basicSchema, replicationApplyOptions(), Options{LocalWrites: policy, RowMigrations: SkipRowMigrations})
	if db.AllowsLocalWrites() {
		t.Fatal("filtered replica reported unrestricted writes")
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Email: "local"}) })
	requireReplicationID(t, db, 0)
	db.Read(TxMetadata{}, func(tx *Tx) {
		if !LookupExists(tx, usersByEmail, "local") {
			t.Fatal("local index missing")
		}
	})
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
		tx.CommitDespiteError()
		DeleteByKey[User](tx, ID(1))
		func() {
			defer func() {
				if recover() != ErrLocalWriteRejected {
					t.Fatal("expected write rejection")
				}
			}()
			tx.KVPutRaw(kubets, []byte("protected"), []byte("v"))
		}()
		return nil
	})
	if !errors.Is(err, ErrLocalWriteRejected) {
		t.Fatalf("mixed transaction: %v", err)
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		if !LookupExists(tx, usersByEmail, "local") {
			t.Fatal("partial delete committed")
		}
	})
	var (
		sink   = &replicationTestSink{}
		source = openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	)
	source.Write(TxMetadata{}, func(tx *Tx) { tx.KVPutRaw(kubets, []byte("protected"), []byte("remote")) })
	group := sink.snapshot()[0]
	defer releaseTestGroup(group)
	if err = db.ApplyReplication(group); err != nil {
		t.Fatal(err)
	}
	db.Write(TxMetadata{}, func(tx *Tx) { DeleteByKey[User](tx, ID(1)) })
	requireReplicationID(t, db, group.TransactionID)
	db.Read(TxMetadata{}, func(tx *Tx) {
		if LookupExists(tx, usersByEmail, "local") || string(tx.KVGetRaw(kubets, []byte("protected"))) != "remote" {
			t.Fatal("unexpected state after replay and local delete")
		}
	})
}

func TestWritePolicies_without_replication(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(map[bool]string{false: "bolt", true: "memory"}[memory], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			if memory {
				path = InMemory
			}
			db, err := Open(path, basicSchema, Options{IsTesting: true, LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if db.AllowsLocalWrites() || db.AllowsRowMigrations() {
				t.Fatal("policies were not installed")
			}
			if err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { tx.CommitDespiteError(); Put(tx, &User{ID: 1}); return nil }); err == nil {
				t.Fatal("application write admitted without replication")
			}
			db.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(usersTable, nil) })
			db.Read(TxMetadata{}, func(tx *Tx) {
				if CountAll(tx, usersTable) != 0 {
					t.Fatal("fenced mutation persisted")
				}
			})
			if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 1}); err == nil {
				t.Fatal("unconfigured apply admitted")
			}
		})
	}
}

func TestReplicationCapabilities_apply_does_not_fence_local_writes(t *testing.T) {
	db := openReplicationTest(t, basicSchema, replicationApplyOptions())
	if !db.AllowsLocalWrites() || !db.AllowsRowMigrations() {
		t.Fatal("apply capability changed unrelated policies")
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "local"}) })
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(1)).Name != "local" {
			t.Fatal("local write failed")
		}
	})
}

func TestReplicationCapabilities_capture_and_apply_are_independent(t *testing.T) {
	upstreamSink := &replicationTestSink{}
	upstream := openReplicationTest(t, basicSchema, replicationCaptureOptions(upstreamSink))
	upstream.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "upstream", Email: "upstream"}) })
	sink := &replicationTestSink{}
	o := replicationCaptureOptions(sink)
	o.Apply = replicationApplyOptions().Apply
	db := openReplicationTest(t, basicSchema, o)
	if err := db.ApplyReplication(upstreamSink.snapshot()[0]); err != nil {
		t.Fatal(err)
	}
	if len(sink.snapshot()) != 0 {
		t.Fatal("apply echoed into capture")
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 2, Name: "local", Email: "local"}) })
	groups := sink.snapshot()
	if len(groups) != 1 || groups[0].TransactionID != 2 || len(groups[0].Changes) != 1 {
		t.Fatalf("local capture=%+v", groups)
	}
	requireReplicationID(t, db, 2)

	releaseTestGroup(upstreamSink.snapshot()[0])
	releaseTestGroup(groups[0])
	replicationAssertClean(t, db)
}

func TestRowMigrationPolicy_without_replication(t *testing.T) {
	for _, writes := range []LocalWriteMode{AllowLocalWrites, RejectLocalWrites} {
		t.Run(writes.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			old := new(Schema)
			oldTable := DefineTable(old, "users", testTableModel[User](), func(b *TableBuilder[ID]) {}).TableDef()
			db, err := Open(path, old, Options{IsTesting: true})
			if err != nil {
				t.Fatal(err)
			}
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "original"}) })
			var original ReplicationValue
			db.Read(TxMetadata{}, func(tx *Tx) {
				original = rowReplicationValue(oldTable.dataBucketIn(tx).Get(oldTable.EncodeKey(ID(1))))
				original.Data = bytes.Clone(original.Data)
			})
			db.Close()
			sc := new(Schema)
			var idx *IndexDef
			tbl := DefineTable(sc, "users", testIndexAccess[User](indexName), func(b *TableBuilder[ID]) {
				b.SetSchemaVersion(2)
				idx = b.AddIndex[string]("name", nil).IndexDef()

				b.SetIndexer(func(_ ID, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.Get(indexName)) })
				b.AddMigrator(2, func(*Tx, ID, *kvo.Editor) { t.Error("row migrator invoked despite skip policy") })
			}).TableDef()
			db, err = Open(path, sc, Options{IsTesting: true, LocalWrites: LocalWritePolicy{Mode: writes}, RowMigrations: SkipRowMigrations})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Get[User](tx, ID(1)).Name != "original" || Lookup[User](tx, idx, "original") == nil {
					t.Fatal("decode/backfill changed source row")
				}
			})
			db.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(tbl, nil) })
			db.Read(TxMetadata{}, func(tx *Tx) {
				v := rowReplicationValue(tbl.dataBucketIn(tx).Get(tbl.EncodeKey(ID(1))))
				if v.SchemaVer != original.SchemaVer || v.ModCount != original.ModCount || v.Flags != original.Flags || !bytes.Equal(v.Data, original.Data) {
					t.Fatal("index-only rebuild changed source envelope")
				}
			})
		})
	}
}

func TestReplicationApply_never_migrates_with_permissive_local_policy(t *testing.T) {
	sink := &replicationTestSink{}
	old := new(Schema)
	DefineTable(old, "users", testTableModel[User](), func(b *TableBuilder[ID]) {}).TableDef()
	source := openReplicationTest(t, old, replicationCaptureOptions(sink))
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "original"}) })
	sc := new(Schema)
	var idx *IndexDef
	tbl := DefineTable(sc, "users", testIndexAccess[User](indexName), func(b *TableBuilder[ID]) {
		b.SetSchemaVersion(2)
		idx = b.AddIndex[string]("name", nil).IndexDef()

		b.SetIndexer(func(_ ID, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.Get(indexName)) })
		b.AddMigrator(2, func(*Tx, ID, *kvo.Editor) { t.Error("apply invoked local migrator") })
	}).TableDef()
	db := openReplicationTest(t, sc, replicationApplyOptions())
	g := sink.snapshot()[0]
	defer releaseTestGroup(g)
	changes := append([]ReplicationChange(nil), g.Changes...)
	changes[0].Object.Table = tbl
	mapped := &ReplicationGroup{TransactionID: g.TransactionID, SchemaRev: g.SchemaRev, Changes: changes}
	if err := db.ApplyReplication(mapped); err != nil {
		t.Fatal(err)
	}
	if !db.AllowsRowMigrations() {
		t.Fatal("apply changed database-wide migration policy")
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		v := rowReplicationValue(tbl.dataBucketIn(tx).Get(tbl.EncodeKey(ID(1))))
		if v.SchemaVer != 1 || !bytes.Equal(v.Data, g.Changes[0].Value.Data) {
			t.Fatal("apply changed payload/version")
		}
		if !LookupExists(tx, idx, "original") {
			t.Fatal("local index not built")
		}
	})
}

func TestReplicationCapabilities_validate_and_copy_configuration(t *testing.T) {
	for _, o := range []Options{
		{LocalWrites: LocalWritePolicy{Mode: 99}},
		{LocalWrites: LocalWritePolicy{Include: func(ReplicationObject) bool { return true }}},
		{RowMigrations: RowMigrationPolicy(99)},
		{Replication: &ReplicationOptions{Capture: &ReplicationCaptureOptions{}}},
	} {
		o.IsTesting = true
		db, err := Open(filepath.Join(t.TempDir(), "db"), basicSchema, o)
		if err == nil {
			db.Close()
			t.Fatal("invalid configuration accepted")
		}
	}
	db, err := Open(InMemory, basicSchema, Options{Replication: &ReplicationOptions{}})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	sink := &replicationTestSink{}
	o := replicationCaptureOptions(sink)
	o.Apply = replicationApplyOptions().Apply
	db = openReplicationTest(t, basicSchema, o)
	o.Capture.SchemaRev = 99
	o.Capture.Sink = &replicationTestSink{fail: ErrReplicationOverload}
	o.Apply.SchemaRev = 1
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
	g := sink.snapshot()[0]
	if g.SchemaRev != 3 {
		t.Fatal("capture config was borrowed")
	}
	releaseTestGroup(g)
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 7, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationCapabilities_restart_preserves_checkpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	sink := &replicationTestSink{}
	o := replicationCaptureOptions(sink)
	o.Apply = replicationApplyOptions().Apply
	db, err := Open(path, basicSchema, Options{IsTesting: true, Replication: o})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 8, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
	requireReplicationID(t, db, 9)
	db.Close()
	db, err = Open(path, basicSchema, Options{IsTesting: true, Replication: o})
	if err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 9)
	db.Close()

}

func TestReplicationCapabilities_index_maintenance_does_not_echo(t *testing.T) {
	sink := &replicationTestSink{}
	o := replicationCaptureOptions(sink)
	db := openReplicationTest(t, basicSchema, o, Options{RowMigrations: SkipRowMigrations})
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "original", Email: "original"}) })
	db.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(usersTable, nil); Put(tx, &User{ID: 2, Name: "next", Email: "next"}) })
	groups := sink.snapshot()
	if len(groups) != 2 || len(groups[1].Changes) != 1 || groups[1].Changes[0].Op != ReplicationPut {
		t.Fatal("maintenance captured or capture scope not restored")
	}
	for _, g := range groups {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, db)
}
