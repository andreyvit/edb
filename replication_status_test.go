package edb

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func requireReplicationID(t *testing.T, db *DB, want uint64) {
	t.Helper()
	id, err := db.ReplicationStatus()
	if err != nil || id != want {
		t.Fatalf("committed=%d, want %d: %v", id, want, err)
	}
}

func TestReplicationStatus_capture_and_apply_share_one_checkpoint(t *testing.T) {
	upstreamSink := &replicationTestSink{}
	upstream := openReplicationTest(t, basicSchema, replicationCaptureOptions(upstreamSink))
	upstream.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "first", Email: "first"}) })
	upstream.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 2, Name: "second", Email: "second"}) })
	requireReplicationID(t, upstream, 2)
	sink := &replicationTestSink{}
	opt := replicationCaptureOptions(sink)
	opt.Apply = replicationApplyOptions().Apply
	db := openReplicationTest(t, basicSchema, opt)
	requireReplicationID(t, db, 0)
	for i, g := range upstreamSink.snapshot() {
		if err := db.ApplyReplication(g); err != nil {
			t.Fatal(err)
		}
		requireReplicationID(t, db, uint64(i+1))
	}
	if len(sink.snapshot()) != 0 {
		t.Fatal("apply generated local capture")
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 3, Name: "next writer", Email: "third"}) })
	groups := sink.snapshot()
	if len(groups) != 1 || groups[0].TransactionID != 3 {
		t.Fatalf("capture did not continue the same sequence: %+v", groups)
	}
	requireReplicationID(t, db, 3)
	if err := db.ApplyReplication(upstreamSink.snapshot()[1]); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 3)
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 9, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 9)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 4, Name: "after filtered gap", Email: "fourth"}) })
	groups = sink.snapshot()
	if len(groups) != 2 || groups[1].TransactionID != 10 {
		t.Fatal("capture ignored committed filtered transaction")
	}
	requireReplicationID(t, db, 10)
	for _, g := range upstreamSink.snapshot() {
		releaseTestGroup(g)
	}
	for _, g := range groups {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, db)
}

func TestReplicationStatus_snapshot_preserves_checkpoint_across_capture_and_apply(t *testing.T) {
	sink := &replicationTestSink{}
	source := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "snapshot", Email: "before"}) })
	snapshot := filepath.Join(t.TempDir(), "snapshot.db")
	source.Read(TxMetadata{}, func(tx *Tx) {
		if err := tx.btx.CopyFile(snapshot, 0600); err != nil {
			t.Fatal(err)
		}
	})
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "after snapshot", Email: "after"}) })
	db, err := Open(snapshot, basicSchema, Options{IsTesting: true, LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations, Replication: replicationApplyOptions()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	requireReplicationID(t, db, 1)
	// Dedup must use the checkpoint copied from normal source commits.
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 1, SchemaRev: 3, Changes: []ReplicationChange{{Op: ReplicationOp(99)}}}); err != nil {
		t.Fatal(err)
	}
	groups := sink.snapshot()
	if err := db.ApplyReplication(groups[1]); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 2)
	db.Read(TxMetadata{}, func(tx *Tx) {
		if u := Get[User](tx, ID(1)); u == nil || u.Name != "after snapshot" || Lookup[User](tx, usersByEmail, "after") == nil {
			t.Fatal("snapshot continuation failed")
		}
	})
	db.Close()
	reopened, err := Open(snapshot, basicSchema, Options{IsTesting: true, LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	requireReplicationID(t, reopened, 2)
	for _, g := range groups {
		releaseTestGroup(g)
	}
}

func TestReplicationStatus_failed_apply_does_not_advance_checkpoint(t *testing.T) {
	sink := &replicationTestSink{}
	source := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "committed", Email: "first"}) })
	db := openReplicationTest(t, basicSchema, replicationApplyOptions())
	g := sink.snapshot()[0]
	defer releaseTestGroup(g)
	if err := db.ApplyReplication(g); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 1)
	changes := slices.Clone(g.Changes)
	changes[0].Key = usersTable.EncodeKey(ID(2))
	changes = append(changes, changes[0])
	changes[1].Ordinal = 1
	changes[1].Key = usersTable.EncodeKey(ID(3))
	changes[1].Value.Data = []byte{0xc1}
	invalid := &ReplicationGroup{TransactionID: 10, SchemaRev: 3, Changes: changes}
	if err := db.ApplyReplication(invalid); err == nil {
		t.Fatal("invalid second row accepted")
	}
	requireReplicationID(t, db, 1)
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(2)) != nil || Get[User](tx, ID(3)) != nil || Lookup[User](tx, usersByEmail, "first").ID != 1 {
			t.Fatal("failed apply committed partial rows/indexes")
		}
	})
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 10, SchemaRev: 4}); !errors.Is(err, ErrReplicationSchema) {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 1)
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 10, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 10)
}

func TestReplicationStatus_consumer_policy_and_disabled_reads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	open := func(opt Options) *DB {
		t.Helper()
		opt.IsTesting = true
		db, err := Open(path, basicSchema, opt)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(db.Close)
		return db
	}
	apply := replicationApplyOptions()
	db := open(Options{Replication: apply})
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 0, SchemaRev: 3}); err == nil {
		t.Fatal("zero transaction ID accepted")
	}
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 105, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 105)
	db.Close()
	db = open(Options{})
	requireReplicationID(t, db, 105)
	db.Close()
	db = open(Options{Replication: apply})
	requireReplicationID(t, db, 105)
	db.Close()
	db = open(Options{})
	requireReplicationID(t, db, 105)
}
