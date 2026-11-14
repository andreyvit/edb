package edb

import (
	"math"
	"testing"

	"github.com/andreyvit/edb/kvo/codec/row2"
)

func TestReplicationCheckpoint_KVO_atomicity_and_full_uint64(t *testing.T) {
	sink := &replicationTestSink{}
	opt := replicationCaptureOptions(sink)
	opt.Apply = replicationApplyOptions().Apply
	db := openReplicationTest(t, basicSchema, opt)
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: math.MaxUint64 - 1, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, math.MaxUint64-1)
	db.Read(TxMetadata{}, func(tx *Tx) {
		raw := tx.btx.Bucket([]byte(replicationBucket)).Get(replicationCommittedKey)
		view, err := row2.TryOpen(raw)
		if err != nil {
			t.Fatal("checkpoint is not KVO", err)
		}
		if view.Kind() != row2.Object || view.Get(1, row2.Word).AsUint() != math.MaxUint64-1 {
			t.Fatal("wrong KVO checkpoint")
		}
	})
	tx := db.BeginUpdate(TxMetadata{})
	defer tx.Close()
	Put(tx, &User{ID: 1, Name: "aborted"})
	requireReplicationID(t, db, math.MaxUint64-1)
	tx.Close()
	requireReplicationID(t, db, math.MaxUint64-1)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "committed"}) })
	requireReplicationID(t, db, math.MaxUint64)
	db.Read(TxMetadata{}, func(tx *Tx) {
		view := row2.Open(tx.btx.Bucket([]byte(replicationBucket)).Get(replicationCommittedKey))
		if view.Get(1, row2.Word).AsUint() != math.MaxUint64 {
			t.Fatal("maximum uint64 was truncated")
		}
	})
	if err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { tx.CommitDespiteError(); Put(tx, &User{ID: 2}); return nil }); err == nil {
		t.Fatal("sequence overflow committed")
	}
	requireReplicationID(t, db, math.MaxUint64)
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(2)) != nil {
			t.Fatal("overflow persisted data")
		}
	})
	groups := sink.snapshot()
	if len(groups) != 1 || groups[0].TransactionID != math.MaxUint64 {
		t.Fatal("wrong committed capture")
	}
	releaseTestGroup(groups[0])
	replicationAssertClean(t, db)
}
