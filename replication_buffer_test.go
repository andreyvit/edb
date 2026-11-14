package edb

import (
	"bytes"
	"errors"
	"testing"
)

func TestReplicationBucketCache_shared_attempt_and_recreation(t *testing.T) {
	db := openReplicationTest(t, basicSchema, replicationCaptureOptions(&replicationTestSink{}))
	db.Write(TxMetadata{}, func(tx *Tx) {
		first := tx.stx.Bucket(usersTable.name, dataBucketName)
		if first != tx.stx.Bucket(usersTable.name, dataBucketName) {
			t.Fatal("bucket wrapper was not reused")
		}
		owner := db.newTx(TxMetadata{}, &boltStorageTx{btx: tx.btx}, true, nil, nil)
		defer func() { owner.Close(); owner.releaseReplication(); owner.release() }()
		if owner.stx.Bucket(usersTable.name, dataBucketName) != first {
			t.Fatal("physical transaction wrappers did not share cache")
		}
		created := must(tx.stx.CreateBucket(usersTable.name, "cache_test"))
		ensure(created.Put([]byte("key"), []byte("old")))
		if created != owner.stx.Bucket(usersTable.name, "cache_test") {
			t.Fatal("created bucket bypassed cache")
		}
		ensure(owner.stx.DeleteBucket(usersTable.name, "cache_test"))
		if tx.stx.Bucket(usersTable.name, "cache_test") != nil {
			t.Fatal("deleted bucket remained cached")
		}
		recreated := must(tx.stx.CreateBucket(usersTable.name, "cache_test"))
		if recreated == created || recreated.Get([]byte("key")) != nil {
			t.Fatal("recreated bucket reused a stale handle")
		}
		ensure(recreated.Put([]byte("key"), []byte("new")))
		if got := owner.stx.Bucket(usersTable.name, "cache_test").Get([]byte("key")); !bytes.Equal(got, []byte("new")) {
			t.Fatal("shared cache did not observe recreation")
		}
	})
}

func TestReplicationBucketCache_poison_survives_wrapper_owner_release(t *testing.T) {
	db := openReplicationTest(t, basicSchema, replicationCaptureOptions(&replicationTestSink{}))
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
		owner := db.newTx(TxMetadata{}, &boltStorageTx{btx: tx.btx}, true, nil, nil)
		bucket := owner.stx.Bucket(usersTable.name, dataBucketName)
		owner.Close()
		owner.releaseReplication()
		owner.release()
		tx.CommitDespiteError()
		if err := bucket.Put(nil, []byte("invalid")); err == nil {
			t.Fatal("invalid storage write accepted")
		}
		return nil
	})
	if err == nil {
		t.Fatal("cached adapter lost physical-attempt failure state")
	}
	requireReplicationID(t, db, 0)
	replicationAssertClean(t, db)
}

func TestReplicationBuffers_live_groups_do_not_alias_recycled_storage(t *testing.T) {
	sink := &replicationTestSink{}
	db := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	write := func(prefix string) {
		db.Write(TxMetadata{}, func(tx *Tx) {
			for i := range 100 {
				Put(tx, &User{ID: ID(i + 1), Name: prefix, Email: prefix})
			}
		})
	}
	write("first")
	write("second")
	groups := sink.snapshot()
	secondKey := bytes.Clone(groups[1].Changes[0].Key)
	secondData := bytes.Clone(groups[1].Changes[0].Value.Data)
	secondShard := bytes.Clone(groups[1].Changes[0].Shard)
	releaseTestGroup(groups[0])
	write("third")
	groups = sink.snapshot()
	if !bytes.Equal(groups[1].Changes[0].Key, secondKey) || !bytes.Equal(groups[1].Changes[0].Value.Data, secondData) || !bytes.Equal(groups[1].Changes[0].Shard, secondShard) {
		t.Fatal("recycled storage changed an unreleased group")
	}
	if len(groups[2].Changes) != 100 || groups[2].TransactionID != 3 {
		t.Fatal("recycled change vector retained old entries")
	}
	for _, g := range groups {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, db)
}

func TestReplicationConsumer_staging_limit_rolls_back_and_reclaims_memory(t *testing.T) {
	sc := new(Schema)
	raw := DefineRawTable(sc, "raw", nil)
	sink := &replicationTestSink{maxGroupBytes: 1024}
	db := openReplicationTest(t, sc, replicationCaptureOptions(sink))
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
		tx.CommitDespiteError()
		tx.KVPutRaw(raw, []byte("first"), []byte("small"))
		tx.KVPutRaw(raw, []byte("second"), bytes.Repeat([]byte("x"), 200<<10))
		return nil
	})
	if !errors.Is(err, ErrReplicationGroupSize) {
		t.Fatal(err)
	}
	if len(sink.snapshot()) != 0 || sink.reserved != 0 || sink.cancelled != 1 {
		t.Fatal("consumer staging was not cancelled and reclaimed")
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		if tx.KVGetRaw(raw, []byte("first")) != nil || tx.KVGetRaw(raw, []byte("second")) != nil {
			t.Fatal("staging failure committed data")
		}
	})
	requireReplicationID(t, db, 0)
	replicationAssertClean(t, db)
}
