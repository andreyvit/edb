package edb

import (
	"errors"
	"testing"
	"unsafe"
)

func TestReplicationStage_borrows_live_values_and_publishes_only_after_commit(t *testing.T) {
	sc := new(Schema)
	raw := DefineRawTable(sc, "raw", nil)
	sink := &replicationTestSink{}
	opt := replicationCaptureOptions(sink)
	var active *Tx
	var inputKey, inputData, inputShard *byte
	shard := []byte("tenant")
	opt.Capture.Shard = func(_ ReplicationObject, key []byte, v ReplicationValue) ([]byte, error) {
		inputKey, inputData, inputShard = unsafe.SliceData(key), unsafe.SliceData(v.Data), unsafe.SliceData(shard)
		return shard, nil
	}
	stages, publications := 0, 0
	sink.onStage = func(c ReplicationChange) {
		stages++
		if active == nil || active.btx.DB() == nil {
			t.Fatal("staged after transaction closed")
		}
		if unsafe.SliceData(c.Key) != inputKey || unsafe.SliceData(c.Value.Data) != inputData || unsafe.SliceData(c.Shard) != inputShard {
			t.Fatal("EDB copied borrowed staging input")
		}
		if publications != 0 {
			t.Fatal("publication preceded staging completion")
		}
	}
	sink.onPublish = func(id, rev uint64) {
		publications++
		if active.btx.DB() != nil || id != 1 || rev != 3 {
			t.Fatal("published before physical commit or with wrong metadata")
		}
	}
	db := openReplicationTest(t, sc, opt)
	key, value := []byte("key"), []byte("value")
	db.Write(TxMetadata{}, func(tx *Tx) {
		active = tx
		tx.KVPutRaw(raw, key, value)
		if stages != 1 || publications != 0 {
			t.Fatal("staging was not synchronous")
		}
	})
	for i := range key {
		key[i] = 'X'
	}
	for i := range value {
		value[i] = 'X'
	}
	for i := range shard {
		shard[i] = 'X'
	}
	g := sink.snapshot()[0]
	defer releaseTestGroup(g)
	if publications != 1 || string(g.Changes[0].Key) != "key" || string(g.Changes[0].Value.Data) != "value" || string(g.Changes[0].Shard) != "tenant" {
		t.Fatal("consumer did not own the staged bytes")
	}
}

func TestReplicationStage_failure_or_panic_cancels_the_whole_attempt(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			sink := &replicationTestSink{}
			db := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
			failure := errors.New("consumer staging failed")
			if mode == "error" {
				sink.stageFail = failure
			} else {
				sink.onStage = func(ReplicationChange) { panic(failure) }
			}
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
				tx.CommitDespiteError()
				Put(tx, &User{ID: 1})
				return nil
			})
			if err == nil || len(sink.snapshot()) != 0 || sink.cancelled != 1 || sink.reserved != 0 {
				t.Fatal("consumer failure was committed or not cancelled")
			}
			requireReplicationID(t, db, 0)
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Get[User](tx, ID(1)) != nil {
					t.Fatal("staging failure persisted the primary write")
				}
			})
			replicationAssertClean(t, db)
		})
	}
}
