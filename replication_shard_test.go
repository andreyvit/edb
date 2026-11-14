package edb

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

type replicationShardRow struct {
	ID     uint64 `msgpack:"-"`
	Tenant string `msgpack:"t"`
	Value  string `msgpack:"v"`
}

func TestReplicationShard_stable_attribution_and_explicit_move(t *testing.T) {
	sc := new(Schema)
	tbl := DefineTable(sc, "rows", testTableModel[replicationShardRow](), func(*TableBuilder[uint64]) {}).TableDef()
	sink := &replicationTestSink{}
	opt := replicationCaptureOptions(sink)
	var scratch [64]byte
	opt.Capture.Shard = func(_ ReplicationObject, _ []byte, v ReplicationValue) ([]byte, error) {
		var row replicationShardRow
		if err := MsgPack.DecodeValue(v.Data, reflect.ValueOf(&row)); err != nil {
			return nil, err
		}
		n := copy(scratch[:], row.Tenant)
		return scratch[:n], nil
	}
	source := openReplicationTest(t, sc, opt)
	source.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &replicationShardRow{1, "A", "first"})
		Put(tx, &replicationShardRow{1, "A", "updated"})
		tx.DeleteByKey(tbl, uint64(1))
	})
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &replicationShardRow{2, "B", "second"}) })
	for i := range scratch {
		scratch[i] = 'X'
	}
	groups := sink.snapshot()
	if len(groups) != 2 || len(groups[0].Changes) != 3 {
		t.Fatalf("groups=%+v", groups)
	}
	for _, c := range groups[0].Changes {
		if string(c.Shard) != "A" {
			t.Fatal("put/delete shard was borrowed or lost")
		}
	}
	if string(groups[1].Changes[0].Shard) != "B" {
		t.Fatal("insert shard missing")
	}
	err := source.Tx(TxMetadata{}, true, func(tx *Tx) error {
		tx.CommitDespiteError()
		Put(tx, &replicationShardRow{3, "B", "must roll back"})
		Put(tx, &replicationShardRow{2, "C", "invalid overwrite"})
		return nil
	})
	if !errors.Is(err, ErrReplicationShardChange) {
		t.Fatalf("shard change: %v", err)
	}
	requireReplicationID(t, source, 2)
	source.Read(TxMetadata{}, func(tx *Tx) {
		if Get[replicationShardRow](tx, uint64(3)) != nil || Get[replicationShardRow](tx, uint64(2)).Tenant != "B" {
			t.Fatal("shard change committed partial group")
		}
	})
	if len(sink.snapshot()) != 2 {
		t.Fatal("rejected shard change published")
	}
	tx := source.BeginUpdate(TxMetadata{})
	_ = safelyCall(func(tx *Tx) error { Put(tx, &replicationShardRow{2, "C", "invalid manual overwrite"}); return nil }, tx)
	if err := tx.Commit(); !errors.Is(err, ErrReplicationShardChange) {
		t.Fatalf("manual shard change: %v", err)
	}
	tx.Close()
	source.Write(TxMetadata{}, func(tx *Tx) { tx.DeleteByKey(tbl, uint64(2)); Put(tx, &replicationShardRow{2, "C", "moved"}) })
	groups = sink.snapshot()
	move := groups[2]
	if len(move.Changes) != 2 || move.Changes[0].Op != ReplicationDelete || string(move.Changes[0].Shard) != "B" || move.Changes[1].Op != ReplicationPut || string(move.Changes[1].Shard) != "C" {
		t.Fatalf("move=%+v", move.Changes)
	}
	for _, shard := range []string{"B", "C"} {
		applyOpt := replicationApplyOptions()

		replica := openReplicationTest(t, sc, applyOpt)
		for _, g := range groups {
			filtered := &ReplicationGroup{TransactionID: g.TransactionID, SchemaRev: g.SchemaRev}
			for _, c := range g.Changes {
				if string(c.Shard) == shard {
					filtered.Changes = append(filtered.Changes, c)
				}
			}
			if err := replica.ApplyReplication(filtered); err != nil {
				t.Fatal(err)
			}
		}
		replica.Read(TxMetadata{}, func(tx *Tx) {
			row := Get[replicationShardRow](tx, uint64(2))
			if shard == "B" && row != nil || shard == "C" && (row == nil || row.Tenant != "C") {
				t.Fatalf("projection for %s: %+v", shard, row)
			}
		})
		requireReplicationID(t, replica, 3)
	}
	for _, g := range groups {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, source)
}

func TestReplicationShard_raw_deletes_and_global_nil_put(t *testing.T) {
	sc := new(Schema)
	raw := DefineRawTable(sc, "raw", nil)
	mp := AddKVMap(sc, "globals")
	key := AddSingletonKey[string](mp, "nil")
	sink := &replicationTestSink{}
	opt := replicationCaptureOptions(sink)
	unknown := errors.New("unknown shard")
	opt.Capture.Shard = func(obj ReplicationObject, key []byte, v ReplicationValue) ([]byte, error) {
		if obj.Map != nil {
			return nil, nil
		}
		data := v.Data
		if !v.Present {
			data = key
		}
		prefix, _, ok := bytes.Cut(data, []byte(":"))
		if !ok {
			return nil, unknown
		}
		return prefix, nil
	}
	db := openReplicationTest(t, sc, opt)
	db.Write(TxMetadata{}, func(tx *Tx) {
		tx.KVPutRaw(raw, []byte("key"), []byte("A:first"))
		tx.KVPutRaw(raw, []byte("key"), []byte("A:updated"))
		tx.KVPutRaw(raw, []byte("key"), nil)
		tx.KVPutRaw(raw, []byte("A:missing"), nil)
		SPutRaw(tx, key, nil)
	})
	g := sink.snapshot()[0]
	if len(g.Changes) != 5 {
		t.Fatal(len(g.Changes))
	}
	for _, c := range g.Changes[:4] {
		if string(c.Shard) != "A" {
			t.Fatal("raw put/delete shard missing")
		}
	}
	global := g.Changes[4]
	if len(global.Shard) != 0 || global.Op != ReplicationPut || !global.Value.Present {
		t.Fatal("global nil PUT lost explicit attribution")
	}
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { tx.CommitDespiteError(); tx.KVPutRaw(raw, []byte("unknown"), nil); return nil })
	if !errors.Is(err, unknown) {
		t.Fatalf("missing delete implicitly became global: %v", err)
	}
	requireReplicationID(t, db, 1)
	if len(sink.snapshot()) != 1 {
		t.Fatal("unknown shard published")
	}
	releaseTestGroup(g)
	replicationAssertClean(t, db)
}

func TestReplicationShard_global_membership_is_stable(t *testing.T) {
	for _, tenants := range [][2]string{{"", "A"}, {"A", ""}} {
		t.Run(tenants[0]+"-to-"+tenants[1], func(t *testing.T) {
			sc := new(Schema)
			DefineTable(sc, "rows", testTableModel[replicationShardRow](), func(*TableBuilder[uint64]) {}).TableDef()
			sink := &replicationTestSink{}
			opt := replicationCaptureOptions(sink)
			opt.Capture.Shard = func(_ ReplicationObject, _ []byte, v ReplicationValue) ([]byte, error) {
				var row replicationShardRow
				if err := MsgPack.DecodeValue(v.Data, reflect.ValueOf(&row)); err != nil {
					return nil, err
				}
				return []byte(row.Tenant), nil
			}
			db := openReplicationTest(t, sc, opt)
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &replicationShardRow{1, tenants[0], "first"}) })
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
				tx.CommitDespiteError()
				Put(tx, &replicationShardRow{1, tenants[1], "invalid"})
				return nil
			})
			if !errors.Is(err, ErrReplicationShardChange) {
				t.Fatal(err)
			}
			requireReplicationID(t, db, 1)
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Get[replicationShardRow](tx, uint64(1)).Tenant != tenants[0] {
					t.Fatal("global membership changed")
				}
			})
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			replicationAssertClean(t, db)
		})
	}
}
