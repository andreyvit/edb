package edb

import (
	"path/filepath"
	"slices"
	"testing"

	"go.etcd.io/bbolt"
)

func TestReplicationApply_hash_lookup_checks_identity_and_kind(t *testing.T) {
	makeSchema := func() (*Schema, *TableDef, *KVTable, *KVMap, *SKey[string]) {
		sc := new(Schema)
		tbl := DefineTable(sc, "users", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
		raw := DefineRawTable(sc, "raw", nil)
		mp := AddKVMap(sc, "settings")
		return sc, tbl, raw, mp, AddSingletonKey[string](mp, "key")
	}
	sc, tbl, raw, mp, key := makeSchema()
	_, foreignTable, foreignRaw, foreignMap, _ := makeSchema()
	if tbl.NameHash() != foreignTable.NameHash() || raw.NameHash() != foreignRaw.NameHash() || mp.NameHash() != foreignMap.NameHash() {
		t.Fatal("foreign fixture needs matching hashes")
	}
	sink := &replicationTestSink{}
	source := openReplicationTest(t, sc, replicationCaptureOptions(sink))
	source.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "seed"})
		tx.KVPutRaw(raw, []byte("key"), []byte("raw"))
		SPutRaw(tx, key, []byte("map"))
	})
	replica := openReplicationTest(t, sc, replicationApplyOptions())
	g := sink.snapshot()[0]
	defer releaseTestGroup(g)
	cases := []struct {
		name   string
		change func([]ReplicationChange)
	}{
		{"foreign-row", func(c []ReplicationChange) { c[0].Object.Table = foreignTable }},
		{"foreign-kv", func(c []ReplicationChange) { c[1].Object.KVTable = foreignRaw }},
		{"foreign-map", func(c []ReplicationChange) { c[2].Object.Map = foreignMap }},
		{"ambiguous", func(c []ReplicationChange) { c[0].Object.Map = mp }},
		{"empty", func(c []ReplicationChange) { c[0].Object = ReplicationObject{} }},
		{"operation", func(c []ReplicationChange) { c[0].Op = ReplicationOp(99) }},
		{"key", func(c []ReplicationChange) { c[0].Key = nil }},
		{"value", func(c []ReplicationChange) { c[0].Value.Present = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changes := slices.Clone(g.Changes)
			tc.change(changes)
			if err := replica.ApplyReplication(&ReplicationGroup{TransactionID: g.TransactionID, SchemaRev: g.SchemaRev, Changes: changes}); err == nil {
				t.Fatal("invalid mapping accepted")
			}
			requireReplicationID(t, replica, 0)
			replica.Read(TxMetadata{}, func(tx *Tx) {
				if CountAll(tx, tbl) != 0 || tx.KVGetRaw(raw, []byte("key")) != nil || SGetRaw(tx, key) != nil {
					t.Fatal("invalid group partially applied")
				}
			})
		})
	}
	if err := replica.ApplyReplication(g); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, replica, 1)
}

func TestReplicationApply_empty_options_enable_default_revision(t *testing.T) {
	db := openReplicationTest(t, basicSchema, &ReplicationOptions{Apply: &ReplicationApplyOptions{}})
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 1}); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 1)
}

func TestReplicationOpen_preserves_disabled_panic_and_enabled_error(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			sc := new(Schema)
			tbl := DefineTable(sc, "users", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
			bdb, err := bbolt.Open(filepath.Join(t.TempDir(), "db"), 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { bdb.Close() })
			if err := bdb.Update(func(tx *bbolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte(tbl.name))
				if err != nil {
					return err
				}
				return b.Put(tableStateKey, []byte{0xc1})
			}); err != nil {
				t.Fatal(err)
			}
			opt := Options{IsTesting: true}
			if enabled {
				opt.Replication = replicationCaptureOptions(&replicationTestSink{})
			}
			var db *DB
			var openErr error
			var caught any
			func() {
				defer func() { caught = recover() }()
				db, openErr = openWithStorage(newBoltStorage(bdb), sc, opt)
			}()
			if db != nil {
				db.Close()
				t.Fatal("invalid schema state accepted")
			}
			if enabled {
				if caught != nil || openErr == nil {
					t.Fatalf("panic=%v error=%v", caught, openErr)
				}
			} else {
				if caught == nil || openErr != nil {
					t.Fatalf("panic=%v error=%v", caught, openErr)
				}
				if _, wrapped := caught.(panicked); wrapped {
					t.Fatal("disabled open rethrew a panic wrapper")
				}
			}
		})
	}
}
