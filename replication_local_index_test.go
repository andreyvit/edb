package edb

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"

	"github.com/andreyvit/edb/kvo/codec/fast1"
)

func TestReplicationApply_maintains_destination_row_indexes(t *testing.T) {
	makeSchema := func(local bool, fail *bool) (*Schema, *TableDef, *IndexDef, *IndexDef) {
		sc := new(Schema)
		var name *IndexDef
		var email *IndexDef
		tbl := DefineTable(sc, "users", testIndexAccess[User](indexName, indexEmail), func(b *TableBuilder[ID]) {
			if local {
				email = b.AddIndex[string]("email", nil, IndexOptDeprecatedUnique).IndexDef()

			}
			name = b.AddIndex[string]("name", nil, IndexOptDeprecatedUnique).IndexDef()

			b.SetIndexer(func(_ ID, r kvo.Record, ib *IndexBuilder) {
				n := r.Get(indexName)
				if local {
					if *fail && n == "reject" {
						panic("local index failure")
					}
					n = strings.ToUpper(n)
					ib.Add(email, r.Get(indexEmail))
				}
				ib.Add(name, n)
			})
		}).TableDef()
		return sc, tbl, name, email
	}
	fail := false
	sourceSchema, sourceTable, sourceName, _ := makeSchema(false, &fail)
	sink := &replicationTestSink{}
	source := openReplicationTest(t, sourceSchema, replicationCaptureOptions(sink))
	localSchema, localTable, localName, localEmail := makeSchema(true, &fail)
	echo := &replicationTestSink{}
	opt := replicationCaptureOptions(echo)
	opt.Apply = replicationApplyOptions().Apply
	replica := openReplicationTest(t, localSchema, opt, Options{LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations})
	apply := func(i int) error {
		g := sink.snapshot()[i]
		changes := append([]ReplicationChange(nil), g.Changes...)
		for j := range changes {
			changes[j].Object.Table = localTable
		}
		return replica.ApplyReplication(&ReplicationGroup{TransactionID: g.TransactionID, SchemaRev: g.SchemaRev, Changes: changes})
	}
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "alice", Email: "a"}) })
	if err := apply(0); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if Lookup[User](tx, localName, "ALICE") == nil || Lookup[User](tx, localEmail, "a") == nil || Lookup[User](tx, localName, "alice") != nil {
			t.Fatal("destination definitions were not used")
		}
		ts := tx.db.tableState(localTable)
		if ts.indexOrdinal(localName) == source.tableState(sourceTable).indexOrdinal(sourceName) {
			t.Fatal("test needs different local ordinals")
		}
		raw := localTable.dataBucketIn(tx).Get(localTable.EncodeKey(ID(1)))
		v := rowReplicationValue(raw)
		want := sink.snapshot()[0].Changes[0].Value
		if v.Flags != want.Flags || v.SchemaVer != want.SchemaVer || v.ModCount != want.ModCount || !bytes.Equal(v.Data, want.Data) {
			t.Fatal("index maintenance changed source payload/metadata")
		}
		var stored value
		ensure(stored.decode(raw, false))
		n := 0
		decodeIndexKeys(stored.Index, func(ord uint64, key []byte) {
			n++
			if ts.indexByOrdinal(ord) == nil {
				t.Fatal("source ordinal retained")
			}
		})
		if n != 2 {
			t.Fatalf("local index tail has %d entries", n)
		}
	})
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "bob", Email: "b"}) })
	if err := apply(1); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if Lookup[User](tx, localName, "ALICE") != nil || Lookup[User](tx, localEmail, "a") != nil || Lookup[User](tx, localName, "BOB") == nil || Lookup[User](tx, localEmail, "b") == nil {
			t.Fatal("replace left stale or missing local index entries")
		}
	})
	source.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(sourceTable, nil) })
	replica.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(localTable, nil) })
	if len(sink.snapshot()) != 2 || len(echo.snapshot()) != 0 {
		t.Fatal("index-only work was replicated")
	}
	requireReplicationID(t, replica, 2)
	source.Write(TxMetadata{}, func(tx *Tx) { tx.UnsafeDeleteByKeyRawSkippingIndex(sourceTable, sourceTable.EncodeKey(ID(1))) })
	if err := apply(2); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(1)) != nil || Lookup[User](tx, localName, "BOB") != nil || Lookup[User](tx, localEmail, "b") != nil {
			t.Fatal("delete failed to clean destination indexes")
		}
	})
	source.Read(TxMetadata{}, func(tx *Tx) {
		if tx.stx.Bucket(sourceTable.name, sourceName.buck).KeyCount() != 1 {
			t.Fatal("source unsafe-delete semantics changed")
		}
	})
	source.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &User{ID: 2, Name: "okay", Email: "okay"})
		Put(tx, &User{ID: 3, Name: "reject", Email: "reject"})
	})
	fail = true
	if err := apply(3); err == nil {
		t.Fatal("local index failure accepted")
	}
	requireReplicationID(t, replica, 3)
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(2)) != nil || Lookup[User](tx, localName, "OKAY") != nil || Lookup[User](tx, localEmail, "okay") != nil {
			t.Fatal("index failure committed partial group")
		}
	})
	fail = false
	if err := apply(3); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, replica, 4)
	if len(echo.snapshot()) != 0 {
		t.Fatal("local index maintenance echoed")
	}
	for _, g := range sink.snapshot() {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, source)
	replicationAssertClean(t, replica)
}

func TestReplicationApply_maintains_destination_raw_KV_indexes(t *testing.T) {
	sourceSchema := new(Schema)
	sourceTable := DefineRawTable(sourceSchema, "raw", nil)
	sink := &replicationTestSink{}
	source := openReplicationTest(t, sourceSchema, replicationCaptureOptions(sink))
	localSchema := new(Schema)
	var idx *KVIndex
	fail := false
	localTable := DefineRawTable(localSchema, "raw", func(b *KVTableBuilder) {
		idx = b.DefineIndex("local", nil, func(ik []byte) []byte { return ik[1:] }, func(b *KVIndexContentBuilder, key []byte, v fast1.ImmutableRecord) {
			n := v.Root().Get(0x42)
			if fail && n == 99 {
				panic("local KV index failure")
			}
			b.Add(append([]byte{byte(n)}, key...))
		})
	})
	replica := openReplicationTest(t, localSchema, replicationApplyOptions(), Options{LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations})
	apply := func(i int) error {
		g := sink.snapshot()[i]
		changes := append([]ReplicationChange(nil), g.Changes...)
		for j := range changes {
			changes[j].Object.KVTable = localTable
		}
		return replica.ApplyReplication(&ReplicationGroup{TransactionID: g.TransactionID, SchemaRev: g.SchemaRev, Changes: changes})
	}
	key := []byte{1, 2}
	source.Write(TxMetadata{}, func(tx *Tx) { tx.KVPutRaw(sourceTable, key, buildKV(0x42, 7).Bytes()) })
	if err := apply(0); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) { indexScan(t, tx, idx, RawRange{Prefix: []byte{7}}, key) })
	source.Write(TxMetadata{}, func(tx *Tx) {
		tx.KVPutRaw(sourceTable, key, buildKV(0x42, 9).Bytes())
		tx.KVPutRaw(sourceTable, key, buildKV(0x42, 99).Bytes())
	})
	fail = true
	if err := apply(1); err == nil {
		t.Fatal("local KV index failure accepted")
	}
	requireReplicationID(t, replica, 1)
	replica.Read(TxMetadata{}, func(tx *Tx) {
		indexScan(t, tx, idx, RawRange{Prefix: []byte{7}}, key)
		indexScan(t, tx, idx, RawRange{Prefix: []byte{9}})
	})
	fail = false
	if err := apply(1); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		indexScan(t, tx, idx, RawRange{Prefix: []byte{7}})
		indexScan(t, tx, idx, RawRange{Prefix: []byte{99}}, key)
	})
	source.Write(TxMetadata{}, func(tx *Tx) { tx.KVPutRaw(sourceTable, key, nil) })
	if err := apply(2); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		indexScan(t, tx, idx, RawRange{})
		if tx.KVGetRaw(localTable, key) != nil {
			t.Fatal("KV delete did not persist")
		}
	})
	requireReplicationID(t, replica, 3)
	for _, g := range sink.snapshot() {
		releaseTestGroup(g)
	}
}

func TestReplicationApply_maintains_encoded_record_indexes(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			ks := kvo.NewSchema()
			entity := kvo.NewEntityType(ks, "row", reflect.TypeFor[keylessRecordRow]())
			kvo.NewProp(ks, 1, "name", kvo.TMachineString, nil)
			model := kvo.NewModel(ks, entity, func(b *kvo.ModelBuilder) { b.Prop(ks.MustPropByCode(1)) })
			sc := new(Schema)
			var idx *IndexDef
			tbl := DefineTable(sc, "rows", model, func(b *TableBuilder[uint64]) {
				idx = b.AddIndex[string]("name", nil, IndexOptDeprecatedUnique).IndexDef()

				storageFormat := format
				if format == MessagePack {
					storageFormat = Compact2
				}
				b.SetRecordStorage(RecordStorage{Format: storageFormat, MessagePackInserts: format == MessagePack})
				b.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.GetValue(1).AsString()) })
			}).TableDef()
			sink := &replicationTestSink{}
			source := openReplicationTest(t, sc, replicationCaptureOptions(sink))
			replica := openReplicationTest(t, sc, replicationApplyOptions())
			source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &keylessRecordRow{ID: 1, Name: "first"}) })
			source.Write(TxMetadata{}, func(tx *Tx) {
				tx.EditRecord(tbl, uint64(1), func(e *kvo.Editor) { ensure(e.TrySetValue(1, values.String("next"))) })
			})
			source.Write(TxMetadata{}, func(tx *Tx) { tx.DeleteByKey(tbl, uint64(1)) })
			groups := sink.snapshot()
			if len(groups) != 3 {
				t.Fatalf("groups=%d", len(groups))
			}
			for i, g := range groups {
				if err := replica.ApplyReplication(g); err != nil {
					t.Fatal(err)
				}
				replica.Read(TxMetadata{}, func(tx *Tx) {
					switch i {
					case 0:
						if Lookup[keylessRecordRow](tx, idx, "first") == nil {
							t.Fatal("missing encoded-record index")
						}
					case 1:
						if Lookup[keylessRecordRow](tx, idx, "first") != nil || Lookup[keylessRecordRow](tx, idx, "next") == nil {
							t.Fatal("stale encoded-record index")
						}
						v := rowReplicationValue(tbl.dataBucketIn(tx).Get(tbl.EncodeKey(uint64(1))))
						want := g.Changes[0].Value
						if v.Flags != want.Flags || v.ModCount != want.ModCount || !bytes.Equal(v.Data, want.Data) {
							t.Fatal("local indexing rewrote record")
						}
					case 2:
						if Lookup[keylessRecordRow](tx, idx, "next") != nil {
							t.Fatal("encoded-record delete left index")
						}
					}
				})
			}
			for _, g := range groups {
				releaseTestGroup(g)
			}
		})
	}
}
