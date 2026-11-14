package edb

import "testing"

func TestSchemaNameHash_typed_handles_share_definition_hash(t *testing.T) {
	sc := new(Schema)
	table := DefineTable(sc, "typed", testTableModel[User](), func(*TableBuilder[ID]) {})
	if table.NameHash() != sc.NameHash("TYPED") || sc.TableByNameHash(table.NameHash()) != table.TableDef() {
		t.Fatal("typed table hash differs from definition")
	}
	if rows := table.Rows[User](); rows.NameHash() != table.NameHash() {
		t.Fatal("native row handle hash differs")
	}
}

func TestSchemaNameHash_registration_composition_and_lookup(t *testing.T) {
	source := new(Schema)
	table := DefineTable(source, "Hello", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
	raw := DefineRawTable(source, "A", nil)
	mp := AddKVMap(source, "Settings")
	if table.NameHash() != 0x26c7827d889f6da3 || raw.NameHash() != 0xd24ec4f1a98c6e5b {
		t.Fatalf("hash contract changed: %016x / %016x", table.NameHash(), raw.NameHash())
	}
	combined := new(Schema)
	DefineTable(combined, "posts", testTableModel[Post](), func(*TableBuilder[string]) {}).TableDef()
	combined.Include(source)
	for _, sc := range []*Schema{source, combined} {
		if sc.NameHash("HELLO") != table.NameHash() || sc.NameHash("a") != raw.NameHash() || sc.NameHash("SETTINGS") != mp.NameHash() {
			t.Fatal("schema name lookup differs from cached hash")
		}
		if sc.TableByNameHash(table.NameHash()) != table || sc.KVTableByNameHash(raw.NameHash()) != raw || sc.KVMapByNameHash(mp.NameHash()) != mp {
			t.Fatal("hash lookup lost descriptor identity")
		}
		if sc.TableByNameHash(raw.NameHash()) != nil || sc.KVTableByNameHash(mp.NameHash()) != nil || sc.KVMapByNameHash(table.NameHash()) != nil {
			t.Fatal("hash lookup accepted wrong object kind")
		}
		if sc.TableByNameHash(0) != nil || sc.KVTableByNameHash(0) != nil || sc.KVMapByNameHash(0) != nil {
			t.Fatal("unknown hash resolved")
		}
		assertRegistrationPanic(t, "no schema object", func() { sc.NameHash("missing") })
	}
	other := new(Schema)
	otherMap := AddKVMap(other, "settings")
	otherRaw := DefineRawTable(other, "a", nil)
	otherTable := DefineTable(other, "hello", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
	if otherTable.NameHash() != table.NameHash() || otherRaw.NameHash() != raw.NameHash() || otherMap.NameHash() != mp.NameHash() {
		t.Fatal("case/order/schema instance changed hashes")
	}
	for _, obj := range []ReplicationObject{{Table: table}, {KVTable: raw}, {Map: mp}} {
		if obj.NameHash() != source.NameHash(objName(obj)) {
			t.Fatal("replication object hash differs")
		}
	}
	assertRegistrationPanic(t, "invalid replication object", func() { (ReplicationObject{}).NameHash() })
	assertRegistrationPanic(t, "invalid replication object", func() { (ReplicationObject{Table: table, Map: mp}).NameHash() })
}

func objName(o ReplicationObject) string {
	switch o.Kind() {
	case ReplicationRowTable:
		return o.Table.Name()
	case ReplicationKVTable:
		return o.KVTable.Name()
	default:
		return o.Map.Name()
	}
}

func TestSchemaNameHash_collision_panics_without_partial_registration(t *testing.T) {
	kinds := []string{"table", "kv", "map"}
	for _, existingKind := range kinds {
		for _, newKind := range kinds {
			for _, include := range []bool{false, true} {
				mode := "direct"
				if include {
					mode = "include"
				}
				t.Run(existingKind+"/"+newKind+"/"+mode, func(t *testing.T) {
					sc := new(Schema)
					var existing schemaNamedObject
					switch existingKind {
					case "table":
						existing = DefineTable(sc, "existing", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
					case "kv":
						existing = DefineRawTable(sc, "existing", nil)
					case "map":
						existing = AddKVMap(sc, "existing")
					}
					peer := new(Schema)
					prefix := DefineTable(peer, "prefix", testTableModel[Post](), func(*TableBuilder[string]) {}).TableDef()
					var incoming schemaNamedObject
					switch newKind {
					case "table":
						incoming = DefineTable(peer, "incoming", testTableModel[Widget](), func(*TableBuilder[AB]) {}).TableDef()
					case "kv":
						incoming = DefineRawTable(peer, "incoming", nil)
					case "map":
						incoming = AddKVMap(peer, "incoming")
					}
					// Force the collision without depending on a brute-force xxHash64 collision.
					switch obj := incoming.(type) {
					case *TableDef:
						obj.nameHash = existing.NameHash()
					case *KVTable:
						obj.nameHash = existing.NameHash()
					case *KVMap:
						obj.nameHash = existing.NameHash()
					}
					before := [3]int{len(sc.tables), len(sc.kvtables), len(sc.maps)}
					assertRegistrationPanic(t, "name hash collision", func() {
						if include {
							sc.Include(peer)
							return
						}
						switch obj := incoming.(type) {
						case *TableDef:
							sc.addTable(obj)
						case *KVTable:
							sc.addKVTable(obj)
						case *KVMap:
							sc.addMap(obj)
						}
					})
					if counts := [3]int{len(sc.tables), len(sc.kvtables), len(sc.maps)}; counts != before {
						t.Fatal("collision changed declarations", counts, before)
					}
					if len(sc.objectsByNameHash) != 1 || sc.objectsByNameHash[existing.NameHash()] != existing {
						t.Fatal("collision changed hash registry")
					}
					if sc.TableNamed("prefix") != nil || sc.TableByNameHash(prefix.NameHash()) != nil {
						t.Fatal("failed include leaked hash registrations")
					}
					if peer.TableByNameHash(prefix.NameHash()) != prefix {
						t.Fatal("include mutated peer registry")
					}
					assertRegistrationPanic(t, "no schema object", func() { sc.NameHash("incoming") })
					after := AddKVMap(sc, "after")
					if sc.KVMapByNameHash(after.NameHash()) != after {
						t.Fatal("rejected registration poisoned later declarations")
					}
				})
			}
		}
	}
}

func TestSchemaNameHash_resolves_replication_objects_across_schemas(t *testing.T) {
	sourceSchema := new(Schema)
	DefineTable(sourceSchema, "Users", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
	sourceRaw := DefineRawTable(sourceSchema, "Raw", nil)
	sourceMap := AddKVMap(sourceSchema, "Globals")
	sourceKey := AddSingletonKey[string](sourceMap, "version")
	sink := &replicationTestSink{}
	source := openReplicationTest(t, sourceSchema, replicationCaptureOptions(sink))
	source.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "replicated"})
		tx.KVPutRaw(sourceRaw, []byte("key"), []byte("raw bytes"))
		SPutRaw(tx, sourceKey, []byte("singleton bytes"))
	})
	peer := new(Schema)
	destMap := AddKVMap(peer, "globals")
	destKey := AddSingletonKey[string](destMap, "version")
	destRaw := DefineRawTable(peer, "raw", nil)
	DefineTable(peer, "users", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
	destSchema := new(Schema)
	destSchema.Include(peer)
	replica := openReplicationTest(t, destSchema, replicationApplyOptions())
	g := sink.snapshot()[0]
	defer releaseTestGroup(g)
	mapped := &ReplicationGroup{TransactionID: g.TransactionID, SchemaRev: g.SchemaRev, Changes: append([]ReplicationChange(nil), g.Changes...)}
	for i := range mapped.Changes {
		src := mapped.Changes[i].Object
		hash := src.NameHash()
		dst := ReplicationObject{}
		switch src.Kind() {
		case ReplicationRowTable:
			dst.Table = destSchema.TableByNameHash(hash)
		case ReplicationKVTable:
			dst.KVTable = destSchema.KVTableByNameHash(hash)
		case ReplicationSingletonMap:
			dst.Map = destSchema.KVMapByNameHash(hash)
		}
		if dst.Kind() != src.Kind() || dst.NameHash() != hash {
			t.Fatal("wire hash did not resolve to destination object")
		}
		mapped.Changes[i].Object = dst
	}
	if err := replica.ApplyReplication(mapped); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if u := Get[User](tx, ID(1)); u == nil || u.Name != "replicated" {
			t.Fatal("row hash mapping failed")
		}
		if string(tx.KVGetRaw(destRaw, []byte("key"))) != "raw bytes" || string(SGetRaw(tx, destKey)) != "singleton bytes" {
			t.Fatal("KV/map hash mapping failed")
		}
	})
	requireReplicationID(t, replica, 1)
}
