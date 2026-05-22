package edb

import (
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
)

func TestOperationCoverage_ReadLookupDeleteVerbosePaths(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	u2 := &User{ID: 2, Name: "bar", Email: "bar@example.com"}
	db := setupVerboseDB(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1, u2)
	})

	db.Read(func(tx *Tx) {
		if meta := GetMeta[User](tx, ID(1)); !meta.Exists() {
			t.Fatalf("GetMeta[User](1) = %+v, wanted existing", meta)
		}
		row, meta := tx.GetByKeyVal(usersTable, reflect.ValueOf(ID(1)))
		if row == nil || !meta.Exists() {
			t.Fatalf("GetByKeyVal(1) = (%v, %+v), wanted row and meta", row, meta)
		}
		row, meta = tx.GetByKeyVal(usersTable, reflect.ValueOf(ID(99)))
		if row != nil || meta.Exists() {
			t.Fatalf("GetByKeyVal(99) = (%v, %+v), wanted missing", row, meta)
		}
		row, meta = tx.GetByKeyRaw(usersTable, usersTable.EncodeKey(ID(99)))
		if row != nil || meta.Exists() {
			t.Fatalf("GetByKeyRaw(99) = (%v, %+v), wanted missing", row, meta)
		}
		if tx.GetMetaByKeyVal(usersTable, reflect.ValueOf(ID(99))).Exists() {
			t.Fatalf("GetMetaByKeyVal(99) should be missing")
		}
		if !tx.Exists(usersTable, ID(1)) {
			t.Fatalf("Exists(1) = false, wanted true")
		}
		if tx.Exists(usersTable, ID(99)) {
			t.Fatalf("Exists(99) = true, wanted false")
		}
		if !tx.ExistsByKeyRaw(usersTable, usersTable.EncodeKey(ID(1))) {
			t.Fatalf("ExistsByKeyRaw(1) = false, wanted true")
		}
		if tx.ExistsByKeyRaw(usersTable, usersTable.EncodeKey(ID(99))) {
			t.Fatalf("ExistsByKeyRaw(99) = true, wanted false")
		}

		if Lookup[User](tx, usersByEmail, "foo@example.com") == nil {
			t.Fatalf("Lookup[User](foo@example.com) = nil")
		}
		if Lookup[User](tx, usersByEmail, "missing@example.com") != nil {
			t.Fatalf("Lookup[User](missing@example.com) returned row")
		}
		if key, ok := LookupKey[ID](tx, usersByEmail, "missing@example.com"); ok || key != 0 {
			t.Fatalf("LookupKey(missing) = (%v, %v), wanted zero false", key, ok)
		}
		if key := tx.LookupKeyVal(usersByEmail, reflect.ValueOf("foo@example.com")); !key.IsValid() {
			t.Fatalf("LookupKeyVal(existing) returned invalid key")
		}
		if key := tx.LookupKeyVal(usersByEmail, reflect.ValueOf("missing@example.com")); key.IsValid() {
			t.Fatalf("LookupKeyVal(missing) = %v, wanted invalid", key)
		}
		if !tx.LookupExists(usersByEmail, reflect.ValueOf("foo@example.com")) {
			t.Fatalf("LookupExists(existing) = false")
		}
		if tx.LookupExists(usersByEmail, reflect.ValueOf("missing@example.com")) {
			t.Fatalf("LookupExists(missing) = true")
		}
		if row, meta := tx.LookupVal(usersByEmail, reflect.ValueOf("missing@example.com")); row.IsValid() || meta.Exists() {
			t.Fatalf("LookupVal(missing) = (%v, %+v), wanted missing", row, meta)
		}

		assertPanics(t, func() {
			_ = Lookup[Widget](tx, usersByEmail, "foo@example.com")
		})
		assertPanics(t, func() {
			_, _ = LookupKey[AB](tx, usersByEmail, "foo@example.com")
		})
		assertPanics(t, func() {
			_ = tx.Lookup(usersByEmail, ID(1))
		})
		assertPanics(t, func() {
			_, _ = tx.GetByKeyVal(nil, reflect.ValueOf(ID(1)))
		})
		assertPanics(t, func() {
			_, _, _ = tx.TryGetByKeyVal(nil, reflect.ValueOf(ID(1)))
		})
		assertPanics(t, func() {
			_, _ = tx.GetByKeyRaw(nil, usersTable.EncodeKey(ID(1)))
		})
	})

	db.Write(func(tx *Tx) {
		if !tx.DeleteByKey(usersTable, ID(1)) {
			t.Fatalf("DeleteByKey(1) = false, wanted true")
		}
		if tx.DeleteByKey(usersTable, ID(1)) {
			t.Fatalf("DeleteByKey(1 again) = true, wanted false")
		}
		if !tx.DeleteByKeyRaw(usersTable, usersTable.EncodeKey(ID(2))) {
			t.Fatalf("DeleteByKeyRaw(2) = false, wanted true")
		}
		if tx.DeleteByKeyRaw(usersTable, usersTable.EncodeKey(ID(2))) {
			t.Fatalf("DeleteByKeyRaw(2 again) = true, wanted false")
		}
		Put(tx, &User{ID: 3, Name: "baz", Email: "baz@example.com"})
		if !tx.UnsafeDeleteByKeyRawSkippingIndex(usersTable, usersTable.EncodeKey(ID(3))) {
			t.Fatalf("UnsafeDeleteByKeyRawSkippingIndex(3) = false, wanted true")
		}
		if tx.UnsafeDeleteByKeyRawSkippingIndex(usersTable, usersTable.EncodeKey(ID(3))) {
			t.Fatalf("UnsafeDeleteByKeyRawSkippingIndex(3 again) = true, wanted false")
		}
	})
}

func TestOperationCoverage_PutNoopIndexCleanupAndChangeFlags(t *testing.T) {
	db := setupVerboseDB(t, basicSchema)
	db.Write(func(tx *Tx) {
		u := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
		if !Put(tx, u) {
			t.Fatalf("initial Put = false, wanted true")
		}
		if Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"}) {
			t.Fatalf("same Put = true, wanted false")
		}
		if !Put(tx, &User{ID: 1, Name: "bar", Email: "foo@example.com"}) {
			t.Fatalf("changed Put = false, wanted true")
		}
	})

	db.Read(func(tx *Tx) {
		if LookupExists(tx, usersByName, "foo") {
			t.Fatalf("old name index entry still exists")
		}
		if !LookupExists(tx, usersByName, "bar") {
			t.Fatalf("new name index entry missing")
		}
	})

	var got []*Change
	db.Write(func(tx *Tx) {
		tx.OnChange(map[*Table]ChangeFlags{
			usersTable: ChangeFlagNotify | ChangeFlagIncludeMutableRow | ChangeFlagIncludeOldRow,
		}, func(tx *Tx, chg *Change) {
			got = append(got, chg)
		})
		Put(tx, &User{ID: 2, Name: "alice", Email: "alice@example.com"})
		Put(tx, &User{ID: 2, Name: "ally", Email: "alice@example.com"})
	})
	if len(got) != 2 || !got[0].HasRow() || got[0].HasOldRow() || !got[1].HasRow() || !got[1].HasOldRow() {
		t.Fatalf("mutable row changes not populated as expected: %+v", got)
	}

	got = nil
	db.Write(func(tx *Tx) {
		Put(tx, &User{ID: 3, Name: "raw", Email: "raw@example.com"})
		Put(tx, &User{ID: 4, Name: "known", Email: "known@example.com"})
		tx.OnChange(map[*Table]ChangeFlags{
			usersTable: ChangeFlagNotify | ChangeFlagIncludeKey,
		}, func(tx *Tx, chg *Change) {
			if chg.Op() == OpDelete {
				got = append(got, chg)
			}
		})
		tx.DeleteByKeyRaw(usersTable, usersTable.EncodeKey(ID(3)))
		tx.DeleteByKey(usersTable, ID(4))
	})
	if len(got) != 2 || !got[0].HasKey() || got[0].Key().(ID) != 3 || !got[1].HasKey() || got[1].Key().(ID) != 4 {
		t.Fatalf("delete key changes not populated as expected: %+v", got)
	}

	got = nil
	db.Write(func(tx *Tx) {
		tx.OnChange(map[*Table]ChangeFlags{
			usersTable: ChangeFlagNotify | ChangeFlagIncludeKey,
		}, func(tx *Tx, chg *Change) {
			got = append(got, chg)
		})
		Put(tx, &User{ID: 5, Name: "keyed", Email: "keyed@example.com"})
	})
	if len(got) != 1 || !got[0].HasKey() || got[0].Key().(ID) != 5 || got[0].HasRow() {
		t.Fatalf("put key change not populated as expected: %+v", got)
	}

	var nilTx *Tx
	assertPanics(t, func() {
		nilTx.PutVal(usersTable, reflect.ValueOf(&User{}))
	})
}

func TestOperationCoverage_ChangeHandlersWithCorruptOldRows(t *testing.T) {
	db := setupCorruptValueDB(t)
	db.logf = func(string, ...any) {}

	db.Write(func(tx *Tx) {
		keyRaw := corruptValuesTable.EncodeKey(ID(1))
		indexRaw := existingValueIndexBytes(t, tx, corruptValuesTable, keyRaw)
		rawValue := encodedCorruptValue(&corruptValueWrongPayload{
			ID:    1,
			Code:  "one",
			Count: "bad",
		}, indexRaw)
		putRawTableValue(t, tx, corruptValuesTable, keyRaw, rawValue)
	})

	var deleteChange *Change
	db.Write(func(tx *Tx) {
		tx.OnChange(map[*Table]ChangeFlags{
			corruptValuesTable: ChangeFlagNotify | ChangeFlagIncludeRow,
		}, func(tx *Tx, chg *Change) {
			deleteChange = chg
		})
		if !tx.DeleteByKey(corruptValuesTable, ID(1)) {
			t.Fatalf("DeleteByKey(1) = false, wanted true")
		}
	})
	if deleteChange == nil || deleteChange.HasRow() {
		t.Fatalf("delete corrupt old row change = %+v, wanted no decoded row", deleteChange)
	}

	db.Write(func(tx *Tx) {
		Put(tx, &corruptValueRow{ID: 1, Code: "one", Count: 1})
		keyRaw := corruptValuesTable.EncodeKey(ID(1))
		indexRaw := existingValueIndexBytes(t, tx, corruptValuesTable, keyRaw)
		rawValue := encodedCorruptValue(&corruptValueWrongPayload{
			ID:    1,
			Code:  "one",
			Count: "bad",
		}, indexRaw)
		putRawTableValue(t, tx, corruptValuesTable, keyRaw, rawValue)
	})

	var putChange *Change
	db.Write(func(tx *Tx) {
		tx.OnChange(map[*Table]ChangeFlags{
			corruptValuesTable: ChangeFlagNotify | ChangeFlagIncludeRow | ChangeFlagIncludeOldRow,
		}, func(tx *Tx, chg *Change) {
			putChange = chg
		})
		Put(tx, &corruptValueRow{ID: 1, Code: "one", Count: 2})
	})
	if putChange == nil || !putChange.HasRow() || putChange.HasOldRow() {
		t.Fatalf("put corrupt old row change = %+v, wanted new row without old row", putChange)
	}

	db.Write(func(tx *Tx) {
		keyRaw := corruptValuesTable.EncodeKey(ID(1))
		putRawTableValue(t, tx, corruptValuesTable, keyRaw, []byte{1, 1, 1, 1})
		assertPanics(t, func() {
			Put(tx, &corruptValueRow{ID: 1, Code: "one", Count: 3})
		})
	})
}

func TestOperationCoverage_CorruptReadLookupAndIndexDecodePaths(t *testing.T) {
	db := setupCorruptValueDB(t)
	db.Write(func(tx *Tx) {
		keyRaw := corruptValuesTable.EncodeKey(ID(1))
		indexRaw := existingValueIndexBytes(t, tx, corruptValuesTable, keyRaw)
		rawValue := encodedCorruptValue(&corruptValueWrongPayload{
			ID:    1,
			Code:  "one",
			Count: "bad",
		}, indexRaw)
		putRawTableValue(t, tx, corruptValuesTable, keyRaw, rawValue)
	})
	db.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_ = Reload[corruptValueRow](tx, &corruptValueRow{ID: 1})
		})
		assertPanics(t, func() {
			_, _ = tx.GetByKeyVal(corruptValuesTable, reflect.ValueOf(ID(1)))
		})
		assertPanics(t, func() {
			_, _ = tx.GetByKeyRaw(corruptValuesTable, corruptValuesTable.EncodeKey(ID(1)))
		})
		assertPanics(t, func() {
			_ = tx.Lookup(corruptValuesByCode, "one")
		})
	})

	db = setupCorruptValueDB(t)
	var memento []byte
	db.Read(func(tx *Tx) {
		c := TableScan[corruptValueRow](tx, ExactScan(ID(1))).Raw()
		if !c.Next() {
			t.Fatalf("scan did not find row")
		}
		memento = append([]byte(nil), c.ValueMemento()...)
	})
	db.Read(func(tx *Tx) {
		row, meta, err := tx.DecodeMementoVal(corruptValuesTable, nil, memento)
		if !row.IsValid() || meta.Exists() || err == nil {
			t.Fatalf("DecodeMementoVal with bad key = (%v, %+v, %v), wanted error", row, meta, err)
		}
	})
	db.Write(func(tx *Tx) {
		putRawTableValue(t, tx, corruptValuesTable, corruptValuesTable.EncodeKey(ID(1)), []byte{1, 1, 1, 1})
	})
	db.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_ = tx.GetMetaByKeyVal(corruptValuesTable, reflect.ValueOf(ID(1)))
		})
	})

	db2 := setup(t, basicSchema)
	db2.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
	})
	db2.Read(func(tx *Tx) {
		c := tx.IndexScan(usersByName, ExactScan("foo"))
		if !c.Next() {
			t.Fatalf("index scan did not find row")
		}
		tup, keyRaw := decodeNonUniqueIndexTableKey(c.ik, usersByName)
		if len(tup) != 2 || !reflect.DeepEqual(keyRaw, usersTable.EncodeKey(ID(1))) {
			t.Fatalf("decodeNonUniqueIndexTableKey = (%v, %x), wanted foo/1", tup, keyRaw)
		}
	})
	db2.Write(func(tx *Tx) {
		_ = tx.UnsafeDeleteByKeyRawSkippingIndex(usersTable, usersTable.EncodeKey(ID(1)))
	})
	db2.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_, _ = tx.LookupVal(usersByEmail, reflect.ValueOf("foo@example.com"))
		})
	})

	db3 := setup(t, basicSchema)
	db3.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
		ensure(usersByEmail.bucketIn(tx).Put(usersByEmail.EncodeKey("foo@example.com"), []byte{0xff}))
	})
	db3.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_ = tx.LookupKey(usersByEmail, "foo@example.com")
		})
	})
	db3.Write(func(tx *Tx) {
		badValue := tuple{usersTable.EncodeKey(ID(1)), usersTable.EncodeKey(ID(2))}.encode(nil)
		ensure(usersByEmail.bucketIn(tx).Put(usersByEmail.EncodeKey("foo@example.com"), badValue))
	})
	db3.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_ = tx.LookupKey(usersByEmail, "foo@example.com")
		})
	})

	db4 := setup(t, basicSchema)
	db4.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
		fe := flatEncoder{}
		usersByName.keyEnc.encodeInto(&fe, reflect.ValueOf("foo"))
		ensure(usersByName.bucketIn(tx).Put(fe.buf, emptyIndexValue))
	})
	db4.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_ = tx.LookupKey(usersByName, "foo")
		})
		assertPanics(t, func() {
			_ = decodeIndexKey([]byte{0xff}, usersByName)
		})
		assertPanics(t, func() {
			_, _ = decodeIndexRow(usersByEmail, usersByEmail.EncodeKey("x"), nil)
		})
		assertPanics(t, func() {
			_, _ = decodeIndexRow(usersByName, usersByName.EncodeKey("x"), tuple{[]byte("bad")}.encode(nil))
		})
	})
}

func TestOperationCoverage_KVOperations(t *testing.T) {
	db := setup(t, basicSchema)
	k1 := x("10 12")
	k2 := x("10 14")
	v1 := buildKV(0x42, 0x0055)
	v2 := buildKV(0x42, 0x8877)

	db.Write(func(tx *Tx) {
		if got := tx.KVGetRaw(wumpets, k1); got != nil {
			t.Fatalf("KVGetRaw(missing) = %x, wanted nil", got)
		}
		_ = tx.KVGet(wumpets, k1)
		tx.KVPutRaw(wumpets, k1, v1.Bytes())
		tx.KVPutRaw(wumpets, k1, v1.Bytes())
		tx.KVPutRaw(wumpets, k1, v2.Bytes())
		tx.KVPut(wumpets, k2, v1)
		tx.KVPut(wumpets, k2, nil)
		tx.KVPutRaw(wumpets, k1, nil)
		tx.KVPutRaw(kubets, []byte("raw"), []byte("value"))
		tx.KVPutRaw(kubets, []byte("raw"), nil)
	})

	db.Write(func(tx *Tx) {
		tx.KVPutRaw(wumpets, k1, v2.Bytes())
	})
	db.Read(func(tx *Tx) {
		tx.logger = nil
		if got := tx.KVGetRaw(wumpets, k1); got == nil {
			t.Fatalf("KVGetRaw(existing) returned nil")
		}
		tableKeys := tx.KVTableScan(wumpets, RawRange{}).Keys()
		tableKeys(func(k []byte) bool {
			return false
		})
		indexKeys := tx.KVIndexScan(wumpetsByB, RawRange{}).IndexKeys()
		indexKeys(func(k []byte) bool {
			return false
		})
		_ = tx.KVTableScan(wumpets, RawRange{})
		_ = tx.KVIndexScan(wumpetsByB, RawRange{})
	})

	ci := &kvIndexCursorImpl{
		idx: &KVIndex{
			indexKeyToPrimaryKey: func([]byte) []byte { return nil },
		},
	}
	if ci.RawTableValue() != nil {
		t.Fatalf("RawTableValue with nil table key should be nil")
	}

	var nilTx *Tx
	assertPanics(t, func() {
		_ = nilTx.KVGetRaw(wumpets, k1)
	})
	assertPanics(t, func() {
		nilTx.KVPutRaw(wumpets, k1, v1.Bytes())
	})
	assertPanics(t, func() {
		_ = nilTx.KVTableScan(wumpets, RawRange{})
	})
	assertPanics(t, func() {
		_ = nilTx.KVIndexScan(wumpetsByB, RawRange{})
	})
}

func TestOperationCoverage_TxAndSingletonHelpers(t *testing.T) {
	scm := &Schema{}
	mp := AddKVMap(scm, "meta")
	sk := AddSingletonKey[int](mp, "k")
	db := setup(t, scm)
	db.Read(func(tx *Tx) {
		var missing int
		if SGet(tx, sk, &missing) {
			t.Fatalf("SGet before write = true, wanted false")
		}
	})
	tx := db.BeginUpdate()
	if !tx.IsWritable() {
		t.Fatalf("BeginUpdate transaction should be writable")
	}
	tx.BeginVerbose()
	tx.EndVerbose()
	tx.SetLogger(slog.Default())
	if tx.DB() != db {
		t.Fatalf("tx.DB() != db")
	}
	if tx.StartTime().IsZero() {
		t.Fatalf("tx.StartTime() is zero")
	}
	tx.prepareToRead()
	value := 7
	SPut(tx, sk, &value)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	tx.Close()

	errBoom := errors.New("boom")
	if err := db.ReadErr(func(tx *Tx) error {
		var missing int
		if SGet(tx, sk, &missing) && missing != value {
			t.Fatalf("SGet returned unexpected value %d", missing)
		}
		if found, ok := tx.GetMemo("x"); ok || found != nil {
			t.Fatalf("GetMemo before set = (%v, %v), wanted nil false", found, ok)
		}
		got, err := tx.Memo("x", func() (any, error) {
			return "memo", nil
		})
		if err != nil || got != "memo" {
			t.Fatalf("Memo set = (%v, %v), wanted memo nil", got, err)
		}
		got, ok := tx.GetMemo("x")
		if !ok || got != "memo" {
			t.Fatalf("GetMemo after set = (%v, %v), wanted memo true", got, ok)
		}
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Fatalf("ReadErr = %v, wanted boom", err)
	}

	readTx := db.BeginRead()
	if readTx.IsWritable() {
		t.Fatalf("BeginRead transaction should not be writable")
	}
	readTx.Close()

	if err := db.Tx(false, func(tx *Tx) error {
		return nil
	}); err != nil {
		t.Fatalf("read Tx failed: %v", err)
	}

	if err := db.Tx(true, func(tx *Tx) error {
		tx.CommitDespiteError()
		next := 9
		SPut(tx, sk, &next)
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Fatalf("write Tx = %v, wanted boom", err)
	}

	db.Read(func(tx *Tx) {
		var got int
		if !SGet(tx, sk, &got) || got != 9 {
			t.Fatalf("SGet after CommitDespiteError = %d, wanted 9", got)
		}
	})

	memDB := must(Open(InMemory, scm, Options{IsTesting: true}))
	t.Cleanup(memDB.Close)
	if err := memDB.Tx(false, func(tx *Tx) error {
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Fatalf("in-memory read Tx = %v, wanted boom", err)
	}
	if err := memDB.Tx(true, func(tx *Tx) error {
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Fatalf("in-memory write Tx before write = %v, wanted boom", err)
	}
	if err := memDB.Tx(true, func(tx *Tx) error {
		dropped := 11
		SPut(tx, sk, &dropped)
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Fatalf("in-memory write Tx after write = %v, wanted boom", err)
	}
	memDB.Read(func(tx *Tx) {
		var got int
		if SGet(tx, sk, &got) {
			t.Fatalf("SGet after failed in-memory write = %d, wanted missing", got)
		}
	})
	if err := memDB.Tx(true, func(tx *Tx) error {
		kept := 13
		SPut(tx, sk, &kept)
		tx.CommitDespiteError()
		return errBoom
	}); !errors.Is(err, errBoom) {
		t.Fatalf("in-memory CommitDespiteError Tx = %v, wanted boom", err)
	}
	memDB.Read(func(tx *Tx) {
		var got int
		if !SGet(tx, sk, &got) || got != 13 {
			t.Fatalf("SGet after in-memory CommitDespiteError = %d, wanted 13", got)
		}
	})

	db.Write(func(tx *Tx) {
		SPutRaw(tx, sk, []byte{0xc1})
	})
	db.Read(func(tx *Tx) {
		assertPanics(t, func() {
			var bad int
			_ = SGet(tx, sk, &bad)
		})
	})

	var nilTx *Tx
	assertPanics(t, func() {
		_ = SGetRaw(nilTx, sk)
	})
	assertPanics(t, func() {
		SPutRaw(nilTx, sk, []byte{1})
	})

	if got := fmt.Sprint(Proto[int]()); got != "<nil>" {
		t.Fatalf("Proto[int]() string = %q, wanted <nil>", got)
	}
}

func TestOperationCoverage_ReindexAllIndexesWithMissingBucket(t *testing.T) {
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx,
			&User{ID: 1, Name: "foo", Email: "foo@example.com"},
			&User{ID: 2, Name: "bar", Email: "bar@example.com"},
		)
		ensure(tx.stx.DeleteBucket(usersTable.name, usersByName.buck))
		tx.Reindex(usersTable, nil)
	})

	db.Read(func(tx *Tx) {
		if Lookup[User](tx, usersByEmail, "foo@example.com") == nil {
			t.Fatalf("email index missing after Reindex")
		}
		if Lookup[User](tx, usersByName, "bar") == nil {
			t.Fatalf("name index missing after Reindex")
		}
	})
}

func setupVerboseDB(t testing.TB, schema *Schema) *DB {
	t.Helper()
	db := setup(t, schema)
	db.verbose = true
	db.logf = func(string, ...any) {}
	return db
}
