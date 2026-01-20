package edb

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

func TestAppendUintHelpers(t *testing.T) {
	if got := appendUint8(nil, 0x42); len(got) != 1 || got[0] != 0x42 {
		t.Fatalf("appendUint8 = %x", got)
	}
	if got := appendUint16(nil, 0x0102); !reflect.DeepEqual(got, []byte{0x01, 0x02}) {
		t.Fatalf("appendUint16 = %x, wanted 0102", got)
	}
	if got := appendUint32(nil, 0x01020304); !reflect.DeepEqual(got, []byte{0x01, 0x02, 0x03, 0x04}) {
		t.Fatalf("appendUint32 = %x, wanted 01020304", got)
	}
}

func TestLookupAndScanHelpers(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	u2 := &User{ID: 2, Name: "bar", Email: "bar@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1, u2)
	})

	db.Read(func(tx *Tx) {
		if !tx.ExistsByKeyRaw(usersTable, usersTable.EncodeKey(ID(1))) {
			t.Fatalf("ExistsByKeyRaw(1) = false, wanted true")
		}
		if tx.ExistsByKeyRaw(usersTable, usersTable.EncodeKey(ID(999))) {
			t.Fatalf("ExistsByKeyRaw(999) = true, wanted false")
		}

		k, ok := LookupKey[ID](tx, usersByEmail, "foo@example.com")
		if !ok || k != ID(1) {
			t.Fatalf("LookupKey = (%v, %v), wanted (1, true)", k, ok)
		}
		if LookupExists(tx, usersByEmail, "xxx@example.com") {
			t.Fatalf("LookupExists(xxx) = true, wanted false")
		}
		if v := tx.Lookup(usersByEmail, "foo@example.com"); v == nil {
			t.Fatalf("tx.Lookup returned nil, wanted row")
		}
		if v := tx.LookupKey(usersByEmail, "foo@example.com"); v == nil {
			t.Fatalf("tx.LookupKey returned nil, wanted key")
		}

		if anyToRow[User](nil) != nil {
			t.Fatalf("anyToRow(nil) != nil")
		}
		if got := anyToRow[User](u1); got != u1 {
			t.Fatalf("anyToRow(u1) != u1")
		}
		if got := valToAny(reflect.ValueOf(u1)); got != u1 {
			t.Fatalf("valToAny(u1) != u1")
		}
		if got := valToAny(reflect.Value{}); got != nil {
			t.Fatalf("valToAny(invalid) != nil")
		}
		if got := keyRawToVal(usersTable.EncodeKey(ID(1)), usersTable); !got.IsValid() || got.Interface().(ID) != ID(1) {
			t.Fatalf("keyRawToVal returned unexpected value: %v", got)
		}
		if got := keyRawToVal(nil, usersTable); got.IsValid() {
			t.Fatalf("keyRawToVal(nil) returned valid value")
		}

		c1 := FullTableScan[User](tx)
		_ = c1.Raw()
		if !c1.Next() {
			t.Fatalf("FullTableScan.Next() = false, wanted true")
		}
		_ = c1.Key()
		_ = c1.Meta()
		for row := range c1.Rows() {
			if row == nil {
				t.Fatalf("Rows() yielded nil row")
			}
			break
		}

		if got := len(AllLimited(TableScan[User](tx, FullScan()), 1)); got != 1 {
			t.Fatalf("AllLimited(limit=1) returned %d rows, wanted 1", got)
		}
		keys := AllKeys[ID](TableScan[User](tx, FullScan()).Raw())
		if len(keys) != 2 {
			t.Fatalf("AllKeys returned %d keys, wanted 2", len(keys))
		}
		uks := AllUntypedKeys(TableScan[User](tx, FullScan()).Raw())
		if len(uks) != 2 {
			t.Fatalf("AllUntypedKeys returned %d keys, wanted 2", len(uks))
		}
		if First(TableScan[User](tx, FullScan())) == nil {
			t.Fatalf("First returned nil, wanted row")
		}
		if got := FirstKey[ID, User](TableScan[User](tx, FullScan())); got == 0 {
			t.Fatalf("FirstKey returned 0, wanted non-zero")
		}
		if Select(TableScan[User](tx, FullScan()), func(u *User) bool { return u.ID == 2 }) == nil {
			t.Fatalf("Select returned nil, wanted a row")
		}
		if got := len(Filter(TableScan[User](tx, FullScan()), func(u *User) bool { return u.ID == 1 })); got != 1 {
			t.Fatalf("Filter returned %d rows, wanted 1", got)
		}
		if got := Count(TableScan[User](tx, FullScan()).Raw()); got != 2 {
			t.Fatalf("Count returned %d, wanted 2", got)
		}

		if !FullReverseTableScan[User](tx).Next() {
			t.Fatalf("FullReverseTableScan.Next() = false, wanted true")
		}
		if !RangeTableScan[User](tx, ID(1), ID(2), true, true).Next() {
			t.Fatalf("RangeTableScan.Next() = false, wanted true")
		}
		if !ReverseRangeTableScan[User](tx, ID(1), ID(2), true, true).Next() {
			t.Fatalf("ReverseRangeTableScan.Next() = false, wanted true")
		}
		if !ExactTableScan[User](tx, ID(1)).Next() {
			t.Fatalf("ExactTableScan.Next() = false, wanted true")
		}
		if !FullIndexScan[User](tx, usersByName).Next() {
			t.Fatalf("FullIndexScan.Next() = false, wanted true")
		}
		if !ExactIndexScan[User](tx, usersByName, "bar").Next() {
			t.Fatalf("ExactIndexScan.Next() = false, wanted true")
		}
		if !ReverseExactIndexScan[User](tx, usersByName, "bar").Next() {
			t.Fatalf("ReverseExactIndexScan.Next() = false, wanted true")
		}
		if !RangeIndexScan[User](tx, usersByName, "a", "z", true, true).Next() {
			t.Fatalf("RangeIndexScan.Next() = false, wanted true")
		}
		if !ReverseRangeIndexScan[User](tx, usersByName, "a", "z", true, true).Next() {
			t.Fatalf("ReverseRangeIndexScan.Next() = false, wanted true")
		}
		if !PrefixIndexScan[User](tx, usersByName, 1, "bar").Next() {
			t.Fatalf("PrefixIndexScan.Next() = false, wanted true")
		}
		if !ReversePrefixIndexScan[User](tx, usersByName, 1, "bar").Next() {
			t.Fatalf("ReversePrefixIndexScan.Next() = false, wanted true")
		}

		tc := tx.TableScan(usersTable, FullScan())
		if tc.Next() {
			_ = tc.Table()
			_ = tc.Tx()
			_ = tc.RawKey()
			_ = tc.RawRow()
			_ = tc.ValueMemento()
			_, _ = tc.Row()
			_, _, _ = tc.TryRow()
		}

		ic := tx.IndexScan(usersByName, FullScan())
		if ic.Next() {
			_ = ic.Table()
			_ = ic.Tx()
			_ = ic.RawKey()
			_ = ic.IndexKey()
			_ = ic.Key()
			_ = ic.RawRow()
			_ = ic.ValueMemento()
			_ = ic.Meta()
			_, _ = ic.Row()
			_, _, _ = ic.TryRow()
		}

		_ = FullScan().LogString()
		_ = ExactScanVal(reflect.ValueOf(ID(1))).LogString()
		_ = LowerBoundScan(ID(1), true).LogString()
		_ = UpperBoundScan(ID(1), true).LogString()
	})
}

func TestKVHelpers(t *testing.T) {
	scm := &Schema{}

	ks := kvo.NewSchema()
	et := kvo.NewEntityType(ks, "root")
	rootModel := kvo.NewModel(ks, et, func(b *kvo.ModelBuilder) {})
	var idx *KVIndex
	tbl := DefineKVTable(scm, "kvt", rootModel, nil, func(b *KVTableBuilder) {
		idx = b.DefineIndex("pk", nil, func(ik []byte) []byte { return ik }, func(b *KVIndexContentBuilder, pk []byte, v kvo.ImmutableRecord) {
			b.Add(pk)
		})
	})

	db := must(Open(InMemory, scm, Options{IsTesting: true}))
	defer db.Close()

	key := []byte("k")
	rec := kvo.NewRecord(nil)
	rec.Set(1, 42)

	db.Write(func(tx *Tx) {
		tx.KVPut(tbl, key, rec.Record())
	})
	db.Read(func(tx *Tx) {
		if got := tx.KVGet(tbl, key).Get(1); got != 42 {
			t.Fatalf("KVGet.Get(1) = %d, wanted 42", got)
		}
		cur := tx.KVTableScan(tbl, RawRange{})
		if !cur.Next() {
			t.Fatalf("KVTableScan.Next() = false, wanted true")
		}
		if cur.RawIndexKey() != nil {
			t.Fatalf("KVTableScan.RawIndexKey() != nil, wanted nil")
		}
		_ = cur.RawValue()
		_ = cur.Object()
		for range tx.KVTableScan(tbl, RawRange{}).Objects() {
			break
		}
		for range tx.KVTableScan(tbl, RawRange{}).RawValues() {
			break
		}

		ic := tx.KVIndexScan(idx, RawRange{})
		if ic.Next() {
			_ = ic.RawIndexKey()
			_ = ic.RawKey()
			_ = ic.RawValue()
		}
	})
}

func TestSingletonKeyOpsAndCountAll(t *testing.T) {
	scm := &Schema{}
	mp := AddKVMap(scm, "meta")
	sk := AddSingletonKey[int](mp, "k")

	db := must(Open(InMemory, scm, Options{IsTesting: true}))
	defer db.Close()

	db.Write(func(tx *Tx) {
		v := 123
		SPut(tx, sk, &v)
	})
	db.Read(func(tx *Tx) {
		var v int
		if !SGet(tx, sk, &v) || v != 123 {
			t.Fatalf("SGet = (%v, %v), wanted (true, 123)", v != 123, v)
		}
		if SGetRaw(tx, sk) == nil {
			t.Fatalf("SGetRaw returned nil, wanted bytes")
		}
	})

	db2 := setup(t, basicSchema)
	db2.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "a", Email: "a@example.com"})
		Put(tx, &User{ID: 2, Name: "b", Email: "b@example.com"})
	})
	db2.Read(func(tx *Tx) {
		if got := CountAll(tx, usersTable); got != 2 {
			t.Fatalf("CountAll = %d, wanted 2", got)
		}
	})

	if reflect.TypeOf(Proto[int]()).String() != "*int" {
		t.Fatalf("Proto[int]() returned unexpected type")
	}
}

func TestReindexAndDecodeMementoVal(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	db := setup(t, basicSchema)

	var keyRaw, memento []byte
	db.Write(func(tx *Tx) { Put(tx, u1) })
	db.Read(func(tx *Tx) {
		c := tx.TableScan(usersTable, FullScan())
		if !c.Next() {
			t.Fatalf("TableScan.Next() = false, wanted true")
		}
		keyRaw = append([]byte(nil), c.RawKey()...)
		memento = append([]byte(nil), c.ValueMemento()...)
	})

	db.Read(func(tx *Tx) {
		rowVal, meta, err := tx.DecodeMementoVal(usersTable, keyRaw, memento)
		if err != nil || meta.IsMissing() || !rowVal.IsValid() {
			t.Fatalf("DecodeMementoVal = (%v, %+v, %v), wanted valid row and meta", rowVal, meta, err)
		}
	})

	db.Write(func(tx *Tx) {
		_ = tx.UnsafeDeleteByKeyRawSkippingIndex(usersTable, keyRaw)
		tx.Reindex(usersTable, usersByEmail)
	})

	db.Read(func(tx *Tx) {
		if usersByEmail.bucketIn(tx).KeyCount() != 0 {
			t.Fatalf("after Reindex, index keycount = %d, wanted 0", usersByEmail.bucketIn(tx).KeyCount())
		}
	})
}

func TestExactIDRangeScan(t *testing.T) {
	u1 := &User{ID: 1, Name: "bar", Email: "bar1@example.com"}
	u2 := &User{ID: 2, Name: "bar", Email: "bar2@example.com"}
	u3 := &User{ID: 3, Name: "bar", Email: "bar3@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) { Put(tx, u1, u2, u3) })

	db.Read(func(tx *Tx) {
		rows := All(IndexScan[User](tx, usersByName, ExactIDRangeScan("bar", ID(2), ID(2), true, true)))
		if len(rows) != 1 || rows[0].ID != 2 {
			t.Fatalf("ExactIDRangeScan returned %+v, wanted only ID=2", rows)
		}
	})
}
