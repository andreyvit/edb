package edb

import "testing"

func TestDeleteByKeyRaw_ReturnValueAndIndexCleanup(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1)
	})

	var rawKey []byte
	db.Read(func(tx *Tx) {
		rawKey = usersTable.EncodeKey(ID(1))
		if usersTable.dataBucketIn(tx).KeyCount() != 1 {
			t.Fatalf("data keycount = %d, wanted 1", usersTable.dataBucketIn(tx).KeyCount())
		}
		if usersByEmail.bucketIn(tx).KeyCount() != 1 {
			t.Fatalf("index keycount = %d, wanted 1", usersByEmail.bucketIn(tx).KeyCount())
		}
	})

	db.Write(func(tx *Tx) {
		if ok := tx.DeleteByKeyRaw(usersTable, usersTable.EncodeKey(ID(999))); ok {
			t.Fatalf("DeleteByKeyRaw(missing) = true, wanted false")
		}
		if ok := tx.DeleteByKeyRaw(usersTable, rawKey); !ok {
			t.Fatalf("DeleteByKeyRaw(existing) = false, wanted true")
		}
	})

	db.Read(func(tx *Tx) {
		if usersTable.dataBucketIn(tx).KeyCount() != 0 {
			t.Fatalf("data keycount = %d, wanted 0", usersTable.dataBucketIn(tx).KeyCount())
		}
		if usersByEmail.bucketIn(tx).KeyCount() != 0 {
			t.Fatalf("index keycount = %d, wanted 0 (index entries should be removed)", usersByEmail.bucketIn(tx).KeyCount())
		}
	})
}

func TestUnsafeDeleteByKeyRawSkippingIndex_SkipsIndexCleanup(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1)
	})

	db.Write(func(tx *Tx) {
		rawKey := usersTable.EncodeKey(ID(1))
		if ok := tx.UnsafeDeleteByKeyRawSkippingIndex(usersTable, rawKey); !ok {
			t.Fatalf("UnsafeDeleteByKeyRawSkippingIndex(existing) = false, wanted true")
		}
	})

	db.Read(func(tx *Tx) {
		if usersTable.dataBucketIn(tx).KeyCount() != 0 {
			t.Fatalf("data keycount = %d, wanted 0", usersTable.dataBucketIn(tx).KeyCount())
		}
		if usersByEmail.bucketIn(tx).KeyCount() != 1 {
			t.Fatalf("index keycount = %d, wanted 1 (unsafe delete should leave index entries)", usersByEmail.bucketIn(tx).KeyCount())
		}
	})
}

func TestDeleteAll(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	u2 := &User{ID: 2, Name: "bar", Email: "bar@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1, u2)
	})
	db.Write(func(tx *Tx) {
		n := DeleteAll(tx.TableScan(usersTable, FullScan()))
		if n != 2 {
			t.Fatalf("DeleteAll = %d, wanted 2", n)
		}
	})
	db.Read(func(tx *Tx) {
		isempty(t, AllTableRows[User](tx))
	})
}

