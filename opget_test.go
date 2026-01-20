package edb

import "testing"

func TestGetHelpers(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1)
	})

	db.Read(func(tx *Tx) {
		if !Exists[User](tx, ID(1)) {
			t.Fatalf("Exists[User](1) = false, wanted true")
		}
		if Exists[User](tx, ID(2)) {
			t.Fatalf("Exists[User](2) = true, wanted false")
		}

		tbl := tx.Schema().TableByRow((*User)(nil))
		rawKey := tbl.EncodeKey(ID(1))
		got := GetByKeyRaw[User](tx, rawKey)
		deepEqual(t, got, u1)
		isnil(t, GetByKeyRaw[User](tx, tbl.EncodeKey(ID(2))))

		if !tx.ExistsByKeyRaw(tbl, rawKey) {
			t.Fatalf("ExistsByKeyRaw(1) = false, wanted true")
		}

		row, meta, err := tx.TryGet(tbl, ID(1))
		if err != nil || row == nil || !meta.Exists() {
			t.Fatalf("TryGet(1) = (%v, %v, %v), wanted non-nil row and meta.Exists", row, meta, err)
		}

		row, meta, err = tx.TryGet(tbl, ID(2))
		if err != nil || row != nil || meta.Exists() {
			t.Fatalf("TryGet(2) = (%v, %v, %v), wanted nil row and meta.IsMissing", row, meta, err)
		}

		meta1 := tx.GetMeta(tbl, ID(1))
		if meta1.IsMissing() || meta1.SchemaVer != 1 || meta1.ModCount == 0 {
			t.Fatalf("GetMeta(1) = %+v, wanted schemaVer=1 and non-zero modCount", meta1)
		}
		meta2 := tx.GetMeta(tbl, ID(2))
		if meta2.Exists() {
			t.Fatalf("GetMeta(2) = %+v, wanted missing", meta2)
		}

		got = Reload[User](tx, &User{ID: 1})
		deepEqual(t, got, u1)
		isnil(t, Reload[User](tx, &User{ID: 2}))
	})
}
