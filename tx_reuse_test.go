package edb

import "testing"

func TestTxSkipsBufferTrackingWhenReuseDisabled(t *testing.T) {
	db := must(Open(InMemory, basicSchema, Options{
		IsTesting:      true,
		ReuseTxBuffers: false,
	}))
	t.Cleanup(db.Close)

	db.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
		if tx.valueBufs != nil {
			t.Fatalf("valueBufs tracked with reuse disabled: %d", len(tx.valueBufs))
		}
		if tx.indexValueBufs != nil {
			t.Fatalf("indexValueBufs tracked with reuse disabled: %d", len(tx.indexValueBufs))
		}
	})

	db.Read(func(tx *Tx) {
		c := ExactIndexScan[User](tx, usersByName, "foo")
		if !c.Next() {
			t.Fatal("ExactIndexScan returned no rows")
		}
		if tx.indexKeyBufs != nil {
			t.Fatalf("indexKeyBufs tracked with reuse disabled: %d", len(tx.indexKeyBufs))
		}
	})
}

func TestTxTracksAndReleasesBuffersWhenReuseEnabled(t *testing.T) {
	db := must(Open(InMemory, basicSchema, Options{
		IsTesting:      true,
		ReuseTxBuffers: true,
	}))
	t.Cleanup(db.Close)

	tx := db.BeginUpdate()
	Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
	if len(tx.valueBufs) == 0 {
		t.Fatalf("valueBufs not tracked")
	}
	if len(tx.indexValueBufs) == 0 {
		t.Fatalf("indexValueBufs not tracked")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	tx.Close()
	if tx.valueBufs != nil || tx.indexValueBufs != nil {
		t.Fatalf("write buffers not released")
	}

	tx = db.BeginRead()
	c := ExactIndexScan[User](tx, usersByName, "foo")
	if !c.Next() {
		t.Fatal("ExactIndexScan returned no rows")
	}
	if len(tx.indexKeyBufs) == 0 {
		t.Fatalf("indexKeyBufs not tracked")
	}
	tx.Close()
	if tx.indexKeyBufs != nil {
		t.Fatalf("index key buffers not released")
	}
}
