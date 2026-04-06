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
