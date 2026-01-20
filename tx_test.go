package edb

import (
	"errors"
	"strings"
	"testing"
)

func TestTx_MemoCachesValueAndError(t *testing.T) {
	db := setup(t, basicSchema)

	db.Read(func(tx *Tx) {
		calls := 0
		v, err := tx.Memo("k", func() (any, error) {
			calls++
			return 42, nil
		})
		if err != nil || v.(int) != 42 || calls != 1 {
			t.Fatalf("Memo #1 = (%v, %v), calls=%d; wanted (42, nil), calls=1", v, err, calls)
		}
		v, err = tx.Memo("k", func() (any, error) {
			calls++
			return 777, nil
		})
		if err != nil || v.(int) != 42 || calls != 1 {
			t.Fatalf("Memo #2 = (%v, %v), calls=%d; wanted (42, nil), calls=1", v, err, calls)
		}

		calls = 0
		wantErr := errors.New("boom")
		v, err = tx.Memo("e", func() (any, error) {
			calls++
			return nil, wantErr
		})
		if err == nil || v != nil || calls != 1 {
			t.Fatalf("Memo err #1 = (%v, %v), calls=%d; wanted (nil, err), calls=1", v, err, calls)
		}
		v, err = tx.Memo("e", func() (any, error) {
			calls++
			return 1, nil
		})
		if err == nil || v != nil || calls != 1 {
			t.Fatalf("Memo err #2 = (%v, %v), calls=%d; wanted (nil, err), calls=1", v, err, calls)
		}
	})

	db.Read(func(tx *Tx) {
		calls := 0
		v, err := Memo[int](tx, "typed", func() (int, error) {
			calls++
			return 7, nil
		})
		if err != nil || v != 7 || calls != 1 {
			t.Fatalf("Memo[int] #1 = (%v, %v), calls=%d; wanted (7, nil), calls=1", v, err, calls)
		}
		v, err = Memo[int](tx, "typed", func() (int, error) {
			calls++
			return 8, nil
		})
		if err != nil || v != 7 || calls != 1 {
			t.Fatalf("Memo[int] #2 = (%v, %v), calls=%d; wanted (7, nil), calls=1", v, err, calls)
		}
	})
}

func TestTx_BeginUpdateRollsBackOnClose(t *testing.T) {
	db := setup(t, basicSchema)

	key := x("aa")
	tx := db.BeginUpdate()
	tx.KVPutRaw(kubets, key, []byte{1})
	tx.Close() // rollback

	db.Read(func(tx *Tx) {
		if got := tx.KVGetRaw(kubets, key); got != nil {
			t.Fatalf("KVGetRaw after rollback = %x, wanted nil", got)
		}
	})
}

func TestDBTx_CommitDespiteError(t *testing.T) {
	db := setup(t, basicSchema)

	key1 := x("01")
	err := db.Tx(true, func(tx *Tx) error {
		tx.KVPutRaw(kubets, key1, []byte{1})
		return errors.New("boom")
	})
	if err == nil {
		t.Fatalf("db.Tx err = nil, wanted error")
	}
	db.Read(func(tx *Tx) {
		if got := tx.KVGetRaw(kubets, key1); got != nil {
			t.Fatalf("KVGetRaw after failed Tx = %x, wanted nil", got)
		}
	})

	key2 := x("02")
	err = db.Tx(true, func(tx *Tx) error {
		tx.KVPutRaw(kubets, key2, []byte{2})
		tx.CommitDespiteError()
		return errors.New("boom")
	})
	if err == nil {
		t.Fatalf("db.Tx err = nil, wanted error")
	}
	db.Read(func(tx *Tx) {
		if got := tx.KVGetRaw(kubets, key2); got == nil || len(got) != 1 || got[0] != 2 {
			t.Fatalf("KVGetRaw after CommitDespiteError = %x, wanted 02", got)
		}
	})
}

func TestDBTx_PanicBecomesError(t *testing.T) {
	db := setup(t, basicSchema)

	err := db.Tx(true, func(tx *Tx) error {
		panic("boom")
	})
	if err == nil {
		t.Fatalf("db.Tx err = nil, wanted error")
	}
	if !strings.Contains(err.Error(), "panic: boom") {
		t.Fatalf("db.Tx err = %q, wanted it to include %q", err.Error(), "panic: boom")
	}
}

func TestTx_BoltTx(t *testing.T) {
	db := setup(t, basicSchema)
	db.Read(func(tx *Tx) {
		btx := tx.BoltTx()
		if testing.Short() {
			if btx != nil {
				t.Fatalf("BoltTx() = %v, wanted nil in -short (in-memory backend)", btx)
			}
		} else {
			if btx == nil {
				t.Fatalf("BoltTx() = nil, wanted non-nil in non-short (Bolt backend)")
			}
		}
	})
}

