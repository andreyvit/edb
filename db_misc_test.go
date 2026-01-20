package edb

import (
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDB_BoltSizeDescribeOpenTxnsAndSafeToQuit(t *testing.T) {
	t.Run("in-memory", func(t *testing.T) {
		db := must(Open(InMemory, basicSchema, Options{IsTesting: true}))

		if db.Bolt() != nil {
			t.Fatalf("Bolt() != nil for in-memory DB")
		}
		_ = db.Size()

		var safe atomic.Int32
		db.CloseWithSafeToQuitCallback(func() { safe.Add(1) })
		if safe.Load() != 1 {
			t.Fatalf("safeToQuit called %d times, wanted 1", safe.Load())
		}
	})

	t.Run("bolt", func(t *testing.T) {
		dbFile := must(os.CreateTemp("", "db_misc_*.db"))
		dbFile.Close()
		db := must(Open(dbFile.Name(), basicSchema, Options{IsTesting: true}))

		if db.Bolt() == nil {
			t.Fatalf("Bolt() = nil for bolt-backed DB")
		}
		_ = db.Size()

		rtx := db.BeginRead()
		desc := db.DescribeOpenTxns()
		if !strings.Contains(desc, "OPEN TRANSACTIONS") {
			t.Fatalf("DescribeOpenTxns() missing expected text, got: %q", desc)
		}
		rtx.Close()
		if got := db.DescribeOpenTxns(); !strings.Contains(got, "NO OPEN TRANSACTIONS") {
			t.Fatalf("DescribeOpenTxns() = %q, wanted NO OPEN TRANSACTIONS", got)
		}

		if Foo() != 42 {
			t.Fatalf("Foo() = %d, wanted 42", Foo())
		}

		var safe atomic.Int32
		db.CloseWithSafeToQuitCallback(func() { safe.Add(1) })
		if safe.Load() != 1 {
			t.Fatalf("safeToQuit called %d times, wanted 1", safe.Load())
		}
	})
}

