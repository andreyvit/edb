package edb

import (
	"os"
	"path/filepath"
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

func TestDB_BoltReopenAddedIndexInitialFillModes(t *testing.T) {
	isempty(t, reopenUsersByAddedNameIndex(t, legacyUsersDB(t)))
	isempty(t, reopenUsersByAddedNameIndex(t, legacyUsersDB(t), IndexInitialFillModeSkip))
	deepEqual(t, reopenUsersByAddedNameIndex(t, legacyUsersDB(t), IndexInitialFillModeBlocking), []ID{1})
	isempty(t, reopenUsersByAddedNameIndex(t, legacyUsersDB(t), IndexOptSkipInitialFill))
}

func TestDB_BoltReopenBlockingFillAlsoPopulatesSkipIndex(t *testing.T) {
	gotBlocking, gotSkipped := reopenUsersByMixedFillModes(t, legacyUsersDB(t))
	deepEqual(t, gotBlocking, []ID{1})
	deepEqual(t, gotSkipped, []ID{1})
}

func legacyUsersDB(t testing.TB) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "users.db")
	emailIdx := AddIndex[string]("Email").Unique()
	schema := &Schema{}
	AddTable(schema, "Users", 1, func(row *User, ib *IndexBuilder) {
		ib.Add(emailIdx, row.Email)
	}, nil, []*Index{emailIdx})

	db := must(Open(path, schema, Options{IsTesting: true}))
	defer db.Close()

	db.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "Alice", Email: "alice@example.com"})
	})

	return path
}

func reopenUsersByAddedNameIndex(t testing.TB, path string, opts ...any) []ID {
	t.Helper()

	emailIdx := AddIndex[string]("Email").Unique()
	nameIdx := AddIndex[string]("Name", opts...)
	schema := &Schema{}
	AddTable(schema, "Users", 1, func(row *User, ib *IndexBuilder) {
		ib.Add(emailIdx, row.Email)
		ib.Add(nameIdx, row.Name)
	}, nil, []*Index{emailIdx, nameIdx})

	db := must(Open(path, schema, Options{IsTesting: true}))
	defer db.Close()

	var ids []ID
	db.Read(func(tx *Tx) {
		for _, row := range All(IndexScan[User](tx, nameIdx, ExactScan("Alice"))) {
			ids = append(ids, row.ID)
		}
	})
	return ids
}

func reopenUsersByMixedFillModes(t testing.TB, path string) ([]ID, []ID) {
	t.Helper()

	emailIdx := AddIndex[string]("Email").Unique()
	nameIdx := AddIndex[string]("Name", IndexInitialFillModeBlocking)
	nameEmailIdx := AddIndex[string]("NameEmail", IndexInitialFillModeSkip)
	schema := &Schema{}
	AddTable(schema, "Users", 1, func(row *User, ib *IndexBuilder) {
		ib.Add(emailIdx, row.Email)
		ib.Add(nameIdx, row.Name)
		ib.Add(nameEmailIdx, row.Name+"|"+row.Email)
	}, nil, []*Index{emailIdx, nameIdx, nameEmailIdx})

	db := must(Open(path, schema, Options{IsTesting: true}))
	defer db.Close()

	var blocking []ID
	var skipped []ID
	db.Read(func(tx *Tx) {
		for _, row := range All(IndexScan[User](tx, nameIdx, ExactScan("Alice"))) {
			blocking = append(blocking, row.ID)
		}
		for _, row := range All(IndexScan[User](tx, nameEmailIdx, ExactScan("Alice|alice@example.com"))) {
			skipped = append(skipped, row.ID)
		}
	})
	return blocking, skipped
}
