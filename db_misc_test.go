package edb

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/andreyvit/edb/kvo"
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
		if err := dbFile.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
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
	var emailIdx *IndexDef
	schema := &Schema{}
	DefineTable(schema, "Users", testIndexAccess[User](indexEmail), func(b *TableBuilder[ID]) {
		emailIdx = b.AddIndex[string]("Email", nil, IndexOptDeprecatedUnique).IndexDef()
		b.SetIndexer(func(key ID, row kvo.Record, ib *IndexBuilder) {
			ib.Add(emailIdx, row.Get(indexEmail))
		})
	}).TableDef()

	db := must(Open(path, schema, Options{IsTesting: true}))
	defer db.Close()

	db.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "Alice", Email: "alice@example.com"})
	})

	return path
}

func reopenUsersByAddedNameIndex(t testing.TB, path string, opts ...any) []ID {
	t.Helper()

	var emailIdx *IndexDef
	var nameIdx *IndexDef
	schema := &Schema{}
	DefineTable(schema, "Users", testIndexAccess[User](indexEmail, indexName), func(b *TableBuilder[ID]) {
		emailIdx = b.AddIndex[string]("Email", nil, IndexOptDeprecatedUnique).IndexDef()
		nameIdx = b.AddIndex[string]("Name", nil, opts...).IndexDef()
		b.SetIndexer(func(key ID, row kvo.Record, ib *IndexBuilder) {
			ib.Add(emailIdx, row.Get(indexEmail))
			ib.Add(nameIdx, row.Get(indexName))
		})
	}).TableDef()

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

	var emailIdx *IndexDef
	var nameIdx *IndexDef
	var nameEmailIdx *IndexDef
	schema := &Schema{}
	DefineTable(schema, "Users", testIndexAccess[User](indexEmail, indexName), func(b *TableBuilder[ID]) {
		emailIdx = b.AddIndex[string]("Email", nil, IndexOptDeprecatedUnique).IndexDef()
		nameIdx = b.AddIndex[string]("Name", nil, IndexInitialFillModeBlocking).IndexDef()
		nameEmailIdx = b.AddIndex[string]("NameEmail", nil, IndexInitialFillModeSkip).IndexDef()
		b.SetIndexer(func(key ID, row kvo.Record, ib *IndexBuilder) {
			ib.Add(emailIdx, row.Get(indexEmail))
			ib.Add(nameIdx, row.Get(indexName))
			ib.Add(nameEmailIdx, row.Get(indexName)+"|"+row.Get(indexEmail))
		})
	}).TableDef()

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

func TestMemBucketDeleteAndRecreate(t *testing.T) {
	s := newMemStorage()
	defer s.Close()
	tx := must(s.BeginTx(true))
	defer tx.Rollback()
	must(tx.CreateBucket("root", "child"))
	bucket := tx.Bucket("root", "child")
	ensure(bucket.Put([]byte("k"), []byte("old")))
	ensure(tx.DeleteBucket("root", "child"))
	if tx.Bucket("root", "child") != nil {
		t.Fatal("deleted bucket still visible to its writer")
	}
	must(tx.CreateBucket("root", "child"))
	if got := tx.Bucket("root", "child").Get([]byte("k")); got != nil {
		t.Fatalf("recreated bucket contains old data: %q", got)
	}
}
