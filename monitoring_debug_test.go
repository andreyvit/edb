package edb

import (
	"reflect"
	"strings"
	"testing"
)

func TestTableStatsAndDump(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	u2 := &User{ID: 2, Name: "bar", Email: "bar@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1, u2)
		tx.KVPutRaw(kubets, x("01"), []byte{1})
		tx.KVPutRaw(kubets, x("02"), []byte{2})
	})

	db.Read(func(tx *Tx) {
		ts := tx.TableStats(usersTable)
		if ts.Rows != 2 || ts.IndexRows == 0 {
			t.Fatalf("TableStats = %+v, wanted Rows=2 and IndexRows>0", ts)
		}
		_ = ts.TotalSize()
		_ = ts.TotalAlloc()

		kts := tx.KVTableStats(kubets)
		if kts.Rows != 2 {
			t.Fatalf("KVTableStats.Rows = %d, wanted 2", kts.Rows)
		}

		if got := loggableRowVal(usersTable, reflect.Value{}); got != "<none>" {
			t.Fatalf("loggableRowVal(invalid) = %q, wanted <none>", got)
		}
	})
}

func TestDumpFlagsAndDump(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) { Put(tx, u1) })

	db.Read(func(tx *Tx) {
		if !DumpTableHeaders.Contains(DumpTableHeaders) || DumpTableHeaders.Contains(DumpRows) {
			t.Fatalf("DumpFlags.Contains returned unexpected results")
		}

		out := tx.Dump(DumpAll)
		if !strings.Contains(out, "Users") || !strings.Contains(out, "foo@example.com") {
			t.Fatalf("Dump output missing expected substrings; got:\n%s", out)
		}
	})
}

func TestRpadf(t *testing.T) {
	got := rpadf('.', "%s", "x")
	if len(got) != 80 || !strings.HasPrefix(got, "x") {
		t.Fatalf("rpadf returned %q (len=%d), wanted len=80 and prefix x", got, len(got))
	}
}
