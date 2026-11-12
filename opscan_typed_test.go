package edb

import (
	"iter"
	"slices"
	"testing"
)

func TestTypedScans(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			t.Run("named ID", func(t *testing.T) { testTypedScans(t, format, ID(1), ID(2), ID(3)) })
			t.Run("string", func(t *testing.T) { testTypedScans(t, format, "a", "b", "c") })
			t.Run("composite", func(t *testing.T) { testTypedScans(t, format, AB{1, 1}, AB{1, 2}, AB{2, 1}) })
		})
	}
}

func testTypedScans[Key comparable](t *testing.T, format StorageFormat, first, second, third Key) {
	db, rows, byName, byGroup := typedTestTable[Key](t, format)
	records := rows.Records()
	unique, group := records.bindIndex[string](byName), records.bindIndex[string](byGroup)
	want := []Key{first, second, third}
	db.Write(func(tx *Tx) {
		rows.Put(tx, &typedTestRow[Key]{third, "chris", "regular"})
		rows.Put(tx, &typedTestRow[Key]{first, "alice", "vip"})
		rows.Put(tx, &typedTestRow[Key]{second, "bob", "vip"})
	})
	db.Read(func(tx *Tx) {
		check := func(name string, got iter.Seq[Key], want []Key) {
			t.Helper()
			if got := slices.Collect(got); !slices.Equal(got, want) {
				t.Fatalf("%s: keys %v, want %v", name, got, want)
			}
		}
		check("scan", records.Scan(tx).Keys(), want)
		check("rows", rows.Scan(tx).Keys(), want)
		check("exact", records.Exact(tx, second).Keys(), want[1:2])
		check("range", records.Range(tx, first, second).Keys(), want[:2])
		check("from", records.From(tx, second).Keys(), want[1:])
		check("after", records.After(tx, second).Keys(), want[2:])
		check("through", records.Through(tx, second).Keys(), want[:2])
		check("before", records.Before(tx, second).Keys(), want[:1])
		check("reverse", records.Scan(tx).Reversed().Keys(), []Key{third, second, first})
		check("unique", unique.Exact(tx, "bob").Keys(), want[1:2])
		check("group", group.Exact(tx, "vip").Keys(), want[:2])
		check("missing", group.Exact(tx, "absent").Keys(), nil)
		check("index range", unique.Range(tx, "alice", "bob").Keys(), want[:2])
		check("index from", unique.From(tx, "bob").Keys(), want[1:])
		check("index after", unique.After(tx, "bob").Keys(), want[2:])
		check("index through", unique.Through(tx, "bob").Keys(), want[:2])
		check("index before", unique.Before(tx, "bob").Keys(), want[:1])
		check("index reverse", group.Exact(tx, "vip").Reversed().Keys(), []Key{second, first})
		for key, r := range records.Scan(tx).Entries() {
			if r.GetValue(1).AsString() != rows.Get(tx, key).Name {
				t.Fatal("record entry key disagrees with row", key)
			}
		}
		for key, row := range rows.Scan(tx).Entries() {
			if key != row.Key || records.Get(tx, key).GetValue(1).AsString() != row.Name {
				t.Fatal("row entry key disagrees with record", key)
			}
		}
		if got := len(slices.Collect(group.Exact(tx, "vip").Records())); got != 2 {
			t.Fatal("record count", got)
		}
		// Reusing even the same iterator restarts after an early break.
		entries := group.Exact(tx, "vip").Entries()
		native := rows.bindIndex[string](byGroup).Exact(tx, "vip").Rows()
		for range 2 {
			for key, r := range entries {
				if key != first || r.GetValue(1).AsString() != "alice" {
					t.Fatal("entries did not restart")
				}
				break
			}
			for row := range native {
				if row.Key != first {
					t.Fatal("rows did not restart")
				}
				break
			}
		}
	})
}

func TestTypedCompositePrefix(t *testing.T) {
	db, rows, _, _ := typedTestTable[AB](t, Fast2)
	records := rows.Records()
	index := records.bindIndex[AB](rows.Table().IndexNamed("key"))
	want := []AB{{1, 1}, {1, 2}}
	db.Write(func(tx *Tx) {
		for i, key := range []AB{{1, 1}, {1, 2}, {2, 1}} {
			rows.Put(tx, &typedTestRow[AB]{key, string(rune('a' + i)), "vip"})
		}
	})
	db.Read(func(tx *Tx) {
		for _, seq := range []iter.Seq[AB]{
			records.Prefix(tx, AB{1, 0}, 1).Keys(),
			rows.Prefix(tx, AB{1, 0}, 1).Keys(),
			index.Prefix(tx, AB{1, 0}, 1).Keys(),
			rows.bindIndex[AB](index.Index()).Prefix(tx, AB{1, 0}, 1).Keys(),
		} {
			if got := slices.Collect(seq); !slices.Equal(got, want) {
				t.Fatal("prefix", got)
			}
		}
		if key, r := index.Lookup(tx, AB{1, 2}); key != want[1] || r.GetValue(1).AsString() != "b" {
			t.Fatal("composite index lookup", key)
		}
	})
}

func TestTypedKeysDoNotDecodeRows(t *testing.T) {
	db, rows, _, byGroup := typedTestTable[ID](t, Fast2)
	db.Write(func(tx *Tx) {
		rows.Put(tx, &typedTestRow[ID]{1, "alice", "vip"})
		// Preserve the key and index entry while making payload access fail.
		putRawTableValue(t, tx, rows.Table(), rows.Table().EncodeKey(ID(1)), []byte{0xff})
	})
	db.Read(func(tx *Tx) {
		for _, keys := range []iter.Seq[ID]{rows.Scan(tx).Keys(), rows.Records().bindIndex[string](byGroup).Exact(tx, "vip").Keys()} {
			if got := slices.Collect(keys); !slices.Equal(got, []ID{1}) {
				t.Fatal("key scan required a valid payload", got)
			}
		}
		assertPanics(t, func() { rows.Records().Get(tx, 1) })
	})
}
