package edb

import (
	"strings"
	"testing"
)

func TestTxReuseBuffersScanRetention(t *testing.T) {
	const limit = 64 * 1024
	const scans = 2 * limit / keyBufferCapacityInitial
	schema := &Schema{}
	unique := AddIndex[string]("unique").Unique()
	group := AddIndex[string]("group")
	AddTable(schema, "scan_retention", 1, func(row *txReuseGrowthRow, ib *IndexBuilder) {
		for _, entry := range row.Entries {
			ib.Add(unique, entry)
			ib.Add(group, entry)
		}
	}, nil, []*Index{unique, group})
	padding := strings.Repeat("x", keyBufferCapacityInitial+1)
	alice := &txReuseGrowthRow{Key: "alice/" + padding, Entries: []string{"alice/" + padding}}
	bob := &txReuseGrowthRow{Key: "bob/" + padding, Entries: []string{"bob/" + padding}}
	for _, source := range []struct {
		name string
		scan func(*Tx, *txReuseGrowthRow) Cursor[txReuseGrowthRow]
	}{
		{"table", func(tx *Tx, row *txReuseGrowthRow) Cursor[txReuseGrowthRow] {
			return TableScan[txReuseGrowthRow](tx, ExactScan(row.Key))
		}},
		{"index", func(tx *Tx, row *txReuseGrowthRow) Cursor[txReuseGrowthRow] {
			return ExactIndexScan[txReuseGrowthRow](tx, unique, row.Entries[0])
		}},
		{"primary_key_range", func(tx *Tx, row *txReuseGrowthRow) Cursor[txReuseGrowthRow] {
			return IndexScan[txReuseGrowthRow](tx, group, ExactScan(row.Entries[0]).PrimaryKeyRange(row.Key, row.Key, true, true))
		}},
	} {
		t.Run(source.name, func(t *testing.T) {
			db := openTxReuseDB(t, schema, true)
			db.Write(func(tx *Tx) { Put(tx, alice, bob) })
			tx := db.BeginRead()
			defer tx.Close()
			var overflow Cursor[txReuseGrowthRow]
			for range scans {
				tracked := len(tx.indexKeyBufs)
				cursor := source.scan(tx, alice)
				if overflow.RawCursor == nil && len(tx.indexKeyBufs) == tracked {
					overflow = cursor
				} else {
					deepEqual(t, All(cursor), []*txReuseGrowthRow{alice})
				}
				assertTxScanBuffersBounded(t, tx, limit)
			}
			if overflow.RawCursor == nil || len(tx.indexKeyBufs) == 0 {
				t.Fatal("scans did not exercise both tracked and overflow buffers")
			}
			grown := false
			for _, buf := range tx.indexKeyBufs {
				grown = grown || cap(buf.data) > keyBufferCapacityInitial
			}
			if !grown {
				t.Fatal("scan buffers did not grow during encoding")
			}
			for range scans {
				deepEqual(t, All(source.scan(tx, bob)), []*txReuseGrowthRow{bob})
				assertTxScanBuffersBounded(t, tx, limit)
			}
			deepEqual(t, All(overflow), []*txReuseGrowthRow{alice})
			tx.Close()
			assertTxReuseReleased(t, tx)
		})
	}
}

func assertTxScanBuffersBounded(t *testing.T, tx *Tx, limit int) {
	t.Helper()
	var retained int
	for _, buf := range tx.indexKeyBufs {
		retained += cap(buf.data)
	}
	if retained > limit {
		t.Fatalf("open transaction retained %d bytes of scan buffers, limit %d", retained, limit)
	}
}
