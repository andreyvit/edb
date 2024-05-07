package edb

import (
	"encoding/json"
	"fmt"
	"strings"

	"go.etcd.io/bbolt"
)

type DumpFlags uint64

const (
	DumpTableHeaders = DumpFlags(1 << iota)
	DumpRows
	DumpStats
	DumpIndices
	DumpIndexRows

	DumpAll = DumpFlags(0xFFFFFFFFFFFFFFFF)

	indentStep = "  "
)

var (
	dumpSep1 = strings.Repeat("=", 80)
	dumpSep2 = strings.Repeat("-", 60)
)

func (f DumpFlags) Contains(v DumpFlags) bool {
	return (f & v) == v
}

func (tx *Tx) Dump(f DumpFlags) string {
	var buf strings.Builder
	for i, tbl := range tx.db.schema.tables {
		tx.dumpTable(&buf, "", f, tbl, i+1, len(tx.db.schema.tables))
	}
	return buf.String()
}

func (tx *Tx) dumpTable(w *strings.Builder, prefix string, f DumpFlags, tbl *Table, tblPos, tblCount int) {
	prefix = prefix + tbl.Name()
	s := tx.TableStats(tbl)
	ts := tx.db.tableState(tbl)

	if f.Contains(DumpTableHeaders) {
		fmt.Fprintln(w, dumpSep1)
		fmt.Fprintf(w, "%s (%d rows)\n", prefix, s.Rows)
	}
	if f.Contains(DumpStats) {
		fmt.Fprintf(w, "%s.stats: index_rows = %d, data_size = %d, data_alloc = %d, index_size = %d, index_alloc = %d, total_alloc = %d\n", prefix, s.IndexRows, s.DataSize, s.DataAlloc, s.IndexSize, s.IndexAlloc, s.TotalAlloc())
	}

	rootB := tbl.rootBucketIn(tx.btx)

	if f.Contains(DumpRows) {
		if f.Contains(DumpStats) {
			fmt.Fprintln(w, dumpSep2)
		}
		c := tbl.dataBucketIn(rootB).Cursor()
		var rowPos int
		for k, v := c.First(); k != nil; k, v = c.Next() {
			rowPos++
			tx.dumpRow(w, prefix, f, tbl, rowPos, k, v, tx)
		}
	}

	if f.Contains(DumpIndices) {
		for _, idx := range tbl.indices {
			tx.dumpIndex(w, prefix, f, idx, ts, rootB)
		}
	}
}

func (tx *Tx) dumpIndex(w *strings.Builder, prefix string, f DumpFlags, idx *Index, ts *tableState, rootB *bbolt.Bucket) {
	fmt.Fprintln(w, dumpSep2)
	prefix = prefix + ".i." + idx.ShortName()
	is := ts.indexStates[idx.pos]

	fmt.Fprintf(w, "%s (0x%x)%s\n", prefix, is.IndexOrdinal, map[bool]string{false: " PENDING", true: ""}[is.Built])

	if f.Contains(DumpIndexRows) {
		c := idx.bucketIn(rootB).Cursor()
		var rowPos int
		for k, v := c.First(); k != nil; k, v = c.Next() {
			rowPos++
			tx.dumpIndexRow(w, prefix, f, idx, rowPos, k, v)
		}
	}
}

func (tx *Tx) dumpRow(w *strings.Builder, prefix string, f DumpFlags, tbl *Table, rowPos int, k, v []byte, migratorTx *Tx) {
	rowVal, rowMeta, err := decodeTableRow(tbl, k, v, migratorTx)
	if err != nil {
		fmt.Fprintf(w, "%s.%d = (m%d s%d) ** ERROR: %v\n", prefix, rowPos, rowMeta.ModCount, rowMeta.SchemaVer, err)
		return
	}
	fmt.Fprintf(w, "%s.%d = (m%d s%d) %s\n", prefix, rowPos, rowMeta.ModCount, rowMeta.SchemaVer, must(json.Marshal(rowVal.Interface())))
}

func (tx *Tx) dumpIndexRow(w *strings.Builder, prefix string, f DumpFlags, idx *Index, rowPos int, k, v []byte) {
	indexKeyTup, keyRaw := decodeIndexRow(idx, k, v)
	indexKeyStr := idx.keyTupleToString(indexKeyTup)
	keyStr := idx.table.RawKeyString(keyRaw)
	fmt.Fprintf(w, "%s.%d: %s => %s\n", prefix, rowPos, indexKeyStr, keyStr)
}

func rpadf(pad rune, format string, args ...any) string {
	s := fmt.Sprintf(format, args...)
	return rpad(s, 80, pad)
}
