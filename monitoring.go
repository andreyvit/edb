package edb

import (
	"encoding/json"
	"reflect"
)

type TableStats struct {
	Rows      int
	IndexRows int

	DataSize   int
	DataAlloc  int
	IndexSize  int
	IndexAlloc int
}

func (ts *TableStats) TotalSize() int {
	return ts.DataSize + ts.IndexSize
}

func (ts *TableStats) TotalAlloc() int {
	return ts.DataAlloc + ts.IndexAlloc
}

func (tx *Tx) TableStats(tbl *Table) TableStats {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))

	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	bs := dataBuck.Stats()
	result := TableStats{
		Rows:      bs.KeyN,
		DataSize:  bs.LeafInuse,
		DataAlloc: bs.BranchAlloc + bs.LeafAlloc,
	}

	for _, idx := range tbl.indices {
		indexBuck := nonNil(tableBuck.Bucket(idx.buck.Raw()))
		bs = indexBuck.Stats()
		result.IndexRows += bs.KeyN
		result.IndexSize += bs.LeafInuse
		result.IndexAlloc += bs.BranchAlloc + bs.LeafAlloc
	}

	return result
}

func loggableRowVal(tbl *Table, rowVal reflect.Value) string {
	if !rowVal.IsValid() {
		return "<none>"
	}
	if tbl.suppressContent {
		return "<suppressed>"
	}
	return string(must(json.Marshal(rowVal.Interface())))
}

func loggableVal(rowVal reflect.Value) string {
	if !rowVal.IsValid() {
		return "<none>"
	}
	return string(must(json.Marshal(rowVal.Interface())))
}
