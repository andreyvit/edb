package edb

import (
	"encoding/json"
	"reflect"
)

type TableStats struct {
	Rows      int64
	IndexRows int64

	DataSize   int64
	DataAlloc  int64
	IndexSize  int64
	IndexAlloc int64
}

func (ts *TableStats) TotalSize() int64 {
	return ts.DataSize + ts.IndexSize
}

func (ts *TableStats) TotalAlloc() int64 {
	return ts.DataAlloc + ts.IndexAlloc
}

func (tx *Tx) TableStats(tbl *Table) TableStats {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))

	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	bs := dataBuck.Stats()
	result := TableStats{
		Rows:      int64(bs.KeyN),
		DataSize:  int64(bs.LeafInuse),
		DataAlloc: int64(bs.BranchAlloc + bs.LeafAlloc),
	}

	for _, idx := range tbl.indices {
		indexBuck := nonNil(tableBuck.Bucket(idx.buck.Raw()))
		bs = indexBuck.Stats()
		result.IndexRows += int64(bs.KeyN)
		result.IndexSize += int64(bs.LeafInuse)
		result.IndexAlloc += int64(bs.BranchAlloc + bs.LeafAlloc)
	}

	return result
}

func (tx *Tx) KVTableStats(tbl *KVTable) TableStats {
	dataBuck := nonNil(tx.btx.Bucket(tbl.dataBuck.Raw()))

	bs := dataBuck.Stats()
	result := TableStats{
		Rows:      int64(bs.KeyN),
		DataSize:  int64(bs.LeafInuse),
		DataAlloc: int64(bs.BranchAlloc + bs.LeafAlloc),
	}

	for _, idx := range tbl.indices {
		indexBuck := nonNil(tx.btx.Bucket(idx.idxBuck.Raw()))
		bs = indexBuck.Stats()
		result.IndexRows += int64(bs.KeyN)
		result.IndexSize += int64(bs.LeafInuse)
		result.IndexAlloc += int64(bs.BranchAlloc + bs.LeafAlloc)
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
