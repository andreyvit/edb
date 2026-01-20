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
	dataBuck := nonNil(tx.stx.Bucket(tbl.name, dataBucketName))
	bs := dataBuck.Stats()
	result := TableStats{
		Rows:      int64(bs.KeyN),
		DataSize:  bs.LeafInuse,
		DataAlloc: bs.TotalAlloc(),
	}

	for _, idx := range tbl.indices {
		indexBuck := nonNil(tx.stx.Bucket(tbl.name, idx.buck))
		bs = indexBuck.Stats()
		result.IndexRows += int64(bs.KeyN)
		result.IndexSize += bs.LeafInuse
		result.IndexAlloc += bs.TotalAlloc()
	}

	return result
}

func (tx *Tx) KVTableStats(tbl *KVTable) TableStats {
	dataBuck := nonNil(tx.stx.Bucket(tbl.name, ""))
	bs := dataBuck.Stats()
	result := TableStats{
		Rows:      int64(bs.KeyN),
		DataSize:  bs.LeafInuse,
		DataAlloc: bs.TotalAlloc(),
	}

	for _, idx := range tbl.indices {
		indexBuck := nonNil(tx.stx.Bucket(idx.idxBuck, ""))
		bs = indexBuck.Stats()
		result.IndexRows += int64(bs.KeyN)
		result.IndexSize += bs.LeafInuse
		result.IndexAlloc += bs.TotalAlloc()
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
