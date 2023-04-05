package edb

import "go.etcd.io/bbolt"

func (tx *Tx) Reindex(tbl *Table, idx *Index) {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	ts := tx.db.tableState(tbl)

	tx.reindexing = true
	defer func() {
		tx.reindexing = false
	}()

	for _, is := range ts.indexStates {
		if idx != nil && idx != is.index {
			continue
		}
		err := tableBuck.DeleteBucket(is.index.buck.Raw())
		if err != nil && err != bbolt.ErrBucketNotFound {
			panic(err)
		}
		_ = must(tableBuck.CreateBucketIfNotExists(is.index.buck.Raw()))
		is.Built = true
	}

	for c := tx.TableScan(tbl, FullScan()); c.Next(); {
		rowVal, _ := c.RowVal()
		tx.PutVal(tbl, rowVal)
	}

	ts.save(tx)
}
