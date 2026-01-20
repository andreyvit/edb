package edb

func (tx *Tx) Reindex(tbl *Table, idx *Index) {
	ts := tx.db.tableState(tbl)

	tx.reindexing = true
	defer func() {
		tx.reindexing = false
	}()

	for _, is := range ts.indexStates {
		if idx != nil && idx != is.index {
			continue
		}
		err := tx.stx.DeleteBucket(tbl.name, is.index.buck)
		if err != nil && err != ErrBucketNotFound {
			panic(err)
		}
		_ = must(tx.stx.CreateBucket(tbl.name, is.index.buck))
		is.Built = true
	}

	for c := tx.TableScan(tbl, FullScan()); c.Next(); {
		rowVal, _ := c.RowVal()
		tx.PutVal(tbl, rowVal)
	}

	ts.save(tx)
}
