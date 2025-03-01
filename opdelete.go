package edb

import (
	"bytes"
	"reflect"
)

func DeleteAll(c RawCursor) int {
	tbl, tx := c.Table(), c.Tx()
	var count int
	keys := AllRawKeys(c)
	for _, key := range keys {
		if tx.DeleteByKeyRaw(tbl, key) {
			count++
		}
	}
	return count
}

func DeleteRow[Row any](txh Txish, row *Row) bool {
	tx := txh.DBTx()
	rowVal := reflect.ValueOf(row)
	tbl := tableOf[Row](tx)
	keyVal := tbl.RowKeyVal(rowVal)
	return tx.DeleteByKeyVal(tbl, keyVal)
}

func DeleteByKey[Row any](txh Txish, key any) bool {
	tx := txh.DBTx()
	tbl := tableOf[Row](tx)
	return tx.DeleteByKey(tbl, key)
}

func (tx *Tx) DeleteByKey(tbl *Table, key any) bool {
	return tx.DeleteByKeyVal(tbl, reflect.ValueOf(key))
}

func (tx *Tx) DeleteByKeyVal(tbl *Table, keyVal reflect.Value) bool {
	keyVal = tbl.ensureCorrectKeyType(keyVal)
	keyBuf := keyBytesPool.Get().([]byte)
	keyRaw := tbl.encodeKeyVal(keyBuf, keyVal, false)
	defer keyBytesPool.Put(keyBuf[:0])
	ok := tx.deleteByKeyRaw(tbl, keyRaw, keyVal)
	if tx.isVerboseLoggingEnabled() {
		if ok {
			tx.db.logf("db: DELETE %s/%v", tbl.name, keyVal.Interface())
		} else {
			tx.db.logf("db: DELETE.NOOP %s/%v", tbl.name, keyVal.Interface())
		}
	}
	return ok
}

func (tx *Tx) DeleteByKeyRaw(tbl *Table, keyRaw []byte) bool {
	ok := tx.deleteByKeyRaw(tbl, keyRaw, reflect.Value{})
	if tx.isVerboseLoggingEnabled() {
		if ok {
			tx.db.logf("db: DELETE %s/%x", tbl.name, keyRaw)
		} else {
			tx.db.logf("db: DELETE.NOOP %s/%x", tbl.name, keyRaw)
		}
	}
	return true
}

func (tx *Tx) deleteByKeyRaw(tbl *Table, keyRaw []byte, keyValIfKnown reflect.Value) bool {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	ts := tx.db.tableState(tbl)

	c := dataBuck.Cursor()
	k, v := c.Seek(keyRaw)
	if !bytes.Equal(k, keyRaw) {
		return false
	}

	var old value
	decodeTableValue(&old, tbl, keyRaw, v, false)

	tx.markWritten()

	del := prepareToDeleteIndexEntries(tableBuck, ts)
	decodeIndexKeys(old.Index, del)

	if opts := tx.changeOptions[tbl]; opts.Contains(ChangeFlagNotify) && tx.changeHandler != nil {
		chg := Change{
			table:  tbl,
			op:     OpDelete,
			rawKey: keyRaw,
		}
		if opts.Contains(ChangeFlagIncludeRow) {
			var err error
			chg.rowVal, chg.keyVal, _, err = decodeTableRowFromValue(&old, tbl, keyRaw, tx)
			if err != nil {
				chg.rowVal = reflect.Value{}
				tx.db.logf("db: DELETE %s/%v: cannot decode old row: %v", tbl.name, keyRaw, err)
			}
		} else if opts.Contains(ChangeFlagIncludeKey) {
			if keyValIfKnown.IsZero() {
				chg.keyVal = tbl.DecodeKeyVal(keyRaw)
			} else {
				chg.keyVal = keyValIfKnown
			}
		}
		tx.changeHandler(tx, &chg)
	}

	ensure(c.Delete())
	return true
}

func (tx *Tx) UnsafeDeleteByKeyRawSkippingIndex(tbl *Table, keyRaw []byte) bool {
	ok := tx.unsafeDeleteByKeyRawSkippingIndex(tbl, keyRaw)
	if tx.isVerboseLoggingEnabled() {
		if ok {
			tx.db.logf("db: UNSAFE_DELETE_SKIPIDX %s/%x", tbl.name, keyRaw)
		} else {
			tx.db.logf("db: UNSAFE_DELETE_SKIPIDX.NOOP %s/%x", tbl.name, keyRaw)
		}
	}
	return true
}

func (tx *Tx) unsafeDeleteByKeyRawSkippingIndex(tbl *Table, keyRaw []byte) bool {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))

	c := dataBuck.Cursor()
	k, _ := c.Seek(keyRaw)
	if !bytes.Equal(k, keyRaw) {
		return false
	}

	tx.markWritten()
	ensure(c.Delete())
	return true
}
