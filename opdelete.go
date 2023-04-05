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
	ok := tx.deleteByKeyRaw(tbl, keyRaw)
	if tx.db.verbose {
		if ok {
			tx.db.logf("db: DELETE %s/%v", tbl.name, keyVal.Interface())
		} else {
			tx.db.logf("db: DELETE.NOOP %s/%v", tbl.name, keyVal.Interface())
		}
	}
	return ok
}

func (tx *Tx) DeleteByKeyRaw(tbl *Table, keyRaw []byte) bool {
	ok := tx.deleteByKeyRaw(tbl, keyRaw)
	if tx.db.verbose {
		if ok {
			tx.db.logf("db: DELETE %s/%x", tbl.name, keyRaw)
		} else {
			tx.db.logf("db: DELETE.NOOP %s/%x", tbl.name, keyRaw)
		}
	}
	return true
}

func (tx *Tx) deleteByKeyRaw(tbl *Table, keyRaw []byte) bool {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	ts := tx.db.tableState(tbl)

	c := dataBuck.Cursor()
	k, v := c.Seek(keyRaw)
	if !bytes.Equal(k, keyRaw) {
		return false
	}

	var old value
	err := old.decode(v)
	if err != nil {
		panic(tableErrf(tbl, nil, keyRaw, err, "decoding old value"))
	}

	tx.markWritten()

	del := prepareToDeleteIndexEntries(tableBuck, ts)
	decodeIndexKeys(old.Index, del)

	ensure(c.Delete())
	return true
}
