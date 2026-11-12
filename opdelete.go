package edb

import (
	"bytes"
	"reflect"

	"github.com/andreyvit/edb/kvo"
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

func (tx *Tx) DeleteByKey(tblArg AnyTable, key any) bool {
	tbl := tblArg.TableDef()

	return tx.DeleteByKeyVal(tbl, reflect.ValueOf(key))
}

// DeleteRecord derives the key from an explicitly declared model property.
// Use DeleteByKey when the record does not store its primary key.
func (tx *Tx) DeleteRecord(tblArg AnyTable, row kvo.Record) bool {
	tbl := tblArg.TableDef()

	return tx.DeleteByKeyVal(tbl, tbl.recordKey(row))
}

func (tx *Tx) DeleteByKeyVal(tblArg AnyTable, keyVal reflect.Value) bool {
	tbl := tblArg.TableDef()

	keyVal = tbl.ensureCorrectKeyType(keyVal)
	keyBuf := acquireKeyBuffer()
	keyRaw := tbl.encodeKeyVal(keyBuf.data, keyVal, false)
	keyBuf.data = keyRaw[:0]
	defer releaseKeyBuffer(keyBuf)
	ok := tx.deleteByKeyRaw(tbl, keyRaw)
	if tx.isVerboseLoggingEnabled() {
		if ok {
			tx.db.logf("db: DELETE %s/%v", tbl.name, keyVal.Interface())
		} else {
			tx.db.logf("db: DELETE.NOOP %s/%v", tbl.name, keyVal.Interface())
		}
	}
	return ok
}

func (tx *Tx) DeleteByKeyRaw(tblArg AnyTable, keyRaw []byte) bool {
	tbl := tblArg.TableDef()

	ok := tx.deleteByKeyRaw(tbl, keyRaw)
	if tx.isVerboseLoggingEnabled() {
		if ok {
			tx.db.logf("db: DELETE %s/%x", tbl.name, keyRaw)
		} else {
			tx.db.logf("db: DELETE.NOOP %s/%x", tbl.name, keyRaw)
		}
	}
	return ok
}

func (tx *Tx) deleteByKeyRaw(tbl *TableDef, keyRaw []byte) bool {
	dataBuck := nonNil(tx.stx.Bucket(tbl.name, dataBucketName))
	ts := tx.db.tableState(tbl)

	c := dataBuck.Cursor()
	k, v := c.Seek(keyRaw)
	if !bytes.Equal(k, keyRaw) {
		return false
	}

	var old value
	decodeTableValue(&old, tbl, keyRaw, v, false)

	tx.markWritten()

	del := prepareToDeleteIndexEntries(tx, ts)
	decodeIndexKeys(old.Index, del)

	ensure(c.Delete())
	tx.notifyChange(tbl, OpDelete, keyRaw, &old, nil)

	return true
}

func (tx *Tx) UnsafeDeleteByKeyRawSkippingIndex(tblArg AnyTable, keyRaw []byte) bool {
	tbl := tblArg.TableDef()

	ok := tx.unsafeDeleteByKeyRawSkippingIndex(tbl, keyRaw)
	if tx.isVerboseLoggingEnabled() {
		if ok {
			tx.db.logf("db: UNSAFE_DELETE_SKIPIDX %s/%x", tbl.name, keyRaw)
		} else {
			tx.db.logf("db: UNSAFE_DELETE_SKIPIDX.NOOP %s/%x", tbl.name, keyRaw)
		}
	}
	return ok
}

func (tx *Tx) unsafeDeleteByKeyRawSkippingIndex(tbl *TableDef, keyRaw []byte) bool {
	dataBuck := nonNil(tx.stx.Bucket(tbl.name, dataBucketName))

	c := dataBuck.Cursor()
	k, _ := c.Seek(keyRaw)
	if !bytes.Equal(k, keyRaw) {
		return false
	}

	tx.markWritten()
	ensure(c.Delete())
	return true
}
