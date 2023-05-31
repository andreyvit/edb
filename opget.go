package edb

import "reflect"

func Reload[Row any](txh Txish, row *Row) *Row {
	tx := txh.DBTx()
	tbl := tx.Schema().TableByRow((*Row)(nil))
	rowVal := reflect.ValueOf(row)
	keyVal := tbl.RowKeyVal(rowVal)
	newRowVal, _ := tx.getRowValByKeyVal(tbl, keyVal, true)
	if !newRowVal.IsValid() {
		return nil
	}
	return newRowVal.Interface().(*Row)
}

func Get[Row any](txh Txish, key any) *Row {
	tx := txh.DBTx()
	tbl := tx.Schema().TableByRow((*Row)(nil))
	row, _ := tx.Get(tbl, key)
	if row == nil {
		return nil
	}
	return row.(*Row)
}

func Exists[Row any](txh Txish, key any) bool {
	tx := txh.DBTx()
	tbl := tx.Schema().TableByRow((*Row)(nil))
	return tx.Exists(tbl, key)
}

func (tx *Tx) Get(tbl *Table, key any) (any, ValueMeta) {
	return tx.GetByKeyVal(tbl, reflect.ValueOf(key))
}

func (tx *Tx) GetByKeyVal(tbl *Table, keyVal reflect.Value) (any, ValueMeta) {
	rowVal, rowMeta := tx.getRowValByKeyVal(tbl, keyVal, true)
	if !rowVal.IsValid() {
		return nil, ValueMeta{}
	}
	return rowVal.Interface(), rowMeta
}

func (tx *Tx) GetMeta(tbl *Table, key any) ValueMeta {
	return tx.GetMetaByKeyVal(tbl, reflect.ValueOf(key))
}

func (tx *Tx) GetMetaByKeyVal(tbl *Table, keyVal reflect.Value) ValueMeta {
	_, rowMeta := tx.getRowValByKeyVal(tbl, keyVal, false)
	return rowMeta
}

func (tx *Tx) Exists(tbl *Table, key any) bool {
	return tx.existsByKeyVal(tbl, reflect.ValueOf(key))
}

func (tx *Tx) getRowValByKeyVal(tbl *Table, keyVal reflect.Value, includeRow bool) (reflect.Value, ValueMeta) {
	keyVal = tbl.ensureCorrectKeyType(keyVal)
	keyBuf := keyBytesPool.Get().([]byte)
	keyRaw := tbl.encodeKeyVal(keyBuf, keyVal, true)
	defer keyBytesPool.Put(keyBuf[:0])
	val, valMeta := tx.getRowValByRawKey(tbl, keyRaw, includeRow)
	if tx.db.verbose {
		if includeRow {
			if val.IsValid() {
				tx.db.logf("db: GET %s/%v => %v", tbl.name, keyVal.Interface(), loggableRowVal(tbl, val))
			} else {
				tx.db.logf("db: GET.NOTFOUND %s/%v", tbl.name, keyVal.Interface())
			}
		} else {
			if val.IsValid() {
				tx.db.logf("db: META %s/%v => %v", tbl.name, keyVal.Interface(), loggableRowVal(tbl, val))
			} else {
				tx.db.logf("db: META.NOTFOUND %s/%v", tbl.name, keyVal.Interface())
			}
		}
	}
	return val, valMeta
}

func (tx *Tx) existsByKeyVal(tbl *Table, keyVal reflect.Value) bool {
	keyVal = tbl.ensureCorrectKeyType(keyVal)
	keyBuf := keyBytesPool.Get().([]byte)
	keyRaw := tbl.encodeKeyVal(keyBuf, keyVal, true)
	defer keyBytesPool.Put(keyBuf[:0])
	found := (tx.getRawByRawKey(tbl, keyRaw) != nil)
	if tx.db.verbose {
		tx.db.logf("db: EXISTS.%s %s/%v", map[bool]string{false: "NO", true: "YES"}[found], tbl.name, keyVal.Interface())
	}
	return found
}

func (tx *Tx) getRowValByRawKey(tbl *Table, keyRaw []byte, includeRow bool) (reflect.Value, ValueMeta) {
	valueRaw := tx.getRawByRawKey(tbl, keyRaw)
	if valueRaw == nil {
		return reflect.Value{}, ValueMeta{}
	}

	if includeRow {
		rowVal, rowMeta := decodeTableRow(tbl, keyRaw, valueRaw, tx)
		return rowVal, rowMeta
	} else {
		vle := decodeTableValue(tbl, keyRaw, valueRaw)
		return reflect.Value{}, vle.ValueMeta()
	}
}

func (tx *Tx) getRawByRawKey(tbl *Table, keyRaw []byte) []byte {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	return dataBuck.Get(keyRaw)
}
