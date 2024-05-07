package edb

import (
	"reflect"
)

func Reload[Row any](txh Txish, row *Row) *Row {
	tx := txh.DBTx()
	tbl := tx.Schema().TableByRow((*Row)(nil))
	rowVal := reflect.ValueOf(row)
	keyVal := tbl.RowKeyVal(rowVal)
	newRowVal, _, err := tx.getRowValByKeyVal(tbl, keyVal, true)
	if err != nil {
		panic(err)
	}
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

func GetByKeyRaw[Row any](txh Txish, keyRaw []byte) *Row {
	tx := txh.DBTx()
	tbl := tx.Schema().TableByRow((*Row)(nil))
	row, _ := tx.GetByKeyRaw(tbl, keyRaw)
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

func (tx *Tx) TryGet(tbl *Table, key any) (any, ValueMeta, error) {
	return tx.TryGetByKeyVal(tbl, reflect.ValueOf(key))
}

func (tx *Tx) GetByKeyVal(tbl *Table, keyVal reflect.Value) (any, ValueMeta) {
	if tbl == nil {
		panic("tbl == nil")
	}
	rowVal, rowMeta, err := tx.getRowValByKeyVal(tbl, keyVal, true)
	if err != nil {
		panic(err)
	}
	if !rowVal.IsValid() {
		return nil, ValueMeta{}
	}
	return rowVal.Interface(), rowMeta
}

func (tx *Tx) TryGetByKeyVal(tbl *Table, keyVal reflect.Value) (any, ValueMeta, error) {
	if tbl == nil {
		panic("tbl == nil")
	}
	rowVal, rowMeta, err := tx.getRowValByKeyVal(tbl, keyVal, true)
	if err != nil {
		return nil, rowMeta, err
	}
	if !rowVal.IsValid() {
		return nil, ValueMeta{}, nil
	}
	return rowVal.Interface(), rowMeta, nil
}

func (tx *Tx) GetByKeyRaw(tbl *Table, keyRaw []byte) (any, ValueMeta) {
	if tbl == nil {
		panic("tbl == nil")
	}
	rowVal, rowMeta, err := tx.getRowValByKeyRaw(tbl, keyRaw, true, hexBytes(keyRaw))
	if err != nil {
		panic(err)
	}
	if !rowVal.IsValid() {
		return nil, ValueMeta{}
	}
	return rowVal.Interface(), rowMeta
}

func (tx *Tx) GetMeta(tbl *Table, key any) ValueMeta {
	return tx.GetMetaByKeyVal(tbl, reflect.ValueOf(key))
}

func (tx *Tx) GetMetaByKeyVal(tbl *Table, keyVal reflect.Value) ValueMeta {
	_, rowMeta, err := tx.getRowValByKeyVal(tbl, keyVal, false)
	if err != nil {
		panic(err)
	}
	return rowMeta
}

func (tx *Tx) Exists(tbl *Table, key any) bool {
	return tx.existsByKeyVal(tbl, reflect.ValueOf(key))
}

func (tx *Tx) getRowValByKeyVal(tbl *Table, keyVal reflect.Value, includeRow bool) (reflect.Value, ValueMeta, error) {
	keyVal = tbl.ensureCorrectKeyType(keyVal)
	keyBuf := keyBytesPool.Get().([]byte)
	keyRaw := tbl.encodeKeyVal(keyBuf, keyVal, true)
	defer keyBytesPool.Put(keyBuf[:0])
	return tx.getRowValByKeyRaw(tbl, keyRaw, includeRow, keyVal.Interface())
}

func (tx *Tx) getRowValByKeyRaw(tbl *Table, keyRaw []byte, includeRow bool, keyValueForLogging any) (reflect.Value, ValueMeta, error) {
	val, valMeta, err := tx.getRowValByRawKey(tbl, keyRaw, includeRow)
	if tx.db.verbose {
		if includeRow {
			if val.IsValid() {
				tx.db.logf("db: GET %s/%v => %v", tbl.name, keyValueForLogging, loggableRowVal(tbl, val))
			} else {
				tx.db.logf("db: GET.NOTFOUND %s/%v", tbl.name, keyValueForLogging)
			}
		} else {
			if val.IsValid() {
				tx.db.logf("db: META %s/%v => %v", tbl.name, keyValueForLogging, loggableRowVal(tbl, val))
			} else {
				tx.db.logf("db: META.NOTFOUND %s/%v", tbl.name, keyValueForLogging)
			}
		}
	}
	return val, valMeta, err
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

func (tx *Tx) ExistsByKeyRaw(tbl *Table, keyRaw []byte) bool {
	found := (tx.getRawByRawKey(tbl, keyRaw) != nil)
	if tx.db.verbose {
		tx.db.logf("db: EXISTS.%s %s/%x", map[bool]string{false: "NO", true: "YES"}[found], tbl.name, keyRaw)
	}
	return found
}

func (tx *Tx) getRowValByRawKey(tbl *Table, keyRaw []byte, includeRow bool) (reflect.Value, ValueMeta, error) {
	valueRaw := tx.getRawByRawKey(tbl, keyRaw)
	if valueRaw == nil {
		return reflect.Value{}, ValueMeta{}, nil
	}

	if includeRow {
		return decodeTableRow(tbl, keyRaw, valueRaw, tx)
	} else {
		var vle value
		decodeTableValue(&vle, tbl, keyRaw, valueRaw)
		return reflect.Value{}, vle.ValueMeta(), nil
	}
}

func (tx *Tx) getRawByRawKey(tbl *Table, keyRaw []byte) []byte {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	return dataBuck.Get(keyRaw)
}
