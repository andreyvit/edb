package edb

import (
	"bytes"
	"fmt"
	"reflect"
)

func Lookup[Row any](txh Txish, idxArg AnyIndex, indexKey any) *Row {
	idx := idxArg.IndexDef()

	tx := txh.DBTx()
	if tbl := tx.Schema().TableByRow((*Row)(nil)); idx.table != tbl {
		panic(fmt.Errorf("invalid index %v for table %v", idx.FullName(), tbl.Name()))
	}

	rowVal, _ := tx.LookupVal(idx, reflect.ValueOf(indexKey))
	return valToRow[Row](rowVal)
}

func LookupKey[Key any](txh Txish, idxArg AnyIndex, indexKey any) (Key, bool) {
	idx := idxArg.IndexDef()

	tx := txh.DBTx()
	if at, et := reflect.TypeOf((*Key)(nil)).Elem(), idx.table.KeyType(); at != et {
		panic(fmt.Errorf("%s: LookupKey has incorrect return type %v, expected %v", idx.FullName(), at, et))
	}

	keyVal := tx.LookupKeyVal(idx, reflect.ValueOf(indexKey))
	if keyVal.IsValid() {
		return keyVal.Interface().(Key), true
	} else {
		var zero Key
		return zero, false
	}
}

func LookupExists(txh Txish, idxArg AnyIndex, indexKey any) bool {
	idx := idxArg.IndexDef()

	tx := txh.DBTx()
	return tx.LookupExists(idx, reflect.ValueOf(indexKey))
}

func (tx *Tx) Lookup(idxArg AnyIndex, indexKey any) any {
	idx := idxArg.IndexDef()

	rowVal, _ := tx.LookupVal(idx, reflect.ValueOf(indexKey))
	return valToAny(rowVal)
}

func (tx *Tx) LookupKey(idxArg AnyIndex, indexKey any) any {
	idx := idxArg.IndexDef()

	return valToAny(tx.LookupKeyVal(idx, reflect.ValueOf(indexKey)))
}
func (tx *Tx) LookupKeyVal(idxArg AnyIndex, indexKeyVal reflect.Value) reflect.Value {
	idx := idxArg.IndexDef()

	keyRaw := tx.lookupRawKeyByVal(idx, indexKeyVal)
	result := keyRawToVal(keyRaw, idx.table)
	if tx.isVerboseLoggingEnabled() {
		if keyRaw != nil {
			tx.db.logf("db: LOOKUP_KEY %s/%v => %v", idx.FullName(), loggableVal(indexKeyVal), loggableVal(result))
		} else {
			tx.db.logf("db: LOOKUP_KEY.NOTFOUND %s/%v", idx.FullName(), loggableVal(indexKeyVal))
		}
	}
	return result
}
func (tx *Tx) LookupExists(idxArg AnyIndex, indexKeyVal reflect.Value) bool {
	idx := idxArg.IndexDef()

	keyRaw := tx.lookupRawKeyByVal(idx, indexKeyVal)
	if tx.isVerboseLoggingEnabled() {
		if keyRaw != nil {
			tx.db.logf("db: LOOKUP_EXISTS.OK %s/%v", idx.FullName(), loggableVal(indexKeyVal))
		} else {
			tx.db.logf("db: LOOKUP_EXISTS.NOTFOUND %s/%v", idx.FullName(), loggableVal(indexKeyVal))
		}
	}
	return keyRaw != nil
}

func (tx *Tx) LookupVal(idxArg AnyIndex, indexKeyVal reflect.Value) (reflect.Value, ValueMeta) {
	idx := idxArg.IndexDef()

	keyRaw := tx.lookupRawKeyByVal(idx, indexKeyVal)
	if keyRaw == nil {
		return reflect.Value{}, ValueMeta{}
	}
	row, rowMeta, err := tx.getRowValByRawKey(idx.table, keyRaw, true)
	if err != nil {
		panic(err)
	}
	if rowMeta.IsMissing() && tx.db.strict {
		panic(fmt.Errorf("data error in %s: index entry points to missing record %x", idx.FullName(), keyRaw))
	}
	if tx.isVerboseLoggingEnabled() {
		tx.db.logf("db: LOOKUP %s/%v => %v", idx.FullName(), loggableVal(indexKeyVal), loggableRowVal(idx.table, row))
	}
	return row, rowMeta
}

func (tx *Tx) lookupRawKeyByVal(idx *IndexDef, indexKeyVal reflect.Value) []byte {
	if at, et := indexKeyVal.Type(), idx.keyType(); at != et {
		panic(fmt.Errorf("%s: attempted to index by incorrect type %v, expected %v", idx.FullName(), at, et))
	}

	indexKeyBuf := acquireKeyBuffer()
	defer releaseKeyBuffer(indexKeyBuf)

	fe := flatEncoder{buf: indexKeyBuf.data}
	idx.keyEnc.encodeInto(&fe, indexKeyVal)
	raw := tx.lookupEncodedKey(idx, &fe)
	indexKeyBuf.data = fe.buf[:0]
	return raw
}

func (tx *Tx) lookupEncodedKey(idx *IndexDef, fe *flatEncoder) []byte {
	idxBuck := nonNil(tx.stx.Bucket(idx.table.name, idx.buck))
	if idx.isUnique {
		indexKeyRaw := fe.finalize()
		fe.buf = indexKeyRaw
		indexVal := idxBuck.Get(indexKeyRaw)
		if indexVal == nil {
			return nil
		}

		return decodeUniqueIndexTableKey(indexKeyRaw, indexVal, idx)
	} else {
		scanPrefix, scanPrefixEls := fe.buf, fe.count()

		c := idxBuck.Cursor()
		for k, _ := c.Seek(scanPrefix); k != nil; k, _ = c.Next() {
			if !bytes.HasPrefix(k, scanPrefix) {
				break
			}
			indexKeyTup := decodeIndexKey(k, idx)
			if len(indexKeyTup) != scanPrefixEls+1 {
				panic(fmt.Errorf("%s: invalid index key %x: got %d els, wanted %d", idx.FullName(), k, len(indexKeyTup), scanPrefixEls+1))
			}

			actualPrefix := indexKeyTup.rawData(k, scanPrefixEls)
			if !bytes.Equal(actualPrefix, scanPrefix) {
				continue
			}

			dk, _ := extractUniqueIndexKey(indexKeyTup)
			return dk
		}
		return nil
	}
}
