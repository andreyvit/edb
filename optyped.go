package edb

import (
	"fmt"
	"github.com/andreyvit/edb/kvo"
	"reflect"
)

// Get returns a borrowed record, or a missing record when the key is absent.
func (t Table[Key]) Get(txh Txish, key Key) kvo.Record {
	r, _, err := t.TryGet(txh, key)
	if err != nil {
		panic(err)
	}
	return r
}
func (t Table[Key]) TryGet(txh Txish, key Key) (kvo.Record, ValueMeta, error) {
	buf := acquireKeyBuffer()
	defer releaseKeyBuffer(buf)
	fe := flatEncoder{buf: buf.data}
	encodeTypedKey(t.tableDef.keyEnc, &fe, key)
	raw := fe.finalize()
	buf.data = raw[:0]
	return txh.DBTx().tryGetRecordByKeyRaw(t.tableDef, raw)
}

// GetRaw accepts an encoded primary key. The returned record does not retain it.
func (t Table[Key]) GetRaw(txh Txish, key []byte) kvo.Record {
	return txh.DBTx().getRecordByKeyRaw(t.tableDef, key)
}
func (t Table[Key]) TryGetRaw(txh Txish, key []byte) (kvo.Record, ValueMeta, error) {
	return txh.DBTx().tryGetRecordByKeyRaw(t.tableDef, key)
}

// Put inserts or replaces a record under an explicit key and reports a change.
// Use PutChecked when replacing a previously read revision conditionally.
func (t Table[Key]) Put(txh Txish, key Key, r kvo.Record) bool {
	old, next := txh.DBTx().PutByKey(t.tableDef, key, r)
	return next.IsModified(old)
}

// Edit opens a private edit in the current write transaction, or returns nil.
func (t Table[Key]) Edit(txh Txish, key Key) *RecordEdit {
	return txh.DBTx().OpenRecordEdit(t.tableDef, key)
}
func (t Table[Key]) TryEdit(txh Txish, key Key) (*RecordEdit, error) {
	return txh.DBTx().TryOpenRecordEdit(t.tableDef, key)
}

// New prepares an insert; Save checks that the primary key is still free.
func (t Table[Key]) New(txh Txish, key Key) *RecordEdit {
	return txh.DBTx().NewRecordEdit(t.tableDef, key)
}
func (t Table[Key]) TryNew(txh Txish, key Key) (*RecordEdit, error) {
	return txh.DBTx().TryNewRecordEdit(t.tableDef, key)
}

// EditOrCreate opens the existing row or prepares an insert under this key.
func (t Table[Key]) EditOrCreate(txh Txish, key Key) *RecordEdit {
	e, err := t.TryEditOrCreate(txh, key)
	if err != nil {
		panic(err)
	}
	return e
}
func (t Table[Key]) TryEditOrCreate(txh Txish, key Key) (*RecordEdit, error) {
	e, err := t.TryEdit(txh, key)
	if err != nil || e != nil {
		return e, err
	}
	return t.TryNew(txh, key)
}

// Key returns this edit's primary key, including when the model omits it.
func (t Table[Key]) Key(edit *RecordEdit) Key {
	if edit == nil || edit.table != t.tableDef {
		panic("edb: edit belongs to another table")
	}
	return decodeTypedKey[Key](t.tableDef, edit.key)
}

// PutChecked replaces only the expected revision (zero metadata means insert).
func (t Table[Key]) PutChecked(txh Txish, key Key, r kvo.Record, expected ValueMeta) bool {
	next, err := t.TryPutChecked(txh, key, r, expected)
	if err != nil {
		panic(err)
	}
	return next.IsModified(expected)
}
func (t Table[Key]) TryPutChecked(txh Txish, key Key, r kvo.Record, expected ValueMeta) (ValueMeta, error) {
	return txh.DBTx().TryPutRecordChecked(t.tableDef, key, r, expected)
}

func (t Table[Key]) Save(txh Txish, edit *RecordEdit) bool {
	var old ValueMeta
	if edit != nil {
		old = edit.meta
	}
	next, err := t.TrySave(txh, edit)
	if err != nil {
		panic(err)
	}
	return next.IsModified(old)
}
func (t Table[Key]) TrySave(txh Txish, edit *RecordEdit) (ValueMeta, error) {
	if edit == nil || edit.table != t.tableDef {
		return ValueMeta{}, fmt.Errorf("%s: edit belongs to another table", t.tableDef.name)
	}
	return txh.DBTx().TrySaveRecord(edit)
}

// Get returns an owned native row, or nil when the key is absent.
func (t RowTable[Row, Key]) Get(txh Txish, key Key) *Row {
	r, _, err := t.TryGet(txh, key)
	if err != nil {
		panic(err)
	}
	return r
}
func (t RowTable[Row, Key]) TryGet(txh Txish, key Key) (*Row, ValueMeta, error) {
	r, meta, err := txh.DBTx().getRowValByKeyVal(t.tableDef, reflect.ValueOf(key), true)
	return valToRow[Row](r), meta, err
}
func (t RowTable[Row, Key]) GetRaw(txh Txish, key []byte) *Row {
	r, _, err := t.TryGetRaw(txh, key)
	if err != nil {
		panic(err)
	}
	return r
}
func (t RowTable[Row, Key]) TryGetRaw(txh Txish, key []byte) (*Row, ValueMeta, error) {
	r, meta, err := txh.DBTx().getRowValByRawKey(t.tableDef, key, true)
	return valToRow[Row](r), meta, err
}
func (t RowTable[Row, Key]) Put(txh Txish, row *Row) bool {
	old, next := txh.DBTx().Put(t.tableDef, row)
	return next.IsModified(old)
}
func (t RowTable[Row, Key]) PutByKey(txh Txish, key Key, row *Row) bool {
	old, next := txh.DBTx().PutByKey(t.tableDef, key, row)
	return next.IsModified(old)
}

func (t typedTable[Key]) Delete(txh Txish, key Key) bool {
	return txh.DBTx().DeleteByKey(t.tableDef, key)
}
func (t typedTable[Key]) Exists(txh Txish, key Key) bool {
	buf := acquireKeyBuffer()
	defer releaseKeyBuffer(buf)
	fe := flatEncoder{buf: buf.data}
	encodeTypedKey(t.tableDef.keyEnc, &fe, key)
	raw := fe.finalize()
	buf.data = raw[:0]
	return txh.DBTx().ExistsByKeyRaw(t.tableDef, raw)
}

func (i typedIndex[IndexKey, Key]) lookupRaw(tx *Tx, key IndexKey) []byte {
	buf := acquireKeyBuffer()
	defer releaseKeyBuffer(buf)
	fe := flatEncoder{buf: buf.data}
	encodeTypedKey(i.index.keyEnc, &fe, key)
	raw := tx.lookupEncodedKey(i.index, &fe)
	buf.data = fe.buf[:0]
	return raw
}

// LookupKey returns the first matching primary key in storage order. Zero can
// be a valid decoded key; the boolean distinguishes a missing index entry.
func (i typedIndex[IndexKey, Key]) LookupKey(txh Txish, key IndexKey) (Key, bool) {
	raw := i.lookupRaw(txh.DBTx(), key)
	if raw == nil {
		var zero Key
		return zero, false
	}
	return decodeTypedKey[Key](i.index.table, raw), true
}
func (i typedIndex[IndexKey, Key]) Exists(txh Txish, key IndexKey) bool {
	return i.lookupRaw(txh.DBTx(), key) != nil
}

// Lookup returns the primary key and record of the first match, including for
// non-unique indexes. A missing result is (zero key, missing record).
func (i recordIndex[IndexKey, Key]) Lookup(txh Txish, key IndexKey) (Key, kvo.Record) {
	pk, r, _, err := i.TryLookup(txh, key)
	if err != nil {
		panic(err)
	}
	return pk, r
}
func (i recordIndex[IndexKey, Key]) TryLookup(txh Txish, key IndexKey) (Key, kvo.Record, ValueMeta, error) {
	var zero Key
	tx := txh.DBTx()
	raw := i.lookupRaw(tx, key)
	if raw == nil {
		return zero, kvo.Record{}, ValueMeta{}, nil
	}
	pk := decodeTypedKey[Key](i.index.table, raw)
	r, meta, err := tx.tryGetRecordByKeyRaw(i.index.table, raw)
	if err == nil && r.Missing() && tx.db.strict {
		err = fmt.Errorf("data error in %s: index entry points to missing record %x", i.index.FullName(), raw)
	}
	return pk, r, meta, err
}

// Lookup has the same first-match semantics as recordIndex.Lookup.
func (i rowIndex[Row, IndexKey, Key]) Lookup(txh Txish, key IndexKey) (Key, *Row) {
	pk, row, _, err := i.TryLookup(txh, key)
	if err != nil {
		panic(err)
	}
	return pk, row
}
func (i rowIndex[Row, IndexKey, Key]) TryLookup(txh Txish, key IndexKey) (Key, *Row, ValueMeta, error) {
	var zero Key
	tx := txh.DBTx()
	raw := i.lookupRaw(tx, key)
	if raw == nil {
		return zero, nil, ValueMeta{}, nil
	}
	pk := decodeTypedKey[Key](i.index.table, raw)
	r, meta, err := tx.getRowValByRawKey(i.index.table, raw, true)
	if err == nil && meta.IsMissing() && tx.db.strict {
		err = fmt.Errorf("data error in %s: index entry points to missing record %x", i.index.FullName(), raw)
	}
	return pk, valToRow[Row](r), meta, err
}
