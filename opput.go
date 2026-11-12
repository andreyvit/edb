package edb

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func Put(txh Txish, rows ...any) bool {
	var isModified bool
	for _, row := range rows {
		tx := txh.DBTx()
		tbl := tx.Schema().TableByRow(row)
		oldMeta, newMeta := tx.Put(tbl, row)
		if newMeta.IsModified(oldMeta) {
			isModified = true
		}
	}
	return isModified
}

func (tx *Tx) DecodeMementoVal(tblArg AnyTable, keyRaw []byte, memento []byte) (rowVal reflect.Value, meta ValueMeta, err error) {
	tbl := tblArg.TableDef()

	return decodeTableRow(tbl, keyRaw, memento, tx, true)
}

func (tx *Tx) Put(tblArg AnyTable, row any) (oldMeta, newMeta ValueMeta) {
	tbl := tblArg.TableDef()

	if r, ok := row.(kvo.Record); ok {
		key := tbl.recordKey(r)
		oldMeta, newMeta, err := tx.putRecord(tbl, key.Interface(), r, nil)
		if err != nil {
			panic(err)
		}
		return oldMeta, newMeta
	}
	return tx.PutVal(tbl, reflect.ValueOf(row))
}

// PutByKey inserts or replaces a row using an explicit database key. KVO
// records need no key property. A native Go row is shallow-copied before its
// key field is set, so the caller's struct is not changed.
func (tx *Tx) PutByKey(tblArg AnyTable, key, row any) (oldMeta, newMeta ValueMeta) {
	tbl := tblArg.TableDef()

	if r, ok := row.(kvo.Record); ok {
		oldMeta, newMeta, err := tx.putRecord(tbl, key, r, nil)
		if err != nil {
			panic(err)
		}
		return oldMeta, newMeta
	}
	keyVal := tbl.ensureCorrectKeyType(reflect.ValueOf(key))
	v := reflect.ValueOf(row)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Type().Elem() != tbl.rowType {
		panic(fmt.Sprintf("%s: expected *%v row", tbl.name, tbl.rowType))
	}
	if tbl.kvoModel != nil && tbl.kvoModel.PrimaryKey() != nil && !bytes.Equal(tbl.EncodeKeyVal(tbl.RowKeyVal(v)), tbl.EncodeKeyVal(keyVal)) {
		panic("record primary key conflicts with supplied key")
	}
	copy := tbl.NewRowVal()
	copy.Elem().Set(v.Elem())
	tbl.SetRowKeyVal(copy, keyVal)
	return tx.putVal(tbl, keyVal, copy, nil)
}

// PutByKey is the explicitly keyed counterpart of Put for native Go rows.
func PutByKey[Row any](txh Txish, key any, row *Row) bool {
	tx := txh.DBTx()
	old, next := tx.PutByKey(tableOf[Row](tx), key, row)
	return next.IsModified(old)
}

func (tx *Tx) PutVal(tblArg AnyTable, rowVal reflect.Value) (oldMeta, newMeta ValueMeta) {
	tbl := tblArg.TableDef()

	return tx.putVal(tbl, tbl.RowKeyVal(rowVal), rowVal, nil)
}

func (tx *Tx) putVal(tbl *TableDef, keyVal, rowVal reflect.Value, edited []byte) (oldMeta, newMeta ValueMeta) {
	if tx == nil {
		panic("nil tx")
	}
	dataBuck := nonNil(tx.stx.Bucket(tbl.name, dataBucketName))

	keyBuf := acquireKeyBuffer()
	keyRaw := tbl.encodeKeyVal(keyBuf.data, keyVal, false)
	keyBuf.data = keyRaw[:0]
	defer releaseKeyBuffer(keyBuf)

	ts := tx.db.tableState(tbl)
	ib := makeIndexBuilder(ts, keyRaw)
	defer ib.release()

	oldValueRaw := dataBuck.Get(keyRaw)
	var old value
	if oldValueRaw != nil {
		err := old.decode(oldValueRaw, false)
		if err != nil {
			panic(tableErrf(tbl, nil, keyRaw, err, "decoding old value"))
		}
	}

	oldSchemaVer := tbl.latestSchemaVer
	oldModCount := old.ModCount
	newSchemaVer, newModCount := oldSchemaVer, oldModCount

	valueBuf := acquireValueBuffer()
	defer func() {
		if valueBuf != nil {
			releaseValueBuffer(valueBuf)
		}
	}()
	valueRaw := reserveValueHeader(valueBuf.data)
	dataOff := len(valueRaw)
	flags := vfDefault
	if old.Flags == vfKVO || oldValueRaw == nil && tbl.binding != nil && !tbl.messagePackInserts {
		flags = vfKVO
	}
	switch {
	case edited != nil:
		valueRaw = append(valueRaw, edited...)
	case flags == vfKVO:
		if tbl.binding == nil {
			panic(ErrNoRecordBinding)
		}
		layout := tbl.rowLayout
		if oldValueRaw != nil {
			layout = row2.Trusted(old.Data).Layout()
		}
		valueRaw = tbl.binding.Encode(valueRaw, rowVal.Elem(), layout)
	default:
		valueRaw = tbl.encodeRowVal(valueRaw, rowVal)
	}
	dataBytes := valueRaw[dataOff:]
	if tbl.hasIndexers() {
		tbl.indexProperties(tx, keyVal, flags, dataBytes, rowVal, ib)
	}
	ib.finalize()
	indexOff := len(valueRaw)
	valueRaw = appendIndexKeys(valueRaw, ib.rows)
	valueBuf.data = valueRaw[:0]
	indexBytes := valueRaw[indexOff:]

	isDataUnchanged := bytes.Equal(dataBytes, old.Data)
	isIndexKeySetUnchanged := bytes.Equal(indexBytes, old.Index)

	if oldValueRaw != nil && (old.SchemaVer == newSchemaVer) && isDataUnchanged && isIndexKeySetUnchanged && !tx.reindexing {
		// Likely nothing changed. Ignore possible index value changes; if data is
		// unchanged, a no-op save is much more likely than a change to indexing algorithm.
		if tx.isVerboseLoggingEnabled() {
			tx.db.logf("db: PUT.NOOP %s/%v => m=%d %s", tbl.name, keyVal, newModCount, loggableRowVal(tbl, rowVal))
		}
		return ValueMeta{oldSchemaVer, oldModCount}, ValueMeta{newSchemaVer, newModCount}
	}
	if !isDataUnchanged {
		newModCount++
	}
	valueRaw = putValueHeader(valueRaw, flags, newSchemaVer, newModCount, indexOff)
	tx.addValueBuf(valueBuf)
	valueBuf = nil
	tx.markWritten()

	ensure(dataBuck.Put(keyRaw, valueRaw))

	if tx.isVerboseLoggingEnabled() {
		tx.db.logf("db: PUT %s/%v => m=%d %s", tbl.name, keyVal, newModCount, loggableRowVal(tbl, rowVal))
	}

	if oldValueRaw != nil && !isIndexKeySetUnchanged && !tx.reindexing {
		// delete removed index entries
		del := prepareToDeleteIndexEntries(tx, ts)
		findRemovedIndexKeys(old.Index, ib.rows, del)
	}

	// put new index entries (do it even if isIndexKeySetUnchanged, in cases values have changed)
	ib.transferValuesTo(tx)
	var idx *IndexDef
	var idxBuck storageBucket
	for _, ir := range ib.rows {
		if ir.Index != idx {
			idx = ir.Index
			idxBuck = tx.stx.Bucket(tbl.name, idx.buck)
			if idxBuck == nil {
				panic(fmt.Errorf("missing bucket for index %v", idx.FullName()))
			}
		}
		ensure(idxBuck.Put(ir.KeyRaw, ir.ValueRaw))
	}

	var previous *value
	if oldValueRaw != nil {
		previous = &old
	}
	next := value{Flags: flags, SchemaVer: newSchemaVer, ModCount: newModCount, Data: dataBytes}
	tx.notifyChange(tbl, OpPut, keyRaw, previous, &next)

	return ValueMeta{oldSchemaVer, oldModCount}, ValueMeta{newSchemaVer, newModCount}
}

func (t *TableDef) indexProperties(tx *Tx, key reflect.Value, flags valueFlags, data []byte, native reflect.Value, ib *IndexBuilder) {
	var r kvo.Record
	if flags == vfKVO {
		r = t.encodedRecord(row2.Trusted(data))
	} else if native.IsValid() {
		r = t.nativeRecord(native)
	} else {
		r = t.messagePackRecord(data)
	}
	t.runIndexers(tx, key, r, ib)
}

func (t *TableDef) hasIndexers() bool { return t.recordIndexer != nil || len(t.indexers) != 0 }

func (t *TableDef) runIndexers(tx *Tx, key reflect.Value, r kvo.Record, ib *IndexBuilder) {
	if t.recordIndexer != nil {
		t.recordIndexer(r, ib)
	}
	if len(t.indexers) != 0 {
		run, _ := t.indexerPool.Get().(*indexerRun)
		if run == nil {
			run = &indexerRun{callbacks: make([]func(*Tx, values.Value, kvo.Record), len(t.indexers))}
			for i, idx := range t.indexers {
				run.callbacks[i] = idx.makeIndexer(run)
			}
		}
		run.builder = ib
		defer func() {
			run.builder = nil
			t.indexerPool.Put(run)
		}()
		pk := indexPrimaryKey(key)
		for _, callback := range run.callbacks {
			callback(tx, pk, r)
		}
	}
}
