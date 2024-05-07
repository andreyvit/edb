package edb

import (
	"bytes"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

func Put(txh Txish, rows ...any) {
	for _, row := range rows {
		tx := txh.DBTx()
		tbl := tx.Schema().TableByRow(row)
		tx.Put(tbl, row)
	}
}

func (tx *Tx) Put(tbl *Table, row any) ValueMeta {
	return tx.PutVal(tbl, reflect.ValueOf(row))
}

func (tx *Tx) PutVal(tbl *Table, rowVal reflect.Value) ValueMeta {
	if tx == nil {
		panic("nil tx")
	}
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	dataBuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))

	keyBuf := keyBytesPool.Get().([]byte)
	keyVal := tbl.RowKeyVal(rowVal)
	keyRaw := tbl.encodeKeyVal(keyBuf, keyVal, false)
	defer keyBytesPool.Put(keyBuf[:0])

	ts := tx.db.tableState(tbl)
	ib := makeIndexBuilder(ts, keyRaw)
	defer ib.release(tx)
	tbl.indexer(rowVal.Interface(), &ib)
	ib.finalize()

	oldValueRaw := dataBuck.Get(keyRaw)
	var old value
	if oldValueRaw != nil {
		err := old.decode(oldValueRaw)
		if err != nil {
			panic(tableErrf(tbl, nil, keyRaw, err, "decoding old value"))
		}
	}

	newSchemaVer := tbl.latestSchemaVer
	newModCount := old.ModCount

	valueBuf := valueBytesPool.Get().([]byte)
	tx.addValueBuf(valueBuf)
	valueRaw := reserveValueHeader(valueBuf)
	dataOff := len(valueRaw)
	valueRaw = tbl.encodeRowVal(valueRaw, rowVal)
	dataBytes := valueRaw[dataOff:]
	indexOff := len(valueRaw)
	valueRaw = appendIndexKeys(valueRaw, ib.rows)
	indexBytes := valueRaw[indexOff:]

	isDataUnchanged := bytes.Equal(dataBytes, old.Data)
	isIndexKeySetUnchanged := bytes.Equal(indexBytes, old.Index)

	if oldValueRaw != nil && (old.SchemaVer == newSchemaVer) && isDataUnchanged && isIndexKeySetUnchanged && !tx.reindexing {
		// Likely nothing changed. Ignore possible index value changes; if data is
		// unchanged, a no-op save is much more likely than a change to indexing algorithm.
		if tx.isVerboseLoggingEnabled() {
			tx.db.logf("db: PUT.NOOP %s/%v => m=%d %s", tbl.name, keyVal, newModCount, loggableRowVal(tbl, rowVal))
		}
		return ValueMeta{newSchemaVer, newModCount}
	}
	if !isDataUnchanged {
		newModCount++
	}
	valueRaw = putValueHeader(valueRaw, vfDefault, newSchemaVer, newModCount, indexOff)
	tx.markWritten()

	// log.Printf("PUT into %s: %x => %x (%s)", tbl.Name(), keyRaw, valueRaw, valueRaw)
	ensure(dataBuck.Put(keyRaw, valueRaw))

	if tx.isVerboseLoggingEnabled() {
		tx.db.logf("db: PUT %s/%v => m=%d %s", tbl.name, keyVal, newModCount, loggableRowVal(tbl, rowVal))
	}

	if oldValueRaw != nil && !isIndexKeySetUnchanged && !tx.reindexing {
		// delete removed index entries
		del := prepareToDeleteIndexEntries(tableBuck, ts)
		findRemovedIndexKeys(old.Index, ib.rows, del)
	}

	// put new index entries (do it even if isIndexKeySetUnchanged, in cases values have changed)
	var idx *Index
	var idxBuck *bbolt.Bucket
	for _, ir := range ib.rows {
		if ir.Index != idx {
			idx = ir.Index
			idxBuck = tableBuck.Bucket(idx.buck.Raw())
			if idxBuck == nil {
				panic(fmt.Errorf("missing bucket for index %v", idx.FullName()))
			}
		}
		// log.Printf("PUT into %s: %x => %x", idx.FullName(), ir.KeyRaw, ir.ValueRaw)
		ensure(idxBuck.Put(ir.KeyRaw, ir.ValueRaw))
	}

	if opts := tx.changeOptions[tbl]; opts.Contains(ChangeFlagNotify) && tx.changeHandler != nil {
		chg := Change{
			table:  tbl,
			op:     OpPut,
			rawKey: keyRaw,
		}
		if opts.Contains(ChangeFlagIncludeMutableRow) {
			var newVal value
			err := newVal.decode(valueRaw)
			if err != nil {
				panic(tableErrf(tbl, nil, keyRaw, err, "decoding new value"))
			}
			chg.rowVal, chg.keyVal, _, err = decodeTableRowFromValue(&newVal, tbl, keyRaw, tx)
			if err != nil {
				panic(err)
			}
		} else if opts.Contains(ChangeFlagIncludeRow) {
			chg.rowVal, chg.keyVal = rowVal, keyVal
		} else if opts.Contains(ChangeFlagIncludeKey) {
			chg.keyVal = keyVal
		}
		if opts.Contains(ChangeFlagIncludeOldRow) {
			var err error
			chg.oldRowVal, _, _, err = decodeTableRowFromValue(&old, tbl, keyRaw, tx)
			if err != nil {
				chg.oldRowVal = reflect.Value{}
				tx.db.logf("db: PUT %s/%v: cannot decode old row value: %v", tbl.name, keyRaw, err)
			}
		}
		tx.changeHandler(tx, &chg)
	}

	return ValueMeta{newSchemaVer, newModCount}
}
