package edb

import (
	"bytes"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

func Put(txh Txish, row any) {
	tx := txh.DBTx()
	tbl := tx.tableByRowPtr(row)
	tx.Put(tbl, row)
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
		if tx.db.verbose {
			tx.db.logf("db: PUT.NOOP %s/%v => m=%d %s", tbl.name, keyVal, newModCount, loggableRowVal(rowVal))
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

	if tx.db.verbose {
		tx.db.logf("db: PUT %s/%v => m=%d %s", tbl.name, keyVal, newModCount, loggableRowVal(rowVal))
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

	return ValueMeta{newSchemaVer, newModCount}
}
