package edb

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
)

func (tx *Tx) Reindex(tblArg AnyTable, idxArg AnyIndex) {
	var idx *IndexDef
	if idxArg != nil {
		idx = idxArg.IndexDef()
	}
	tbl := tblArg.TableDef()
	previousWrite, previousRaw := tx.internalWrite, tx.rawWrites
	if !tx.allowsRowMigrations() || !tx.db.AllowsLocalWrites() && !tx.internalWrite {
		tx.internalWrite = true
		tx.rawWrites = true
	}
	defer func() {
		tx.internalWrite, tx.rawWrites = previousWrite, previousRaw
	}()
	ts := tx.db.tableState(tbl)
	complete := false
	defer tx.guardReplicationMutation(&complete)
	tx.markWritten()

	tx.reindexing = true
	defer func() {
		tx.reindexing = false
	}()

	for _, is := range ts.indexStates {
		if idx != nil && idx != is.index {
			continue
		}
		err := tx.stx.DeleteBucket(tbl.name, is.index.buck)
		if err != nil && !errors.Is(err, ErrBucketNotFound) {
			panic(err)
		}
		_ = must(tx.stx.CreateBucket(tbl.name, is.index.buck))
	}

	for c := tx.TableScan(tbl, FullScan()); c.Next(); {
		tx.reindexRow(c)
	}

	for _, is := range ts.indexStates {
		if idx == nil || idx == is.index {
			is.Built = true
		}
	}
	ts.save(tx)
	complete = true
}

// Reuse encoded data and apply schema migrations without decoding a Go struct.
// Verbose logging and codecs without direct access still use native reads.
func (tx *Tx) reindexRow(c *RawTableCursor) {
	if !tx.allowsRowMigrations() {
		tx.reindexStoredRow(c)
		return
	}
	t := c.Table()
	if t.kvoModel != nil && !tx.isVerboseLoggingEnabled() {
		var v value
		decodeTableValue(&v, t, c.RawKey(), c.RawRow(), false)
		if v.Flags == vfKVO || t.directMessagePack() {
			r, native, _, err := tx.recordFromValue(t, c.RawKey(), &v)
			ensure(err)
			if native.IsValid() {
				tx.PutVal(t, native)
				return
			}
			payload := r.MessagePack()
			if payload == nil {
				payload = r.Snapshot().Raw()
				ensure(t.binding.TryValidate(payload))
			} else {
				msgpack.Open(payload)
			}
			tx.putVal(t, t.DecodeKeyVal(c.RawKey()), reflect.Value{}, payload)
			return
		}
	}
	row, _ := c.RowVal()
	tx.PutVal(t, row)
}

// Replace the payload unchanged, computing the index tail with local definitions.
func (tx *Tx) putStoredRow(t *TableDef, key []byte, v *value) {
	b := nonNil(tx.stx.Bucket(t.name, dataBucketName))
	ts := tx.db.tableState(t)
	ib := tx.buildStoredRowIndexes(t, key, v)
	defer ib.release()
	ib.transferValuesTo(tx)
	if raw := b.Get(key); raw != nil {
		var old value
		ensure(old.decode(raw, false))
		findRemovedIndexKeys(old.Index, ib.rows, prepareToDeleteIndexEntries(tx, ts))
	}
	for _, ir := range ib.rows {
		ensure(nonNil(tx.stx.Bucket(t.name, ir.Index.buck)).Put(ir.KeyRaw, ir.ValueRaw))
	}
	out := reserveValueHeader(make([]byte, 0, maxValueHeaderSize+len(v.Data)))
	out = append(out, v.Data...)
	off := len(out)
	out = appendIndexKeys(out, ib.rows)
	out = putValueHeader(out, v.Flags, v.SchemaVer, v.ModCount, off)
	ensure(b.Put(key, out))
}
func (tx *Tx) deleteStoredRow(t *TableDef, key []byte) {
	b := nonNil(tx.stx.Bucket(t.name, dataBucketName))
	if raw := b.Get(key); raw != nil {
		var old value
		ensure(old.decode(raw, false))
		decodeIndexKeys(old.Index, prepareToDeleteIndexEntries(tx, tx.db.tableState(t)))
	}
	ensure(b.Delete(key))
}
func (tx *Tx) buildStoredRowIndexes(t *TableDef, key []byte, v *value) *IndexBuilder {
	ib := makeIndexBuilder(tx.db.tableState(t), key)
	ok := false
	defer func() {
		if !ok {
			ib.release()
		}
	}()
	if t.hasIndexers() {
		var record kvo.Record
		if v.Flags == vfKVO {
			record = t.encodedRecord(row2.Trusted(v.Data))
		} else {
			var err error
			record, _, _, err = tx.recordFromValue(t, key, v)
			if err != nil {
				panic(fmt.Errorf("index stored row: %w", err))
			}
		}
		t.runIndexers(tx, t.DecodeKeyVal(key), record, ib)
	}
	ib.finalize()
	ok = true
	return ib
}

// Local rebuilds preserve source payload, codec, schema version and revision.
func (tx *Tx) reindexStoredRow(c *RawTableCursor) {
	var v value
	ensure(v.decode(c.RawRow(), false))
	tx.putStoredRow(c.Table(), c.RawKey(), &v)
}
