package edb

import (
	"errors"
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
)

func (tx *Tx) Reindex(tblArg AnyTable, idxArg AnyIndex) {
	var idx *IndexDef
	if idxArg != nil {
		idx = idxArg.IndexDef()
	}

	tbl := tblArg.TableDef()

	ts := tx.db.tableState(tbl)

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
		is.Built = true
	}

	for c := tx.TableScan(tbl, FullScan()); c.Next(); {
		tx.reindexRow(c)
	}

	ts.save(tx)
}

// Reuse encoded data and apply schema migrations without decoding a Go struct.
// Verbose logging and codecs without direct access still use native reads.
func (tx *Tx) reindexRow(c *RawTableCursor) {
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
