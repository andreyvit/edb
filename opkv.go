package edb

import (
	"log/slog"

	"github.com/andreyvit/edb/kvo"
	"go.etcd.io/bbolt"
)

func (tx *Tx) KVGet(tbl *KVTable, key []byte) kvo.ImmutableMap {
	raw := tx.KVGetRaw(tbl, key)
	if raw == nil {
		return kvo.ImmutableMap{}
	}
	return tbl.decodeValue(raw)
}

func (tx *Tx) KVGetRaw(tbl *KVTable, key []byte) []byte {
	if tx == nil {
		panic("nil tx")
	}
	dataBuck := nonNil(tx.btx.Bucket(tbl.dataBuck.Raw()))
	return dataBuck.Get(key)
}

func (tx *Tx) KVPut(tbl *KVTable, key []byte, value kvo.Packable) {
	var data kvo.ImmutableRecordData
	if value != nil {
		data = value.Pack()
	}
	if data == nil {
		tx.KVPutRaw(tbl, key, nil)
	} else {
		tx.KVPutRaw(tbl, key, value.Pack().Bytes())
	}
}

func (tx *Tx) KVPutRaw(tbl *KVTable, key, value []byte) {
	if tx == nil {
		panic("nil tx")
	}
	dataBuck := nonNil(tx.btx.Bucket(tbl.dataBuck.Raw()))
	if len(tbl.indices) > 0 {
		oldValue := dataBuck.Get(key)
		for _, idx := range tbl.indices {
			var oldEntries, newEntries [][]byte
			if oldValue != nil {
				oldEntries = idx.entries(key, oldValue)
			}
			if value != nil {
				newEntries = idx.entries(key, value)
			}

			var idxBuck *bbolt.Bucket
			for _, ik := range oldEntries {
				if !containsBytes(newEntries, ik) {
					if idxBuck == nil {
						idxBuck = nonNil(tx.btx.Bucket(idx.idxBuck.Raw()))
					}
					idxBuck.Delete(ik)
				}
			}
			for _, ik := range newEntries {
				if !containsBytes(oldEntries, ik) {
					if idxBuck == nil {
						idxBuck = nonNil(tx.btx.Bucket(idx.idxBuck.Raw()))
					}
					idxBuck.Put(ik, emptyIndexValue)
				}
			}
		}
	}
	if value == nil {
		err := dataBuck.Delete(key)
		if err != nil {
			panic(kvtableErrf(tbl, nil, key, err, "KVDelete"))
		}
	} else {
		err := dataBuck.Put(key, value)
		if err != nil {
			panic(kvtableErrf(tbl, nil, key, err, "KVPut"))
		}
	}
}

func (tx *Tx) KVTableScan(tbl *KVTable, rang RawRange) *KVCursor {
	if tx == nil {
		panic("nil tx")
	}
	dataBuck := nonNil(tx.btx.Bucket(tbl.dataBuck.Raw()))
	logger := tx.logger
	if logger == nil {
		logger = slog.Default()
	}
	if debugLogRawScans {
		logger = logger.With("tbl", tbl.Name())
	}
	return &KVCursor{tbl, (*kvTableCursorImpl)(rang.newCursor(dataBuck.Cursor(), logger))}
}

func (tx *Tx) KVIndexScan(idx *KVIndex, rang RawRange) *KVCursor {
	if tx == nil {
		panic("nil tx")
	}
	dataBuck := nonNil(tx.btx.Bucket(idx.table.dataBuck.Raw()))
	idxBuck := nonNil(tx.btx.Bucket(idx.idxBuck.Raw()))
	logger := tx.logger
	if logger == nil {
		logger = slog.Default()
	}
	if debugLogRawScans {
		logger = logger.With("tbl", idx.table.Name(), "idx", idx.ShortName())
	}
	return &KVCursor{idx.table, &kvIndexCursorImpl{
		idxCur: RawRangeCursor{
			rang:   rang,
			bcur:   idxBuck.Cursor(),
			logger: logger,
		},
		idx:      idx,
		dataBuck: dataBuck,
	}}
}

type KVCursor struct {
	tbl  *KVTable
	impl kvCursorImpl
}

func (c *KVCursor) Next() bool          { return c.impl.Next() }
func (c *KVCursor) RawIndexKey() []byte { return c.impl.RawIndexKey() }
func (c *KVCursor) RawKey() []byte      { return c.impl.RawTableKey() }
func (c *KVCursor) RawValue() []byte    { return c.impl.RawTableValue() }

func (c *KVCursor) Object() kvo.ImmutableMap {
	return c.tbl.decodeValue(c.impl.RawTableValue())
}

func (c *KVCursor) Objects() func(yield func(k []byte, o kvo.ImmutableMap) bool) {
	return func(yield func(k []byte, o kvo.ImmutableMap) bool) {
		for c.Next() {
			if !yield(c.RawKey(), c.Object()) {
				break
			}
		}
	}
}

func (c *KVCursor) RawValues() func(yield func(k, v []byte) bool) {
	return func(yield func(k, v []byte) bool) {
		for c.Next() {
			if !yield(c.RawKey(), c.RawValue()) {
				break
			}
		}
	}
}

func (c *KVCursor) Keys() func(yield func(k []byte) bool) {
	return func(yield func(k []byte) bool) {
		for c.Next() {
			if !yield(c.RawKey()) {
				break
			}
		}
	}
}

func (c *KVCursor) IndexKeys() func(yield func(k []byte) bool) {
	return func(yield func(k []byte) bool) {
		for c.Next() {
			if !yield(c.RawIndexKey()) {
				break
			}
		}
	}
}

type kvCursorImpl interface {
	Next() bool
	RawIndexKey() []byte
	RawTableKey() []byte
	RawTableValue() []byte
}

type kvTableCursorImpl RawRangeCursor

func (ci *kvTableCursorImpl) Next() bool            { return (*RawRangeCursor)(ci).Next() }
func (ci *kvTableCursorImpl) RawIndexKey() []byte   { return nil }
func (ci *kvTableCursorImpl) RawTableKey() []byte   { return ci.k }
func (ci *kvTableCursorImpl) RawTableValue() []byte { return ci.v }

type kvIndexCursorImpl struct {
	idxCur   RawRangeCursor
	idx      *KVIndex
	dataBuck *bbolt.Bucket
}

func (ci *kvIndexCursorImpl) Next() bool          { return ci.idxCur.Next() }
func (ci *kvIndexCursorImpl) RawIndexKey() []byte { return ci.idxCur.k }
func (ci *kvIndexCursorImpl) RawTableKey() []byte {
	return ci.idx.indexKeyToPrimaryKey(ci.idxCur.k)
}
func (ci *kvIndexCursorImpl) RawTableValue() []byte {
	k := ci.RawTableKey()
	if k == nil {
		return nil
	}
	return ci.dataBuck.Get(k)
}
