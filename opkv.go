package edb

import (
	"github.com/andreyvit/edb/kvo"
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
	return &KVCursor{tbl, (*kvTableCursorImpl)(rang.newCursor(dataBuck.Cursor()))}
}

type KVCursor struct {
	tbl  *KVTable
	impl kvCursorImpl
}

func (c *KVCursor) Next() bool       { return c.impl.Next() }
func (c *KVCursor) RawKey() []byte   { return c.impl.RawTableKey() }
func (c *KVCursor) RawValue() []byte { return c.impl.RawTableValue() }

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

type kvCursorImpl interface {
	Next() bool
	RawTableKey() []byte
	RawTableValue() []byte
}

type kvTableCursorImpl RawRangeCursor

func (ci *kvTableCursorImpl) Next() bool            { return (*RawRangeCursor)(ci).Next() }
func (ci *kvTableCursorImpl) RawTableKey() []byte   { return ci.k }
func (ci *kvTableCursorImpl) RawTableValue() []byte { return ci.v }
