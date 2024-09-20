package edb

import "github.com/andreyvit/edb/kvo"

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
