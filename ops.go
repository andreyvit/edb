package edb

import (
	"errors"
	"fmt"
	"reflect"
)

var Break = errors.New("break")

func Proto[T any]() any {
	return ((*T)(nil))
}

func SGetRaw(txh Txish, sk *SKey) []byte {
	tx := txh.DBTx()
	buck := nonNil(tx.stx.Bucket(sk.mp.name, ""))
	return buck.Get(sk.keyBytes)
}

func SPutRaw(txh Txish, sk *SKey, raw []byte) {
	tx := txh.DBTx()
	buck := nonNil(tx.stx.Bucket(sk.mp.name, ""))
	tx.markWritten()
	err := buck.Put(sk.keyBytes, raw)
	if err != nil {
		panic(fmt.Errorf("SPut %v: %w", sk, err))
	}
}

func SGet[T any](txh Txish, sk *SKey, v *T) bool {
	tx := txh.DBTx()
	raw := SGetRaw(tx, sk)
	if raw == nil {
		return false
	}
	err := sk.valueEnc.DecodeValue(raw, reflect.ValueOf(v))
	if err != nil {
		panic(fmt.Errorf("SGet %v: %w", sk, err))
	}
	return true
}

func SPut[T any](txh Txish, sk *SKey, v *T) {
	tx := txh.DBTx()
	valueBuf := valueBytesPool.Get().([]byte)
	tx.addValueBuf(valueBuf)
	valueRaw := sk.valueEnc.EncodeValue(valueBuf, reflect.ValueOf(v))
	SPutRaw(tx, sk, valueRaw)
}

func CountAll(txh Txish, tbl *Table) int {
	tx := txh.DBTx()
	dataBuck := nonNil(tx.stx.Bucket(tbl.name, dataBucketName))
	return dataBuck.KeyCount()
}
