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

// SGetRaw returns the encoded value, bypassing the key's declared value type.
func SGetRaw[T any](txh Txish, sk *SKey[T]) []byte {
	tx := txh.DBTx()
	buck := nonNil(tx.stx.Bucket(sk.mp.name, ""))
	return buck.Get(sk.keyBytes)
}

// SPutRaw stores encoded bytes without validating their value type.
func SPutRaw[T any](txh Txish, sk *SKey[T], raw []byte) {
	tx := txh.DBTx()
	tx.checkLocalWrite(ReplicationObject{Map: sk.mp})
	buck := nonNil(tx.stx.Bucket(sk.mp.name, ""))
	complete := false
	defer tx.guardReplicationMutation(&complete)
	obj := ReplicationObject{Map: sk.mp}
	opt := tx.captureOptions(obj)
	var old ReplicationValue
	if opt != nil {
		old = rawReplicationValue(buck.Get(sk.keyBytes))
	}
	tx.markWritten()
	err := buck.Put(sk.keyBytes, raw)
	if err != nil {
		panic(fmt.Errorf("SPut %v: %w", sk, err))
	}
	if opt != nil {
		next := rawReplicationValue(raw)
		next.Present = true
		tx.stageReplicationChange(opt, obj, ReplicationPut, sk.keyBytes, old, next)
	}
	complete = true
}

func SGet[T any](txh Txish, sk *SKey[T], v *T) bool {
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

func SPut[T any](txh Txish, sk *SKey[T], v *T) {
	tx := txh.DBTx()
	valueBuf := acquireValueBuffer()
	tx.addValueBuf(valueBuf)
	valueRaw := sk.valueEnc.EncodeValue(valueBuf.data, reflect.ValueOf(v))
	valueBuf.data = valueRaw[:0]
	SPutRaw(tx, sk, valueRaw)
}

func CountAll(txh Txish, tblArg AnyTable) int {
	tbl := tblArg.TableDef()

	tx := txh.DBTx()
	dataBuck := nonNil(tx.stx.Bucket(tbl.name, dataBucketName))
	return dataBuck.KeyCount()
}
