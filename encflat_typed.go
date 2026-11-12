package edb

import (
	"fmt"
	"reflect"
	"unsafe"
)

// Registration establishes exact type identity before these helpers are used.
// Unsigned keys use the same tuple encoding as flatEncoding without boxing.
func encodeTypedKey[Key any](enc *flatEncoding, fe *flatEncoder, key Key) {
	if enc.uintKeyBits != 0 {
		fe.begin()
		var n uint64
		switch unsafe.Sizeof(key) {
		case 1:
			n = uint64(*(*uint8)(unsafe.Pointer(&key)))
		case 2:
			n = uint64(*(*uint16)(unsafe.Pointer(&key)))
		case 4:
			n = uint64(*(*uint32)(unsafe.Pointer(&key)))
		case 8:
			n = *(*uint64)(unsafe.Pointer(&key))
		}
		fe.buf = appendUint64(fe.buf, n)
		return
	}
	if enc.scalarKind == reflect.String {
		fe.begin()
		fe.buf = appendString(fe.buf, *(*string)(unsafe.Pointer(&key)))
		return
	}
	enc.encodeInto(fe, reflect.ValueOf(key))
}

func decodeTypedKey[Key any](t *TableDef, raw []byte) Key {
	if t.keyEnc.uintKeyBits != 0 {
		n := t.decodeUintKey(raw)
		var key Key
		switch unsafe.Sizeof(key) {
		case 1:
			*(*uint8)(unsafe.Pointer(&key)) = uint8(n)
		case 2:
			*(*uint16)(unsafe.Pointer(&key)) = uint16(n)
		case 4:
			*(*uint32)(unsafe.Pointer(&key)) = uint32(n)
		case 8:
			*(*uint64)(unsafe.Pointer(&key)) = n
		}
		return key
	}
	if t.keyEnc.scalarKind == reflect.String {
		var scratch [1][]byte
		tup, err := decodeTupleInto(scratch[:0], raw)
		if err != nil || len(tup) != 1 {
			panic(fmt.Errorf("%s: invalid string primary key %x: %v", t.name, raw, err))
		}
		var key Key
		// Own the string just as the ordinary flat decoder does. The key can
		// safely outlive the transaction, independently of its borrowed record.
		*(*string)(unsafe.Pointer(&key)) = string(tup[0])
		return key
	}
	return decodeReflectedKey[Key](t, raw)
}

func decodeReflectedKey[Key any](t *TableDef, raw []byte) Key {
	var key Key
	t.DecodeKeyValInto(reflect.ValueOf(&key).Elem(), raw)
	return key
}
