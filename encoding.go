package edb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

type encodingMethod int

const (
	MsgPack encodingMethod = iota
	JSON

	defaultValueEncoding = MsgPack
)

func (enc encodingMethod) EncodeValue(buf []byte, objVal reflect.Value) []byte {
	switch enc {
	case MsgPack:
		bb := bytesBuilder{buf}
		enc := msgpack.GetEncoder()
		enc.ResetDict(&bb, nil)
		enc.SetSortMapKeys(true)
		err := enc.EncodeValue(objVal)
		msgpack.PutEncoder(enc)
		if err != nil {
			panic(fmt.Errorf("failed to encode %T using MsgPack: %w", objVal.Interface(), err))
		}
		return bb.Buf
	case JSON:
		raw, err := json.Marshal(objVal.Interface())
		if err != nil {
			panic(fmt.Errorf("failed to encode %T to JSON: %w", objVal.Interface(), err))
		}
		return appendRaw(buf, raw)
	default:
		panic("unsupported encoding")
	}
}

func (enc encodingMethod) DecodeValue(buf []byte, objPtrVal reflect.Value) error {
	switch enc {
	case MsgPack:
		var r bytes.Reader
		r.Reset(buf)
		dec := msgpack.GetDecoder()
		dec.ResetDict(&r, nil)
		err := dec.DecodeValue(objPtrVal)
		msgpack.PutDecoder(dec)
		if err != nil {
			return dataErrf(buf, 0, err, "failed to decode msgpack into %T", objPtrVal.Interface())
		}
		return nil
	case JSON:
		err := json.Unmarshal(buf, objPtrVal.Interface())
		if err != nil {
			return dataErrf(buf, 0, err, "failed to decode JSON into %T", objPtrVal.Interface())
		}
		return nil
	default:
		panic("unsupported encoding")
	}
}
