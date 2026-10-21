package edb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

type encodingMethod int

const (
	MsgPack encodingMethod = iota
	JSON

	defaultValueEncoding = MsgPack
)

type msgpackEncoderState struct {
	encoder *msgpack.Encoder
	buffer  bytesBuilder
}

var msgpackEncoderPool = sync.Pool{
	New: func() any {
		state := &msgpackEncoderState{}
		state.encoder = msgpack.NewEncoder(&state.buffer)
		return state
	},
}

type msgpackDecoderState struct {
	decoder *msgpack.Decoder
	reader  bytes.Reader
}

var msgpackDecoderPool = sync.Pool{
	New: func() any {
		state := &msgpackDecoderState{}
		state.decoder = msgpack.NewDecoder(&state.reader)
		return state
	},
}

func (enc encodingMethod) EncodeValue(buf []byte, objVal reflect.Value) []byte {
	switch enc {
	case MsgPack:
		state := msgpackEncoderPool.Get().(*msgpackEncoderState)
		state.buffer.Buf = buf
		state.encoder.SetSortMapKeys(true)
		err := state.encoder.EncodeValue(objVal)
		raw := state.buffer.Buf
		state.buffer.Buf = nil
		state.encoder.ResetDict(&state.buffer, nil)
		msgpackEncoderPool.Put(state)
		if err != nil {
			panic(fmt.Errorf("failed to encode %T using MsgPack: %w", objVal.Interface(), err))
		}
		return raw
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
		state := msgpackDecoderPool.Get().(*msgpackDecoderState)
		state.reader.Reset(buf)
		err := state.decoder.DecodeValue(objPtrVal)
		state.reader.Reset(nil)
		state.decoder.ResetDict(&state.reader, nil)
		msgpackDecoderPool.Put(state)
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
