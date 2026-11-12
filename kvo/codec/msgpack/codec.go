// Package msgpack implements EDB's MessagePack struct encoding. Like Fast1,
// MessagePack remains supported, with migration to Fast2 or Compact2 intended.
package msgpack

import (
	"bytes"
	"reflect"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
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

// Encode panics on error. Use TryEncode to handle errors explicitly.
func Encode(dst []byte, value reflect.Value) []byte {
	v, err := TryEncode(dst, value)
	if err != nil {
		panic(err)
	}
	return v
}

func TryEncode(dst []byte, value reflect.Value) ([]byte, error) {
	state := msgpackEncoderPool.Get().(*msgpackEncoderState)
	state.buffer.Buf = dst
	state.encoder.SetSortMapKeys(true)
	err := state.encoder.EncodeValue(value)
	raw := state.buffer.Buf
	state.buffer.Buf = nil
	state.encoder.ResetDict(&state.buffer, nil)
	msgpackEncoderPool.Put(state)
	return raw, err
}

// Decode panics on error. Use TryDecode to handle errors explicitly.
func Decode(src []byte, value reflect.Value) {
	err := TryDecode(src, value)
	if err != nil {
		panic(err)
	}
}

func TryDecode(src []byte, value reflect.Value) error {
	state := msgpackDecoderPool.Get().(*msgpackDecoderState)
	state.reader.Reset(src)
	err := state.decoder.DecodeValue(value)
	state.reader.Reset(nil)
	state.decoder.ResetDict(&state.reader, nil)
	msgpackDecoderPool.Put(state)
	return err
}
