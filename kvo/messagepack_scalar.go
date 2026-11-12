package kvo

import (
	"reflect"
	"sync"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
)

var messagePackScalars = struct {
	sync.Mutex
	decoders map[reflect.Type]func(msgpack.View) values.Value
	compiled map[reflect.Type]bool
}{decoders: make(map[reflect.Type]func(msgpack.View) values.Value), compiled: make(map[reflect.Type]bool)}

// RegisterMessagePackScalar supplies direct access for a Go scalar with custom
// MessagePack decoding. Register during initialization, before compiling any
// binding/projection of this type. The function must match the Go decoder,
// including historical encodings and nil, and must not mutate borrowed bytes.
// It returns the same normalized values.Value as Get. Encoding and editing
// continue to use the Go type's normal MessagePack encoder.
func RegisterMessagePackScalar(t reflect.Type, decode func(msgpack.View) values.Value) {
	if t == nil || decode == nil {
		panic("record: invalid MessagePack scalar decoder")
	}
	k := t.Kind()
	if k != reflect.String && k != reflect.Bool && !(k >= reflect.Int && k <= reflect.Float64) {
		panic("record: MessagePack decoder requires a scalar type")
	}
	messagePackScalars.Lock()
	defer messagePackScalars.Unlock()
	if messagePackScalars.compiled[t] || messagePackScalars.decoders[t] != nil {
		panic("record: MessagePack scalar decoder must be registered once before binding")
	}
	messagePackScalars.decoders[t] = decode
}
func messagePackScalarDecoder(t reflect.Type) func(msgpack.View) values.Value {
	messagePackScalars.Lock()
	defer messagePackScalars.Unlock()
	messagePackScalars.compiled[t] = true
	return messagePackScalars.decoders[t]
}
