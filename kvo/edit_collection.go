package kvo

import (
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type collectionEdit interface {
	packed() values.Value
	messagePack() []byte
	detach()
}

func (e *Editor) detachCollection(key uint64) {
	if c := e.collections[key]; c != nil {
		c.detach()
		delete(e.collections, key)
	}
}
func (e *Editor) registerCollection(key uint64, c collectionEdit) {
	if e.collections == nil {
		e.collections = make(map[uint64]collectionEdit)
	}
	e.collections[key] = c
}
func (e *Editor) collectionChanged(key uint64, c collectionEdit) {
	if e.view.object != nil {
		e.touch()
	} else if e.view.memory != nil {
		// Generic-memory aggregates use owned encoded spans.
		e.view.memory.set([]row2.Step{{Key: key, Options: row2.Present}}, c.packed(), row2.Data)
		e.touch()
	} else if e.packedMode == sharedPackedEdit {
		e.wire.Set([]row2.Step{{Key: key, Options: row2.Present}}, c.packed(), row2.Data)
		e.touch()
	} else {
		e.stage(editField{step: row2.Step{Key: key, Options: row2.Present}, encoding: row2.Data, collection: c})
	}
}

// collectionValue reads just this property, never serializing its parent.
func (e *Editor) collectionValue(key uint64, elem AnyType) ListValue {
	e.checkAttached()
	enc := Encoding(elem)
	if f := e.pendingField(key); f != nil {
		if f.collection != nil {
			return ListValue{data: f.collection.packed(), encoding: enc, kind: compactRepresentation}
		}
		if !e.view.msg.Missing() {
			return messagePackCollection(e.view.plan.field(key).plan, msgpack.Trusted(f.raw), enc)
		}
		return ListValue{data: f.value, encoding: enc, kind: compactRepresentation}
	}
	if e.view.plan != nil {
		f := e.view.plan.field(key)
		if e.view.object != nil {
			return ListValue{data: f.access.Get(e.view.object), plan: f.plan, encoding: enc, kind: nativeRepresentation}
		}
		if !e.view.msg.Missing() {
			return messagePackCollection(f.plan, e.view.messagePackField(f), enc)
		}
	}
	return ListValue{data: e.GetValue(key), encoding: enc, kind: compactRepresentation}
}

func messagePackCollection(p *nativePlan, raw msgpack.View, enc row2.Encoding) ListValue {
	if p.msgFallback {
		// Keep the selected container's native carrier: its custom codec and
		// unmodeled fields must survive edits without decoding the parent.
		v := decodeProperty(p, raw)
		return ListValue{data: values.Value{P: v.Addr().UnsafePointer()}, plan: p, encoding: enc, kind: nativeRepresentation}
	}
	return ListValue{data: values.Bytes(raw.Raw()), plan: p, encoding: enc, kind: messagePackRepresentation}
}

func nativeCollectionAssignment(p *nativePlan, v values.Value) reflect.Value {
	r := reflect.New(p.typ).Elem()
	p.read(r, v, true, 0)
	return r
}
