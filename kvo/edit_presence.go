package kvo

import (
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Has reports the model's presence semantics, including pending changes.
func (e *Editor) Has(p interface{ Code() uint64 }) bool {
	e.checkAttached()
	key := p.Code()
	if f := e.pendingField(key); f != nil {
		if f.child != nil || f.collection != nil {
			return true
		}
		if !e.view.msg.Missing() {
			field := e.view.plan.field(key)
			return messagePackHas(e.view.plan, field, e.messagePackField(field))
		}
		return f.step.Options&row2.Present != 0 || f.value.R != 0
	}
	if e.wire != nil {
		return e.wire.Snapshot().Has(key)
	}
	return e.view.Has(key)
}

// Delete removes an encoded property or resets its native field to zero. Native
// fields without explicit presence cannot distinguish deletion from zero.
func (e *Editor) Delete(p interface{ Code() uint64 }) {
	e.checkAttached()
	key := p.Code()
	e.detachChild(key)
	e.detachCollection(key)
	if e.view.object != nil {
		f := e.view.plan.field(key)
		reflect.NewAt(f.plan.typ, f.access.Address(e.view.object)).Elem().SetZero()
		e.touch()
	} else if !e.view.msg.Missing() {
		f := e.view.plan.field(key)
		if f.msgInline {
			panic("record: cannot delete an inline property")
		}
		e.stage(editField{step: row2.Step{Key: key}})
	} else {
		e.FastSetPathValue([]row2.Step{{Key: key}}, values.Value{})
	}
}
