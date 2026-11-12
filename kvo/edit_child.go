package kvo

import (
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func (e *Editor) checkAttached() {
	for p := e; p != nil; p = p.parent {
		if p.detached {
			panic("record: child editor was replaced or deleted")
		}
	}
}
func (e *Editor) detachChild(key uint64) {
	if c := e.children[key]; c != nil {
		c.detached = true
		delete(e.children, key)
	}
}
func (e *Editor) attachChild(key uint64, child *Editor) *Editor {
	if e.children == nil {
		e.children = make(map[uint64]*Editor)
	}
	e.children[key] = child
	child.parent, child.parentKey = e, key
	return child
}
func (e *Editor) Model() *Model {
	if e.view.plan != nil {
		return e.view.plan.model
	}
	if e.view.layout != nil {
		return e.view.layout.model
	}
	return nil
}

// Edit opens an attached child editor, or returns nil for an absent/null child.
// Merely opening an editor does not create or mutate the child.
func (e *Editor) Edit(p *PropRef[Record]) *Editor { return e.editChild(p.Code()) }

// New explicitly creates an empty child. It panics if the child already exists.
func (e *Editor) New(p *PropRef[Record]) *Editor {
	if e.editChild(p.Code()) != nil {
		panic("record: child already exists")
	}
	return e.newChild(p.Code())
}
func (e *Editor) EditOrCreate(p *PropRef[Record]) *Editor {
	if c := e.editChild(p.Code()); c != nil {
		return c
	}
	return e.newChild(p.Code())
}

func (e *Editor) childView(key uint64) recordView {
	model := e.Model().MustPropByCode(key).TypeModel()
	if model == nil {
		panic("record: property is not a record")
	}
	if f := e.pendingField(key); f != nil {
		if f.child != nil {
			return f.child.currentView()
		}
		if !e.view.msg.Missing() {
			if len(f.raw) == 0 || msgpack.Trusted(f.raw).Nil() {
				return recordView{}
			}
			return messagePackChildValue(e.view.plan.field(key).plan, msgpack.Trusted(f.raw))
		}
		return encodedView(row2.Trusted(f.value.AsBytes())).WithModel(model)
	}
	if !e.view.msg.Missing() {
		f := e.view.plan.field(key)
		raw := e.view.messagePackField(f)
		if raw.Nil() {
			return recordView{}
		}
		return messagePackChildValue(f.plan, raw)
	}
	r := e.view
	if e.wire != nil {
		r.wire = e.wire.Snapshot()
	}
	return r.Child(row2.Step{Key: key, Options: e.view.propertyWriteOptions(key)})
}
func (e *Editor) editChild(key uint64) *Editor {
	e.checkAttached()
	if child := e.children[key]; child != nil {
		return child
	}
	r := e.childView(key)
	if r.Missing() {
		return nil
	}
	return e.attachChild(key, r.Edit())
}
func (e *Editor) emptyChild(key uint64) *Editor {
	model := e.Model().MustPropByCode(key).TypeModel()
	if model == nil {
		panic("record: property is not a record")
	}
	if !e.view.msg.Missing() {
		p := indirectPlan(e.view.plan.field(key).plan)
		if p.msgFallback {
			return nativeRecord(p, reflect.New(p.typ)).Edit()
		}
		return (&recordView{plan: p, msg: msgpack.Trusted(emptyMessagePackObject)}).Edit()
	}
	layout := row2.Fast2
	if e.view.propertyWriteOptions(key)&row2.Compact != 0 {
		layout = row2.Compact2
	}
	return Encoded(row2.Empty(layout)).WithModel(model).Edit()
}
func (e *Editor) newChild(key uint64) *Editor {
	e.checkAttached()
	if e.view.object != nil {
		f := e.view.plan.field(key)
		dst := reflect.NewAt(f.plan.typ, f.access.Address(e.view.object)).Elem()
		switch f.plan.kind {
		case nativePointer:
			dst.Set(reflect.New(f.plan.typ.Elem()))
		case nativeOption:
			dst.SetZero()
			dst.Field(1).SetBool(true)
		default:
			panic("record: native child cannot be absent")
		}
		e.touch()
		return e.editChild(key)
	}
	child := e.emptyChild(key)
	if e.view.memory != nil {
		e.FastSetPathValue([]row2.Step{{Key: key, Options: row2.Present}}, values.Bytes(child.Snapshot().Raw()))
		return e.editChild(key)
	}
	e.attachChild(key, child)
	child.touch()
	return child
}

func (e *Editor) setNested(path []row2.Step, value values.Value) error {
	child := e.editChild(path[0].Key)
	if child != nil {
		return child.TryFastSetPathValue(path[1:], value)
	}
	if value.R == 0 && path[len(path)-1].Options&row2.Present == 0 {
		return nil
	}
	child = e.emptyChild(path[0].Key)
	if err := child.TryFastSetPathValue(path[1:], value); err != nil {
		return err
	}
	e.attachChild(path[0].Key, child)
	child.touch()
	return nil
}
