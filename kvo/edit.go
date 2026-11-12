package kvo

import (
	"bytes"
	"slices"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type packedEditMode uint8

const (
	ownedPackedEdit packedEditMode = iota
	sharedPackedEdit
)

func (m packedEditMode) String() string {
	switch m {
	case ownedPackedEdit:
		return "owned"
	case sharedPackedEdit:
		return "shared"
	default:
		return "invalid"
	}
}

type editField struct {
	step       row2.Step
	encoding   row2.Encoding
	value      values.Value
	raw        []byte
	child      *Editor
	collection collectionEdit
}

func compareEditField(f editField, key uint64) int {
	if f.step.Key < key {
		return -1
	}
	if f.step.Key > key {
		return 1
	}
	return 0
}
func (e *Editor) pendingField(key uint64) *editField {
	i, ok := slices.BinarySearchFunc(e.pending, key, compareEditField)
	if !ok {
		return nil
	}
	return &e.pending[i]
}
func (e *Editor) stage(f editField) {
	if f.collection == nil {
		e.detachCollection(f.step.Key)
	}
	if f.child == nil {
		e.detachChild(f.step.Key)
	}
	i, found := slices.BinarySearchFunc(e.pending, f.step.Key, compareEditField)
	if found {
		e.pending[i] = f
	} else {
		e.pending = slices.Insert(e.pending, i, f)
	}
	e.touch()
}
func (e *Editor) touch() {
	e.changes++
	e.cachedMessagePack = nil
	e.cachedPacked = row2.View{}
	if e.onChange != nil {
		e.onChange()
		return
	}
	if e.parent != nil {
		if e.parent.wire != nil || !e.parent.view.msg.Missing() {
			e.parent.stage(editField{step: row2.Step{Key: e.parentKey, Options: row2.Present}, encoding: row2.Data, child: e})
		} else {
			e.parent.touch()
		}
	}
}
func (e *Editor) messagePackField(f *nativeField) msgpack.View {
	if pending := e.pendingField(f.key); pending != nil {
		return msgpack.Trusted(e.messagePackEditField(*pending))
	}
	return e.view.messagePackField(f)
}
func (e *Editor) currentView() recordView {
	e.checkAttached()
	r := e.view
	if e.wire != nil {
		r.wire = e.Snapshot()
	} else if !r.msg.Missing() {
		r.msg = msgpack.Trusted(e.MessagePack())
	}
	return r
}

// Record returns the current read view. Encoded edits produce a cached immutable
// snapshot; native edits still refer to their native object. Clone for isolation.
func (e *Editor) Record() Record { return e.currentView().compact() }

func (e *Editor) stagedMessagePack() []byte {
	if e.view.msg.Missing() {
		return nil
	}
	if len(e.pending) == 0 {
		return e.view.msg.Raw()
	}
	if e.cachedMessagePack != nil {
		return e.cachedMessagePack
	}
	var scratch [16]msgpack.FieldEdit
	edits := scratch[:0]
	for _, f := range e.pending {
		field := e.view.plan.field(f.step.Key)
		raw := e.messagePackEditField(f)
		if field.msgInline {
			names, _ := messagePackFields(field.plan.typ, make(map[string]bool))
			for _, name := range names {
				edits = append(edits, msgpack.FieldEdit{Name: name, Value: msgpack.Trusted(raw).Lookup(name).Raw()})
			}
		} else {
			edits = append(edits, msgpack.FieldEdit{Name: field.msgName, Value: raw})
		}
	}
	e.cachedMessagePack = e.view.msg.Apply(edits).Raw()
	return e.cachedMessagePack
}

func (e *Editor) stagedPacked() row2.View {
	base := e.wire.Snapshot()
	if len(e.pending) == 0 {
		return base
	}
	if !e.cachedPacked.Missing() {
		return e.cachedPacked
	}
	capacity := len(base.Raw())
	for _, f := range e.pending {
		capacity += int(row2.PayloadSize(editFieldValue(f))) + 16
	}
	w := row2.AcquireWriter(make([]byte, 0, capacity))
	defer w.Release()
	w.Begin(0, "", row2.Object, base.Layout())
	put := func(f editField) {
		value := editFieldValue(f)
		if f.step.Options&row2.Present != 0 {
			w.PutPresent(f.step.Key, "", value, f.encoding)
		} else {
			w.Put(f.step.Key, "", value, f.encoding)
		}
	}
	i := 0
	it := base.Iter()
	for {
		key, _, field, ok := it.Next()
		if !ok {
			break
		}
		for i < len(e.pending) && e.pending[i].step.Key < key {
			put(e.pending[i])
			i++
		}
		if i < len(e.pending) && e.pending[i].step.Key == key {
			put(e.pending[i])
			i++
		} else {
			w.PutRaw(key, "", field)
		}
	}
	for ; i < len(e.pending); i++ {
		put(e.pending[i])
	}
	w.Finish(true)
	e.cachedPacked = row2.Trusted(w.BytesWritten())
	return e.cachedPacked
}

// flush transfers pending edits to the codec-specific editor for callers using
// the advanced packed editor or the older nested path API.
func (e *Editor) flush() {
	if len(e.pending) == 0 {
		return
	}
	if e.wire != nil {
		*e.wire = *row2.Edit(e.Snapshot())
	} else {
		e.view.msg = msgpack.Trusted(e.MessagePack())
	}
	e.pending = e.pending[:0]
}

func (e *Editor) setPackedField(step row2.Step, value values.Value, enc row2.Encoding) {
	if e.packedMode == sharedPackedEdit {
		e.detachCollection(step.Key)
		e.detachChild(step.Key)
		e.wire.Set([]row2.Step{step}, value, enc)
		e.touch()
		return
	}
	old := e.GetAs([]row2.Step{step}, enc)
	present := e.wire.Snapshot().Has(step.Key)
	if p := e.pendingField(step.Key); p != nil {
		present = editFieldValue(*p).R != 0 || p.step.Options&row2.Present != 0
	}
	wantPresent := value.R != 0 || step.Options&row2.Present != 0
	if present == wantPresent && old.R == value.R && (old.P == value.P || old.P != nil && value.P != nil && bytes.Equal(old.AsBytes(), value.AsBytes())) {
		return
	}
	if value.P != nil {
		value = values.Bytes(bytes.Clone(value.AsBytes()))
	}
	e.stage(editField{step: step, encoding: enc, value: value})
}

func editFieldValue(f editField) values.Value {
	if f.collection != nil {
		return f.collection.packed()
	}
	if f.child != nil {
		return values.Bytes(f.child.Snapshot().Raw())
	}
	return f.value
}

func (e *Editor) messagePackEditField(f editField) []byte {
	if f.collection != nil {
		return f.collection.messagePack()
	}
	if f.child == nil {
		return f.raw
	}
	raw := f.child.MessagePack()
	if raw == nil {
		if f.child.view.object == nil {
			panic("record: incompatible child editor codec")
		}
		raw = msgpack.Encode(nil, f.child.view.objectValue())
	}
	p := e.view.plan.field(f.step.Key).plan
	if p.kind == nativeOption {
		return msgpack.Trusted(emptyMessagePackObject).Apply([]msgpack.FieldEdit{{Name: "Value", Value: raw}, {Name: "Present", Value: []byte{0xc3}}}).Raw()
	}
	return raw
}
