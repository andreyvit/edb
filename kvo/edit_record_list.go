package kvo

import (
	"iter"
	"reflect"
	"slices"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// RecordListEditor keeps element editors attached until the list is replaced or
// structurally changed. Append/RemoveAt/Clear invalidate existing element editors.
type RecordListEditor struct {
	owner             *Editor
	prop              *RecordListPropRef
	base              RecordList
	items             []Record
	loaded            bool
	children          map[int]*Editor
	detached          bool
	plan              *nativePlan
	native            reflect.Value
	cached            values.Value
	cachedMessagePack []byte
}

func (p *RecordListPropRef) editList(e *Editor) *RecordListEditor {
	e.checkAttached()
	if l := e.collections[p.Code()]; l != nil {
		return l.(*RecordListEditor)
	}
	v := e.collectionValue(p.Code(), p.entity)
	l := &RecordListEditor{owner: e, prop: p, base: RecordList{p.access.wrap(v)}, plan: v.plan}
	if v.kind == nativeRepresentation {
		l.native = v.native()
	}
	e.registerCollection(p.Code(), l)
	return l
}
func (l *RecordListEditor) check() {
	l.owner.checkAttached()
	if l.detached {
		panic("record: list editor was replaced or deleted")
	}
}
func (l *RecordListEditor) invalidate() {
	for _, c := range l.children {
		c.detached = true
	}
	clear(l.children)
}
func (l *RecordListEditor) detach() { l.detached = true; l.invalidate() }
func (l *RecordListEditor) load() {
	l.check()
	if !l.loaded {
		l.items = l.base.Slice()
		l.loaded = true
	}
}
func (l *RecordListEditor) changed() {
	l.cached = values.Value{}
	l.cachedMessagePack = nil
	l.owner.collectionChanged(l.prop.Code(), l)
}
func (l *RecordListEditor) Len() int {
	l.check()
	if l.loaded {
		return len(l.items)
	}
	return l.base.Len()
}
func (l *RecordListEditor) Get(index int) Record {
	l.load()
	if c := l.children[index]; c != nil {
		return c.Record()
	}
	return l.items[index]
}
func (l *RecordListEditor) Iter() iter.Seq[Record] {
	return func(yield func(Record) bool) {
		l.check()
		for i := range l.Len() {
			if !yield(l.Get(i)) {
				return
			}
		}
	}
}
func (l *RecordListEditor) Edit(index int) *Editor {
	l.load()
	if c := l.children[index]; c != nil {
		return c
	}
	r := l.items[index]
	if r.Missing() {
		return nil
	}
	c := r.Edit()
	if l.children == nil {
		l.children = make(map[int]*Editor)
	}
	l.children[index] = c
	c.parent = l.owner
	c.onChange = func() { l.check(); l.changed() }
	return c
}
func (l *RecordListEditor) materializeEdits() {
	l.load()
	for i, c := range l.children {
		l.items[i] = c.Record()
	}
}
func (l *RecordListEditor) appendNative(r Record) {
	v := assignRecordValue(l.plan.elem, r)
	l.native.Set(reflect.Append(l.native, v))
}
func (l *RecordListEditor) refreshNative() { l.items = l.base.Slice(); l.loaded = true }
func (l *RecordListEditor) Append(r Record) {
	l.check()
	if r.Missing() || r.Model() != l.prop.entity.Model() {
		panic("record: nonmissing record of the list model required")
	}
	r = r.Clone()
	l.materializeEdits()
	if l.native.IsValid() {
		l.appendNative(r)
	}
	l.invalidate()
	if l.native.IsValid() {
		l.refreshNative()
	} else {
		l.items = append(l.items, r)
	}
	l.changed()
}
func (l *RecordListEditor) New() *Editor {
	l.check()
	var r Record
	if l.plan != nil {
		p := indirectPlan(l.plan.elem)
		if l.native.IsValid() || p.msgFallback {
			r = nativeRecord(p, reflect.New(p.typ)).compact()
		} else {
			r = planMessagePack(p, emptyMessagePackObject)
		}
	} else {
		r = Encoded(row2.Empty(row2.Fast2)).WithModel(l.prop.entity.Model())
	}
	l.Append(r)
	return l.Edit(l.Len() - 1)
}
func (l *RecordListEditor) RemoveAt(index int) {
	l.materializeEdits()
	if index < 0 || index >= len(l.items) {
		panic("record: list index out of range")
	}
	l.invalidate()
	if l.native.IsValid() {
		reflect.Copy(l.native.Slice(index, l.native.Len()-1), l.native.Slice(index+1, l.native.Len()))
		l.native.Index(l.native.Len() - 1).SetZero()
		l.native.SetLen(l.native.Len() - 1)
		l.refreshNative()
	} else {
		l.items = slices.Delete(l.items, index, index+1)
	}
	l.changed()
}
func (l *RecordListEditor) Clear() {
	l.load()
	l.invalidate()
	l.items = nil
	if l.native.IsValid() {
		l.native.Set(reflect.MakeSlice(l.native.Type(), 0, 0))
	}
	l.changed()
}
func (l *RecordListEditor) packed() values.Value {
	if l.cached.P != nil {
		return l.cached
	}
	w := row2.AcquireWriter(nil)
	defer w.Release()
	w.Begin(0, "", row2.List, row2.Compact2)
	for r := range l.Iter() {
		w.PutPresent(0, "", values.Bytes(r.Snapshot().Raw()), row2.Data)
	}
	w.Finish(true)
	l.cached = values.Bytes(w.BytesWritten())
	return l.cached
}
func (l *RecordListEditor) messagePack() []byte {
	if l.cachedMessagePack == nil && l.native.IsValid() {
		l.cachedMessagePack = msgpack.Encode(nil, l.native)
	}
	if l.cachedMessagePack == nil {
		l.cachedMessagePack = msgpack.BuildArray(l.Len()*32, func(yield func(msgpack.View) bool) {
			for r := range l.Iter() {
				raw := encodeRecordMessagePack(l.plan.elem, r)
				if !yield(msgpack.Trusted(raw)) {
					return
				}
			}
		}).Raw()
	}
	return l.cachedMessagePack
}
