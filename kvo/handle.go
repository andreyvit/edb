package kvo

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Record is a 16-byte typed cell: a GC-visible data pointer plus a physical
// representation ID and byte length. Model and binding metadata are shared.
// Encoded views borrow their input; Own copies only when ownership is needed.
// A database primary key is available only when stored as a modeled property.
type Record struct{ value values.Value }

type representationKind uint8

const (
	fastRepresentation representationKind = iota
	compactRepresentation
	messagePackRepresentation
	nativeRepresentation
	memoryRepresentation
	ownedFastRepresentation
	ownedCompactRepresentation
	ownedMessagePackRepresentation
	representationCount
)

type recordRepresentation struct {
	get     func(*recordRepresentation, unsafe.Pointer, uint32, uint64) values.Value
	plan    *nativePlan
	layout  *modelLayout
	binding *Binding
	set     *representationSet
	kind    representationKind
}

type representationSet struct {
	once    sync.Once
	ids     [representationCount]values.RepresentationID
	plan    *nativePlan
	layout  *modelLayout
	binding *Binding
}

var recordRepresentations descriptorTable[recordRepresentation]
var unmodeledRepresentations representationSet
var unmodeledType = values.NewType("record", values.Pair)

func (b *Binding) initRepresentations() {
	b.representations = &representationSet{plan: b.root, binding: b}
}

// Type is the logical record identity, shared by all physical representations.
func Type(model *Model) *values.Type {
	if model == nil {
		return unmodeledType
	}
	return layoutFor(model).valueType
}

func (s *representationSet) init() {
	layout := s.layout
	if s.plan != nil {
		layout = s.plan.layout
	}
	typ := unmodeledType
	if layout != nil {
		typ = layout.valueType
	}
	shape := &values.Shape{Type: typ, Kind: values.TypedPairKind, Cells: 1, PointerLike: true}
	var entries [representationCount]recordRepresentation
	for kind := representationKind(0); kind < representationCount; kind++ {
		e := recordRepresentation{plan: s.plan, layout: layout, binding: s.binding, set: s, kind: kind}
		switch kind {
		case fastRepresentation, ownedFastRepresentation:
			e.get = getFast
		case compactRepresentation, ownedCompactRepresentation:
			e.get = getCompact
		case messagePackRepresentation, ownedMessagePackRepresentation:
			e.get = getMessagePack
		case nativeRepresentation:
			e.get = getNative
		case memoryRepresentation:
			e.get = getMemory
		}
		entries[kind] = e
	}
	recordRepresentations.register(shape, entries[:], s.ids[:])
}

func (s *representationSet) value(kind representationKind, p unsafe.Pointer, n uint32) Record {
	s.once.Do(s.init)
	return Record{values.TypedPair(s.ids[kind], p, n)}
}

func (r Record) representation() *recordRepresentation {
	return &(*recordRepresentations.entries.Load())[r.value.Representation()]
}

func (r recordView) compact() Record {
	s := &unmodeledRepresentations
	if r.binding != nil {
		s = r.binding.representations
	} else if r.plan != nil {
		s = r.plan.representations
	} else if r.layout != nil {
		s = r.layout.representations
	}
	if r.memory != nil {
		return s.value(memoryRepresentation, unsafe.Pointer(r.memory), 0)
	}
	if r.object != nil {
		return s.value(nativeRepresentation, r.object, 0)
	}
	data, kind := r.wire.Raw(), fastRepresentation
	if !r.msg.Missing() {
		data, kind = r.msg.Raw(), messagePackRepresentation
	} else if !r.wire.Missing() && r.wire.Layout() != row2.Fast2 {
		kind = compactRepresentation
	}
	if uint64(len(data)) > math.MaxUint32 {
		panic("record: encoded value exceeds 4 GiB")
	}
	if len(data) == 0 {
		data = nil
	}
	return s.value(kind, unsafe.Pointer(unsafe.SliceData(data)), uint32(len(data)))
}

// expanded is a temporary stack view for cold operations, never a per-record
// backing allocation. Hot reads dispatch directly through the flat table.
func (r Record) expanded() recordView {
	if r.value == (values.Value{}) {
		return recordView{}
	}
	e := r.representation()
	v := recordView{plan: e.plan, layout: e.layout, binding: e.binding}
	switch e.kind {
	case nativeRepresentation:
		v.object = r.value.P
	case memoryRepresentation:
		v.memory = (*memoryObject)(r.value.P)
	case messagePackRepresentation, ownedMessagePackRepresentation:
		v.msg = msgpack.Trusted(unsafe.Slice((*byte)(r.value.P), r.value.Auxiliary()))
	default:
		v.wire = row2.Trusted(unsafe.Slice((*byte)(r.value.P), r.value.Auxiliary()))
	}
	return v
}

func getFast(e *recordRepresentation, p unsafe.Pointer, n uint32, key uint64) values.Value {
	v := row2.Trusted(unsafe.Slice((*byte)(p), n))
	if e.layout == nil || !e.layout.decodeScalars {
		return v.GetFast(key)
	}
	return v.Get(key, e.layout.encoding(key))
}
func getCompact(e *recordRepresentation, p unsafe.Pointer, n uint32, key uint64) values.Value {
	if e.layout == nil {
		panic("record: typed access requires a model; use GetAs")
	}
	v := row2.Trusted(unsafe.Slice((*byte)(p), n))
	if enc := e.layout.encodingSlot(key); enc != 0 {
		return v.RawField(key).Decode(row2.Encoding(enc - 1))
	}
	return v.RawField(key).Decode(e.layout.fieldSlow(key).encoding)
}
func getMessagePack(e *recordRepresentation, p unsafe.Pointer, n uint32, key uint64) values.Value {
	return (recordView{plan: e.plan, msg: msgpack.Trusted(unsafe.Slice((*byte)(p), n))}).messagePackGet(key)
}
func getNative(e *recordRepresentation, p unsafe.Pointer, _ uint32, key uint64) values.Value {
	f := e.plan.field(key)
	v := f.access.Get(p)
	if f.plan.kind <= nativeFloat || f.plan.kind == nativeString {
		return v
	}
	return nativeFieldValue(f.plan, v, f.layout)
}
func getMemory(_ *recordRepresentation, p unsafe.Pointer, _ uint32, key uint64) values.Value {
	return (*memoryObject)(p).get(key)
}

func wireRecord(s *representationSet, v row2.View) Record {
	kind := compactRepresentation
	if v.Layout() == row2.Fast2 {
		kind = fastRepresentation
	}
	return spanRecord(s, kind, v.Raw())
}
func spanRecord(s *representationSet, kind representationKind, data []byte) Record {
	if uint64(len(data)) > math.MaxUint32 {
		panic("record: encoded value exceeds 4 GiB")
	}
	if len(data) == 0 {
		data = nil
	}
	return s.value(kind, unsafe.Pointer(unsafe.SliceData(data)), uint32(len(data)))
}
func Encoded(v row2.View) Record                    { return wireRecord(&unmodeledRepresentations, v) }
func (b *Binding) Encoded(v row2.View) Record       { return wireRecord(b.representations, v) }
func (b *Binding) MessagePack(raw []byte) Record    { return planMessagePack(b.root, raw) }
func (p *Projection) MessagePack(raw []byte) Record { return planMessagePack(p.root, raw) }
func planMessagePack(p *nativePlan, raw []byte) Record {
	if p.msgFallback || p.hasMessagePackInlineAlias(msgpack.Trusted(raw)) {
		return messagePackRecord(p, raw).compact()
	}
	return spanRecord(p.representations, messagePackRepresentation, raw)
}
func (b *Binding) Record(ptr reflect.Value) Record    { return planRecord(b.root, ptr) }
func (p *Projection) Record(ptr reflect.Value) Record { return planRecord(p.root, ptr) }
func planRecord(p *nativePlan, ptr reflect.Value) Record {
	if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Type().Elem() != p.typ {
		panic(row2.ErrType)
	}
	return p.representations.value(nativeRepresentation, ptr.UnsafePointer(), 0)
}

func (r Record) GetValue(key uint64) values.Value {
	if r.value.P == nil {
		return values.Value{}
	}
	e := r.representation()
	return e.get(e, r.value.P, r.value.Auxiliary(), key)
}
func (r Record) Missing() bool { return r.value.P == nil }
func (r Record) GetAs(key uint64, enc row2.Encoding) values.Value {
	return r.expanded().GetAs(key, enc)
}
func (r Record) Has(key uint64) bool { return r.expanded().Has(key) }
func (r Record) Child(step row2.Step) Record {
	child := r.expanded().Child(step).compact()
	if r.OwnsData() {
		child = child.markOwned()
	}
	return child
}
func (r Record) Snapshot() row2.View { return r.expanded().Snapshot() }

// Model returns the logical model independently of native binding or encoding.
func (r Record) Model() *Model {
	if r.value == (values.Value{}) {
		return nil
	}
	e := r.representation()
	if e.layout != nil {
		return e.layout.model
	}
	return nil
}

// MessagePack returns the borrowed original payload, or nil for another
// representation. It preserves unmodeled fields without re-encoding.
func (r Record) MessagePack() []byte {
	if r.Missing() {
		return nil
	}
	k := r.representation().kind
	if k != messagePackRepresentation && k != ownedMessagePackRepresentation {
		return nil
	}
	return unsafe.Slice((*byte)(r.value.P), r.value.Auxiliary())
}

// DecodeProperty panics on error. Use TryDecodeProperty to handle errors explicitly.
func (b *Binding) DecodeProperty(dst reflect.Value, r Record, key uint64) {
	err := b.TryDecodeProperty(dst, r, key)
	if err != nil {
		panic(err)
	}
}

// TryDecodeProperty decodes one modeled property into an addressable Go value
// using the shared native binding. It does not decode the rest of the record.
func (b *Binding) TryDecodeProperty(dst reflect.Value, r Record, key uint64) error {
	return b.root.decodeProperty(dst, r, key)
}

// DecodeProperty panics on error. Use TryDecodeProperty to handle errors explicitly.
func (p *Projection) DecodeProperty(dst reflect.Value, r Record, key uint64) {
	err := p.TryDecodeProperty(dst, r, key)
	if err != nil {
		panic(err)
	}
}

func (p *Projection) TryDecodeProperty(dst reflect.Value, r Record, key uint64) error {
	return p.root.decodeProperty(dst, r, key)
}
func (p *nativePlan) decodeProperty(dst reflect.Value, r Record, key uint64) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = recoveredError(fmt.Sprintf("record property %d", key), v)
		}
	}()
	f := p.field(key)
	if !dst.CanSet() || dst.Type() != f.plan.typ {
		return row2.ErrType
	}
	f.plan.read(dst, r.GetValue(key), r.Has(key), 0)
	return nil
}

// NativeFieldIndex resolves a direct Go field at table setup. It returns -1
// for a property inside an inlined struct, which cannot be the table key field.
func (b *Binding) NativeFieldIndex(key uint64) int    { return b.root.field(key).index }
func (p *Projection) NativeFieldIndex(key uint64) int { return p.root.field(key).index }
func (r Record) NativeModel() *Model {
	if r.value == (values.Value{}) {
		return nil
	}
	if p := r.representation().plan; p != nil {
		return p.model
	}
	return nil
}
func (r Record) NativeType() *values.Type { return r.expanded().NativeType() }
func (r Record) Native() any              { return r.expanded().Native() }
func (r Record) Edit() *Editor            { return r.expanded().Edit() }
func (r Record) WithModel(m *Model) Record {
	if r.value != (values.Value{}) {
		e := r.representation()
		if e.layout != nil && e.layout.model == m {
			return r
		}
	}
	result := r.expanded().WithModel(m).compact()
	if r.OwnsData() {
		result = result.markOwned()
	}
	return result
}
func (r Record) Fast() Record {
	if r.Missing() {
		return r
	}
	k := r.representation().kind
	if k == fastRepresentation || k == ownedFastRepresentation {
		return r
	}
	return r.Fastest()
}
func (r Record) Fastest() Record {
	if r.Missing() {
		return r
	}
	k := r.representation().kind
	if k == nativeRepresentation || k == memoryRepresentation {
		return r
	}
	return r.expanded().Fastest().compact()
}
func (r Record) OwnsData() bool {
	if r.Missing() {
		return true
	}
	k := r.representation().kind
	return k == nativeRepresentation || k == memoryRepresentation || k >= ownedFastRepresentation
}
func (r Record) markOwned() Record {
	if r.Missing() || r.OwnsData() {
		return r
	}
	e := r.representation()
	return e.set.value(e.kind+ownedFastRepresentation, r.value.P, r.value.Auxiliary())
}

// Own returns a view independent of borrowed encoded storage. It copies the
// payload, including unknown properties, without decoding it. Already owned
// data is returned unchanged; this is not a deep clone of mutable Go objects.
func (r Record) Own() Record {
	if r.OwnsData() {
		return r
	}
	data := bytes.Clone(unsafe.Slice((*byte)(r.value.P), r.value.Auxiliary()))
	r.value.P = unsafe.Pointer(unsafe.SliceData(data))
	return r.markOwned()
}
func (r Record) GetOwnedString(key uint64) values.Value {
	v := r.GetValue(key)
	if !r.OwnsData() {
		return values.String(strings.Clone(v.AsString()))
	}
	return v
}

// ToJSON panics on error. Use TryToJSON to handle errors explicitly.
func (r Record) ToJSON(model *Model, options JSONOptions) map[string]any {
	v, err := r.TryToJSON(model, options)
	if err != nil {
		panic(err)
	}
	return v
}

func (r Record) TryToJSON(model *Model, options JSONOptions) (out map[string]any, err error) {
	defer func() {
		if p := recover(); p != nil {
			out = nil
			err = recoveredError("record JSON", p)
		}
	}()
	return r.expanded().ToJSON(model, options)
}

func ownValue(v values.Value, budget values.OwnershipBudget) (values.Value, error) {
	if v.P == nil {
		return v, nil
	}
	r := FromValue(v)
	if !r.OwnsData() {
		if err := budget.ChargeAllocation(uint64(v.Auxiliary())); err != nil {
			return values.Value{}, err
		}
	}
	return r.Own().Value(), nil
}

// Value transports the record in the shared cell without boxing.
func (r Record) Value() values.Value { return r.value }

// FromValue validates that a typed cell carries a registered KVO representation.
func FromValue(v values.Value) Record {
	r, ok := TryFromValue(v)
	if !ok {
		panic("record: incompatible typed value")
	}
	return r
}

// TryFromValue checks a cell's representation without interpreting its data.
func TryFromValue(v values.Value) (Record, bool) {
	if v == (values.Value{}) {
		return Record{}, true
	}
	id := v.Representation()
	p := recordRepresentations.entries.Load()
	if id == 0 || p == nil || uint64(id) >= uint64(len(*p)) || (*p)[id].set == nil {
		return Record{}, false
	}
	return Record{v}, true
}
