package kvo

import (
	"bytes"
	"fmt"

	"reflect"
	"slices"
	"strings"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// FieldMap associates exposed properties with direct, exported Go fields.
// A type without an entry uses its registered kvo tags (or legacy edb tags). Unmapped Go fields
// stay on the decoded row and do not become script properties.
type FieldMap map[reflect.Type]map[PropCode]string

func (m FieldMap) resolve(t reflect.Type, model *Model) []NativeField {
	names, ok := m[t]
	if !ok {
		return model.NativeStruct(t).Fields()
	}
	return resolveNativeFields(t, model, names)
}

// Projection binds properties to a Go struct or its MessagePack representation.
// It does not implement the row2 codec: the model may omit stored Go fields.
type Projection struct{ root *nativePlan }

// Project shares default projections (nil fields) by Go type and model.
// Explicit field maps create independent configurations; build those once and
// retain them, just like WithGenerated and WithNativeSchema configurations.
func Project(t reflect.Type, m *Model, fields FieldMap) *Projection {
	if m == nil || t == nil || t.Kind() != reflect.Struct {
		nativePanic(t, "expected a struct and model")
	}
	if fields == nil {
		bindings.Lock()
		defer bindings.Unlock()
		key := bindingKey{t, m}
		if p := bindings.projections[key]; p != nil {
			return p
		}
		p := &Projection{nativeCompile(t, m.Type(), make(map[nativePlanKey]*nativePlan), FieldMap{})}
		bindings.projections[key] = p
		return p
	}
	return &Projection{nativeCompile(t, m.Type(), make(map[nativePlanKey]*nativePlan), fields)}
}
func (p *Projection) recordView(ptr reflect.Value) recordView { return nativeRecord(p.root, ptr) }
func (b *Binding) recordView(ptr reflect.Value) recordView    { return nativeRecord(b.root, ptr) }
func nativeRecord(p *nativePlan, ptr reflect.Value) recordView {
	if ptr.Kind() != reflect.Pointer || ptr.IsNil() || ptr.Type().Elem() != p.typ {
		panic(row2.ErrType)
	}
	return recordView{plan: p, layout: p.layout, object: ptr.UnsafePointer()}
}

// recordView addresses an encoded object or an addressable Go struct by property
// code. Children of a native record refer to fields on that same Go object.
type recordView struct {
	memory  *memoryObject
	layout  *modelLayout
	wire    row2.View
	plan    *nativePlan
	object  unsafe.Pointer
	msg     msgpack.View
	binding *Binding
}

func encodedView(v row2.View) recordView { return recordView{wire: v} }

// NativeModel identifies the attached Go binding, including on encoded records.
func (r recordView) NativeModel() *Model {
	if r.plan == nil {
		return nil
	}
	return r.plan.model
}

// Snapshot materializes the projected properties as row2 bytes. An existing
// row2 view is returned unchanged and still borrows its original storage.
func (r recordView) Snapshot() row2.View {
	if r.memory != nil {
		return r.memory.snapshot()
	}
	if !r.msg.Missing() {
		return r.messagePackSnapshot()
	}
	if r.object == nil {
		return r.wire
	}
	var w row2.Writer
	r.plan.emit(&w, r.objectValue(), 0, "", row2.Compact2, true, 0)
	return row2.Trusted(w.BytesWritten())
}
func (e *Editor) Snapshot() row2.View {
	e.checkAttached()
	if e.wire != nil {
		return e.stagedPacked()
	}
	return e.currentView().Snapshot()
}

func (r recordView) Missing() bool {
	return r.memory == nil && r.object == nil && r.msg.Missing() && r.wire.Missing()
}
func (p *nativePlan) field(key uint64) *nativeField {
	if i := key - p.base; i < uint64(len(p.index)) {
		if n := p.index[i]; n != 0 {
			return &p.fields[n-1]
		}
	}
	return p.fieldSlow(key)
}
func (p *nativePlan) fieldSlow(key uint64) *nativeField {
	i, ok := slices.BinarySearchFunc(p.fields, key, func(f nativeField, k uint64) int {
		if f.key < k {
			return -1
		}
		if f.key > k {
			return 1
		}
		return 0
	})
	if !ok {
		panic(fmt.Errorf("%v has no property %d", p.typ, key))
	}
	return &p.fields[i]
}
func unwrap(p *nativePlan, v reflect.Value, create bool) (*nativePlan, reflect.Value) {
	for p.kind == nativeOption || p.kind == nativePointer {
		if p.kind == nativeOption {
			if !v.Field(1).Bool() {
				if !create {
					return p.elem, reflect.Value{}
				}
				v.Field(1).SetBool(true)
			}
			v = v.Field(0)
		} else {
			if v.IsNil() {
				if !create {
					return p.elem, reflect.Value{}
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		p = p.elem
	}
	return p, v
}
func (r recordView) Child(s row2.Step) recordView {
	if r.memory != nil {
		return r.memory.child(s)
	}
	if r.Missing() {
		if r.plan != nil {
			return recordView{plan: indirectPlan(r.plan.field(s.Key).plan)}
		}
		return recordView{}
	}
	if !r.msg.Missing() {
		return r.messagePackChild(s)
	}
	if r.object == nil {
		child := encodedView(r.wire.Child(s))
		if r.plan != nil {
			child.plan = indirectPlan(r.plan.field(s.Key).plan)
			child.layout = child.plan.layout
		} else if r.layout != nil {
			child.layout = r.layout.field(s.Key).child
		}
		return child
	}
	f := r.plan.field(s.Key)
	p, v, ok := unwrapValue(f.plan, f.access.Get(r.object))
	if !ok {
		return recordView{plan: indirectPlan(p)}
	}
	if p.kind != nativeObject {
		panic(row2.ErrKind)
	}
	return recordView{plan: p, layout: p.layout, object: v.P}
}
func (r recordView) Get(k uint64) values.Value {
	if r.memory != nil {
		return r.memory.get(k)
	}
	if r.object != nil {
		f := r.plan.field(k)
		if f.plan.kind <= nativeFloat || f.plan.kind == nativeString {
			return f.access.Get(r.object)
		}
		return nativeFieldValue(f.plan, f.access.Get(r.object), f.layout)
	}
	if r.Missing() {
		return values.Value{}
	}
	if !r.msg.Missing() {
		return r.messagePackGet(k)
	}
	if r.wire.Layout() == row2.Fast2 && (r.layout == nil || !r.layout.decodeScalars) {
		return r.wire.GetFast(k)
	}
	if r.layout != nil {
		if enc := r.layout.encodingSlot(k); enc != 0 {
			return r.wire.RawField(k).Decode(row2.Encoding(enc - 1))
		}
	}
	return r.wire.RawField(k).Decode(r.propertyEncoding(k))
}
func nativeValue(p *nativePlan, v reflect.Value, layout row2.Layout) values.Value {
	switch p.kind {
	case nativeSigned:
		return values.Int(v.Int())
	case nativeUnsigned:
		return values.Uint(v.Uint())
	case nativeBool:
		if v.Bool() {
			return values.Uint(1)
		}
		return values.Value{}
	case nativeFloat:
		if p.typ.Kind() == reflect.Float32 {
			return values.Float32(float32(v.Float()))
		}
		return values.Float(v.Float())
	case nativeTime:
		return values.Uint(TimeToUint64(v.Interface().(time.Time)))
	case nativeTimestamp:
		return values.Time(nativeTimestampValue(v))
	case nativeJSON:
		return JSONValue(v.Interface())
	case nativeString:
		return values.String(v.String())
	case nativeBytes:
		return values.Bytes(bytes.Clone(v.Bytes()))
	default:
		// Requesting a whole collection or object explicitly materializes that
		// property's wire value. Use Child for direct navigation between objects.
		var w row2.Writer
		p.emit(&w, v, 0, "", layout, false, 0)
		return values.Bytes(w.BytesWritten())
	}
}
func (r recordView) Has(k uint64) bool {
	if r.memory != nil {
		return r.memory.has(k)
	}
	if r.Missing() {
		return false
	}
	if !r.msg.Missing() {
		return r.messagePackHas(k)
	}
	if r.object == nil {
		return r.wire.Has(k)
	}
	f := r.plan.field(k)
	if r.plan.model.PropFlags(k)&PrimaryKey != 0 {
		return true
	}
	return nativeFieldHas(f.plan, f.access.Get(r.object))
}
func nativeHas(p *nativePlan, v reflect.Value) bool {
	switch p.kind {
	case nativeTime, nativeTimestamp:
		return !nativeTimestampValue(v).IsZero()
	case nativeOption:
		return v.Field(1).Bool()
	case nativePointer, nativeList, nativeBytes, nativeNumbers, nativeDictionary:
		if p.nilable() {
			return !v.IsNil()
		}
	case nativeObject:
		return true
	}
	return !v.IsZero()
}

// Editor applies property edits by path. A native editor writes to its bound struct.
type Editor struct {
	wire              *row2.Editor
	view              recordView
	changes           uint64
	pending           []editField
	cachedMessagePack []byte
	cachedPacked      row2.View
	packedMode        packedEditMode
	parent            *Editor
	parentKey         uint64
	children          map[uint64]*Editor
	collections       map[uint64]collectionEdit
	onChange          func()
	detached          bool
}

// EditEncoded shares an existing codec editor and writes through immediately.
// Use Record.Edit for a private editor that batches encoded changes.
func EditEncoded(e *row2.Editor) *Editor { return &Editor{wire: e, packedMode: sharedPackedEdit} }
func (r recordView) Edit() *Editor {
	if r.memory != nil {
		return &Editor{view: r}
	}
	if !r.msg.Missing() {
		return &Editor{view: r}
	}
	if r.object == nil {
		return &Editor{wire: row2.Edit(r.wire), view: r}
	}
	return &Editor{view: r}
}

// Changes counts assignments that changed a MessagePack or native record.
func (e *Editor) Changes() uint64     { return e.changes }
func (e *Editor) NativeModel() *Model { return e.view.NativeModel() }

// Packed exposes the codec editor. Direct codec writes bypass attached child
// editors, so obtaining it invalidates those child handles. Prefer Snapshot.
func (e *Editor) Packed() *row2.Editor {
	e.checkAttached()
	if e.wire != nil {
		e.flush()
		for key := range e.collections {
			e.detachCollection(key)
		}
		for key := range e.children {
			e.detachChild(key)
		}
	}
	return e.wire
}

// GetValue reads one property, including pending edits.
func (e *Editor) GetValue(key uint64) values.Value {
	e.checkAttached()
	if !e.view.msg.Missing() {
		f := e.view.plan.field(key)
		return messagePackFieldValue(f, e.messagePackField(f))
	}
	if e.wire != nil {
		if f := e.pendingField(key); f != nil {
			return editFieldValue(*f)
		}
		return e.wire.Snapshot().Get(key, e.view.propertyEncoding(key))
	}
	return e.view.Get(key)
}

// GetPathValue reads a nested property. Use GetValue for a direct property.
func (e *Editor) GetPathValue(path []row2.Step) values.Value {
	e.checkAttached()
	if len(path) == 0 {
		panic(row2.ErrKind)
	}
	if !e.view.msg.Missing() && len(path) == 1 {
		f := e.view.plan.field(path[0].Key)
		return messagePackFieldValue(f, e.messagePackField(f))
	}
	if len(path) > 1 && e.Model() != nil && e.packedMode != sharedPackedEdit {
		if child := e.editChild(path[0].Key); child != nil {
			return child.GetPathValue(path[1:])
		}
		return values.Value{}
	}
	if e.wire != nil {
		if f := e.pendingField(path[0].Key); f != nil {
			return editFieldValue(*f)
		}
		return e.wire.Get(path, e.pathEncoding(path))
	}
	r := e.view
	for _, s := range path[:len(path)-1] {
		r = r.Child(s)
	}
	return r.Get(path[len(path)-1].Key)
}

// SetValue writes one property using the model's write policy, including zero
// preservation for presence-significant and stored-primary-key properties.
// It panics on error; use TrySetValue to handle errors explicitly.
func (e *Editor) SetValue(key uint64, x values.Value) {
	if err := e.TrySetValue(key, x); err != nil {
		panic(err)
	}
}

// TrySetValue is SetValue with explicit error handling.
func (e *Editor) TrySetValue(key uint64, x values.Value) (err error) {
	defer func() {
		if ex := recover(); ex != nil {
			err = recoveredError("property assignment", ex)
		}
	}()
	e.checkAttached()
	field := e.view.propertyField(key)
	step := row2.Step{Key: key, Options: field.writeOptions}
	if e.wire != nil {
		e.setPackedField(step, x, field.encoding)
		return nil
	}
	return e.setPathValue([]row2.Step{step}, x, nil)
}

// FastSetPathValue writes a path using its explicitly supplied Step.Options.
// EVM uses this API because its linker knows the model and precomputes these
// options once, avoiding model-policy lookups on every guest assignment. The
// options could be derived from the model, but doing so here would discard that
// optimization. They are used as-is, not merged with model defaults: callers
// must supply the intended compact-child and zero-preservation policy for each
// step. Explicit deletion also uses this path to bypass zero preservation.
// Ordinary property writes should use SetValue, which derives the policy.
// FastSetPathValue panics on error; use TryFastSetPathValue for error handling.
func (e *Editor) FastSetPathValue(path []row2.Step, x values.Value) {
	err := e.TryFastSetPathValue(path, x)
	if err != nil {
		panic(err)
	}
}

// TryFastSetPathValue is FastSetPathValue with explicit error handling.
func (e *Editor) TryFastSetPathValue(path []row2.Step, x values.Value) error {
	return e.TryFastSetPathValueWithWriter(path, x, nil)
}

// whole reports whether the projection includes every field of this value.
func (p *nativePlan) whole(seen map[*nativePlan]bool) bool {
	if seen[p] {
		return true
	}
	seen[p] = true
	if p.kind == nativeObject && !p.coversFields(p.typ, nil) {
		return false
	}
	for _, f := range p.fields {
		if !f.plan.whole(seen) {
			return false
		}
	}
	return p.elem == nil || p.elem.whole(seen)
}

func (p *nativePlan) coversFields(t reflect.Type, path []int) bool {
	for _, f := range p.fields {
		if slices.Equal(f.path, path) {
			return true
		}
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < t.NumField(); i++ {
		if !p.coversFields(t.Field(i).Type, append(slices.Clone(path), i)) {
			return false
		}
	}
	return true
}

// FastSetPathValueWithWriter is FastSetPathValue with reusable encoding scratch.
func (e *Editor) FastSetPathValueWithWriter(path []row2.Step, x values.Value, w *row2.Writer) {
	err := e.TryFastSetPathValueWithWriter(path, x, w)
	if err != nil {
		panic(err)
	}
}

// TryFastSetPathValueWithWriter is TryFastSetPathValue with reusable encoding scratch.
func (e *Editor) TryFastSetPathValueWithWriter(path []row2.Step, x values.Value, w *row2.Writer) (err error) {
	defer func() {
		if ex := recover(); ex != nil {
			err = recoveredError("property assignment", ex)
		}
	}()
	e.checkAttached()
	return e.setPathValue(path, x, w)
}

func (e *Editor) setPathValue(path []row2.Step, x values.Value, w *row2.Writer) error {
	if len(path) == 0 {
		return row2.ErrKind
	}
	if len(path) > 1 && e.Model() != nil && (e.view.object != nil || e.view.memory != nil) {
		if child := e.editChild(path[0].Key); child != nil {
			return child.TryFastSetPathValue(path[1:], x)
		}
	}
	if e.view.memory != nil {
		return e.setMemory(path, x)
	}
	if len(path) > 1 && e.Model() != nil && e.packedMode != sharedPackedEdit && (e.wire != nil || !e.view.msg.Missing()) {
		return e.setNested(path, x)
	}
	if !e.view.msg.Missing() {
		return e.setMessagePack(path, x)
	}
	if e.wire != nil {
		if len(path) == 1 {
			e.setPackedField(path[0], x, e.pathEncoding(path))
			return nil
		}
		e.flush()
		e.wire.SetWithWriter(path, x, e.pathEncoding(path), w)
		e.touch()
		return nil
	}
	// Check and convert before creating a previously nil parent.
	p := e.view.plan
	for _, s := range path {
		for p.kind == nativePointer || p.kind == nativeOption {
			p = p.elem
		}
		p = p.field(s.Key).plan
	}
	leaf := p
	for leaf.kind == nativeOption || leaf.kind == nativePointer {
		leaf = leaf.elem
	}
	if leaf.kind >= nativeObject && leaf.kind <= nativeList && !leaf.whole(make(map[*nativePlan]bool)) {
		return fmt.Errorf("cannot replace an aggregate containing unexposed fields; assign its properties")
	}
	present := !(x.R == 0) || path[len(path)-1].Options&row2.Present != 0
	assigned, scalar := scalarAssignment(p, x)
	if !scalar {
		val := reflect.New(p.typ).Elem()
		p.read(val, x, present, 0)
		assigned = nativeAssignment(p, val)
	}
	current := e.view
	for _, s := range path[:len(path)-1] {
		f := current.plan.field(s.Key)
		cp, cv, ok := unwrapForEdit(f.plan, f.access.Get(current.object), f.access.Address(current.object), present)
		if !ok {
			return nil
		}
		if cp.kind != nativeObject {
			return row2.ErrKind
		}
		current = recordView{plan: cp, object: cv.P}
	}
	f := current.plan.field(path[len(path)-1].Key)
	if scalar {
		old := f.access.Get(current.object)
		if p.kind == nativeString {
			if old.AsString() == assigned.AsString() {
				return nil
			}
		} else if old == assigned {
			return nil
		}
	} else if len(path) == 1 && p.typ.Comparable() {
		before := reflect.NewAt(p.typ, f.access.Address(current.object)).Elem()
		after := reflect.New(p.typ).Elem()
		// Compare using the same shared assignment carrier, including pointers.
		storeAssigned(p, after, assigned)
		if before.Equal(after) {
			return nil
		}
	}
	if scalar && p.kind == nativeString {
		assigned = values.String(strings.Clone(assigned.AsString()))
	}
	if len(path) == 1 {
		e.detachChild(path[0].Key)
		e.detachCollection(path[0].Key)
	}
	f.access.Set(current.object, assigned)
	e.touch()
	return nil
}
