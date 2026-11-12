// Immutable KVO records travel as cells and borrow storage until the compiler
// inserts an ownership copy. Mutable editors remain bound to a RecordScope.
package evm

import (
	"context"
	"fmt"
	"go/token"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// VMFieldBase separates VM layout fields from persistent KVO property codes.
const VMFieldBase = kvo.RuntimePropBase

type RecordType struct {
	// SchemaView exposes the properties supported by the current KVO codec.
	SchemaView bool
	Name       string
	Model      *kvo.Model
	Mutable    bool
	Pointer    *values.Type // guest *Model: Pair for immutable cells, Pointer for editors
}

// Only direct pointer-result imports need this index. A schema can expose
// many more models than a script uses.
func recordResults(packages []Package) map[*values.Type]*RecordType {
	records := make(map[*values.Type]*RecordType)
	for _, pkg := range packages {
		for _, r := range pkg.Records {
			if r != nil {
				records[r.Pointer] = r
			}
		}
	}
	return records
}

func NewRecordType(name string, model *kvo.Model, mutable bool) *RecordType {
	typ := kvo.Type(model)
	if mutable {
		typ = values.NewType("*"+name, values.Pointer)
	}
	return &RecordType{Name: name, Model: model, Mutable: mutable, Pointer: typ}
}

// ModelPackage declares read and edit handles for every entity in a completed
// KVO schema. Register once and share its type identities across compilations.
// Database operations are supplied separately by the host.
func ModelPackage(path string, schemas ...*kvo.Schema) (Package, error) {
	pkg := Package{Path: path}
	anyShape := &values.Shape{Type: values.AnyType, Kind: values.InterfaceKind, Cells: 1}
	stringShape := &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}
	object := &values.Shape{Type: JSONObjectType, Kind: values.MapKind, Cells: 1, Key: stringShape, Elem: anyShape}
	pkg.Types = append(pkg.Types, TypeDecl{Shape: object})
	names := map[string]string{}
	var models []*kvo.Model
	for _, schema := range schemas {
		models = append(models, schema.Models()...)
	}
	for _, model := range models {
		var b strings.Builder
		upper := true
		for _, r := range model.Name() {
			if r == '_' {
				upper = true
				continue
			}
			if upper {
				r = unicode.ToUpper(r)
				upper = false
			}
			b.WriteRune(r)
		}
		base := b.String()
		if !token.IsIdentifier(base) || !token.IsExported(base) {
			return Package{}, fmt.Errorf("invalid guest model name %q from %q", base, model.Name())
		}
		for _, mutable := range []bool{false, true} {
			name := base
			if mutable {
				name += "Edit"
			}
			if prev, exists := names[name]; exists {
				return Package{}, fmt.Errorf("KVO models %q and %q both declare %s", prev, model.Name(), name)
			}
			names[name] = model.Name()
			r := NewRecordType(name, model, mutable)
			r.SchemaView = true
			pkg.Records = append(pkg.Records, r)
			pkg.Methods = append(pkg.Methods, ExternalMethod{Name: "ToJSON", Receiver: r.Pointer, Signature: Signature{Results: []*values.Type{object.Type}}, Call: func(c *Call) error { return c.recordJSON(r) }})
		}
	}
	return pkg, nil
}

// Record returns the package's handle for the given model and access mode.
func (p *Package) Record(model *kvo.Model, mutable bool) *RecordType {
	for _, r := range p.Records {
		if r.Model == model && r.Mutable == mutable {
			return r
		}
	}
	return nil
}

// RecordWordType maps a scalar KVO property to its script type.
func RecordWordType(p kvo.PropImpl) *values.Type {
	t, ok := p.AnyType().(*kvo.WordType)
	if !ok {
		return nil
	}
	return RecordScalarType(t)
}

// RecordScalarType supplies the VM type for KVO leaves.
func RecordScalarType(t kvo.AnyType) *values.Type {
	if t == kvo.TTimestamp {
		return values.TimeType
	}
	if _, ok := t.(*kvo.JSONType); ok {
		return values.AnyType
	}
	if d, ok := t.(*kvo.ScalarDataType); ok && d.NativeType().Kind() == reflect.String {
		return values.StringType
	}
	w, ok := t.(*kvo.WordType)
	if !ok {
		return nil
	}
	switch w.NativeType().Kind() {
	case reflect.Bool:
		return values.Boolean
	case reflect.Int:
		return values.IntType
	case reflect.Int64:
		return values.Int64
	case reflect.Int32:
		return values.Int32
	case reflect.Int16:
		return values.Int16
	case reflect.Int8:
		return values.Int8
	case reflect.Uint:
		return values.UintType
	case reflect.Uint64:
		return values.Uint64
	case reflect.Uint32:
		return values.Uint32
	case reflect.Uint16:
		return values.Uint16
	case reflect.Uint8:
		return values.Uint8
	case reflect.Float32:
		return values.Float32Type
	case reflect.Float64:
		return values.Float64
	case reflect.Struct:
		if w.NativeType() == reflect.TypeFor[time.Time]() {
			return values.Uint64
		}
	}
	return nil
}

type recordScopeKey struct{}
type RecordScope struct {
	fields      map[*kvo.Editor][]*recordFieldValue
	writer      row2.Writer // shared scratch for structural row edits in this transaction
	closed      bool
	entries     []*borrowedRecord
	spareRows   []borrowedRecord
	spareTokens [][2]values.Value
	local       [8]localRecordSlot
	reads       map[recordSite]*borrowedRecord
}
type localRecordSlot struct {
	site recordSite
	row  *borrowedRecord
}
type borrowedRecord struct {
	identity unsafe.Pointer
	read     kvo.Record
	edit     *kvo.Editor
	model    *kvo.Model
	parent   *borrowedRecord
	step     row2.Step
}

type recordContext struct {
	context.Context
	scope *RecordScope
}

func (c *recordContext) Value(key any) any {
	if _, ok := key.(recordScopeKey); ok {
		return c.scope
	}
	return c.Context.Value(key)
}

// WithRecords binds mutable editors to one transaction. Close the scope before
// ending the transaction. Immutable reads do not need a scope.
func WithRecords(ctx context.Context) (context.Context, *RecordScope) {
	if ctx == nil {
		panic("nil parent context")
	}
	s := new(RecordScope)
	return &recordContext{Context: ctx, scope: s}, s
}

func scopeFromContext(ctx context.Context) *RecordScope {
	if c, ok := ctx.(*recordContext); ok {
		return c.scope
	}
	// WithCancel or another context value can wrap the record scope.
	scope, _ := ctx.Value(recordScopeKey{}).(*RecordScope)
	return scope
}

func (s *RecordScope) Close() { *s = RecordScope{closed: true} }

// Tokens and rows live in separate arrays. A script retaining a token after
// Close must not keep transaction data alive. Host-supplied rows arrive in
// batches; script allocations grow by one to honor the current run's budget.
func (s *RecordScope) newRecord(host bool) *borrowedRecord {
	if s.closed {
		panic("closed record scope")
	}
	if len(s.spareRows) == 0 {
		n := 1
		if host {
			n = min(32, 1+len(s.entries))
		}
		s.spareRows = make([]borrowedRecord, n)
		s.spareTokens = make([][2]values.Value, n)
	}
	r := &s.spareRows[0]
	r.identity = unsafe.Pointer(&s.spareTokens[0])
	s.spareRows, s.spareTokens = s.spareRows[1:], s.spareTokens[1:]
	// Copying the opaque script struct preserves its private index and seal.
	*(*values.Value)(r.identity) = values.Value{R: uint64(len(s.entries)), P: r.identity}
	s.entries = append(s.entries, r)
	return r
}
func (s *RecordScope) Read(model *kvo.Model, m row2.View) values.Value {
	return s.ReadRecord(model, kvo.Encoded(m))
}
func (s *RecordScope) ReadRecord(model *kvo.Model, m kvo.Record) values.Value {
	if m.NativeModel() != nil && m.NativeModel() != model {
		panic("incorrect native record model")
	}
	if m.Missing() {
		return values.Value{}
	}
	return m.WithModel(model).Value()
}

// A call site in a recursive frame needs its own live view.
type recordSite struct {
	vm           *VM
	function, pc int
	depth        int
}

func (vm *VM) recordSite(f frame) recordSite {
	return recordSite{vm, f.function, f.pc - 1, len(vm.stack)}
}

const recordHandleBytes = uint64(unsafe.Sizeof(borrowedRecord{}) + unsafe.Sizeof([2]values.Value{}))

func (s *RecordScope) resultSlot(site recordSite, b *executionBudget) (values.Value, *borrowedRecord, error) {
	// Bytecode layout can put several hot sites in the same initial slot.
	// Probe the small table before allocating an overflow map.
	var free *localRecordSlot
	if site.vm != nil {
		for i := range s.local {
			slot := &s.local[(site.pc+i)&(len(s.local)-1)]
			if slot.site == site {
				return values.Value{P: slot.row.identity}, slot.row, nil
			}
			if slot.row == nil {
				free = slot
				break
			}
		}
		if free == nil {
			if r := s.reads[site]; r != nil {
				return values.Value{P: r.identity}, r, nil
			}
		}
	}
	if err := charge(recordHandleBytes, &b.allocated, b.limits); err != nil {
		return values.Value{}, nil, err
	}
	r := s.newRecord(false)
	if free != nil {
		// Put the new site first. In a scan, the entry call runs once and
		// the back-edge call can then hit its home slot on every row.
		home := &s.local[site.pc&(len(s.local)-1)]
		*free = *home
		home.site, home.row = site, r
	} else if site.vm != nil {
		if s.reads == nil {
			s.reads = make(map[recordSite]*borrowedRecord)
		}
		s.reads[site] = r
	}
	return values.Value{P: r.identity}, r, nil
}

// SetReadRecordResult returns one immutable KVO row from a host function.
// The compiler owns the payload when it escapes its borrowing boundary.
func (c *Call) SetReadRecordResult(model *kvo.Model, m row2.View) error {
	return c.SetRecordResult(model, kvo.Encoded(m))
}
func (c *Call) SetRecordResult(model *kvo.Model, m kvo.Record) error {
	_, err := c.recordResult(model, m, true)
	return err
}

// RecordValue returns the immutable row cell without allocating a handle.
func (c *Call) RecordValue(model *kvo.Model, m kvo.Record) (values.Value, error) {
	return c.recordResult(model, m, false)
}

func (c *Call) recordResult(model *kvo.Model, m kvo.Record, window bool) (values.Value, error) {
	if m.NativeModel() != nil && m.NativeModel() != model {
		return values.Value{}, fmt.Errorf("incorrect native record model")
	}
	v := values.Value{}
	if !m.Missing() {
		v = m.WithModel(model).Value()
	}
	if window {
		c.SetResult(0, v)
	}
	return v, nil
}

// SetEditRecordResult returns a transaction-bound editor and meters its handle.
func (c *Call) SetEditRecordResult(model *kvo.Model, e *kvo.Editor) error {
	scope := c.budget.recordScope()
	if scope == nil || scope.closed {
		return fmt.Errorf("KVO access outside record scope")
	}
	if e != nil {
		if err := c.ChargeAllocation(recordHandleBytes); err != nil {
			return err
		}
	}
	c.SetResult(0, scope.EditRecord(model, e))
	return nil
}

func (s *RecordScope) Edit(model *kvo.Model, m *row2.Editor) values.Value {
	if m == nil {
		return values.Value{}
	}
	return s.EditRecord(model, kvo.EditEncoded(m))
}
func (s *RecordScope) EditRecord(model *kvo.Model, m *kvo.Editor) values.Value {
	if m != nil && m.NativeModel() != nil && m.NativeModel() != model {
		panic("incorrect native record model")
	}
	if m == nil {
		return values.Value{}
	}
	r := s.newRecord(true)
	r.edit, r.model = m.WithModel(model), model
	return values.Value{P: r.identity}
}
func (s *RecordScope) lookup(v values.Value) *borrowedRecord {
	if v.P == nil {
		return nil
	}
	seal := *(*values.Value)(v.P)
	if seal.R >= uint64(len(s.entries)) {
		return nil
	}
	r := s.entries[seal.R]
	if r.identity != seal.P {
		return nil
	}
	return r
}

func (s *RecordScope) Mutable(v values.Value, model *kvo.Model) (*row2.Editor, error) {
	e, err := s.MutableRecord(v, model)
	if err != nil {
		return nil, err
	}
	if e.Packed() == nil {
		return nil, fmt.Errorf("expected an encoded record")
	}
	return e.Packed(), nil
}
func (s *RecordScope) MutableRecord(v values.Value, model *kvo.Model) (*kvo.Editor, error) {
	r := s.lookup(v)
	if r == nil || r.model != model || r.edit == nil || r.parent != nil {
		return nil, fmt.Errorf("expected a live mutable root record")
	}
	return r.edit, nil
}

// Resolve child views on use so a read of a missing child does not create it.
// Iterative resolution keeps guest-controlled nesting off the Go call stack.
func (r *borrowedRecord) editPath(path []row2.Step, b *executionBudget) (*kvo.Editor, []row2.Step, error) {
	for r.parent != nil {
		if len(path)&255 == 255 {
			if err := b.contextErr(); err != nil {
				return nil, nil, err
			}
		}
		if len(path) == cap(path) {
			if err := charge(uint64(2*cap(path))*uint64(unsafe.Sizeof(row2.Step{})), &b.allocated, b.limits); err != nil {
				return nil, nil, err
			}
		}
		path = append(path, r.step)
		r = r.parent
	}
	slices.Reverse(path)
	return r.edit, path, nil
}

func (b *executionBudget) recordScope() *RecordScope {
	if b.records == nil {
		b.records = scopeFromContext(b.ctx)
	}
	return b.records
}

// One workspace per linked indexing site, reused across rows. A new list
// expression invalidates cached iteration; neither records nor rows get slots.
// Sequential range reads are O(n), not a fresh scan from element zero each time.
type recordListIterator struct {
	owner      values.Value
	generation uint64
	list       kvo.ListValue
	iterator   kvo.ListValueIterator
	next       int
	length     int
}

func (vm *VM) nextRecordListGeneration() {
	vm.recordListGeneration++
	if vm.recordListGeneration == 0 {
		clear(vm.recordLists)
	}
}

func (vm *VM) recordOp(f frame, op, second *ExtendedOp, b *executionBudget) error {
	if !op.Record.Mutable {
		if op.Flags&RecordListDirect == 0 {
			switch op.Kind {
			case ExtendedRecordListLen:
				list, ok := op.listAccess.TryCell(vm.read(f, op.In[0]))
				if !ok {
					return fmt.Errorf("invalid KVO list cell")
				}
				vm.write(f, op.Out[0], values.Int(int64(list.Len())))
				return nil
			case ExtendedRecordListIndex:
				value := vm.read(f, op.In[0])
				cache := &vm.recordLists[op.listSlot]
				if cache.owner != value || cache.generation != vm.recordListGeneration {
					list, ok := op.listAccess.TryCell(value)
					if !ok {
						return fmt.Errorf("invalid KVO list cell")
					}
					cache.owner, cache.generation = value, vm.recordListGeneration
					cache.list = list.View()
					cache.iterator = cache.list.Iter()
					cache.length, cache.next = cache.list.Len(), 0
				}
				i := vm.read(f, op.In[1]).AsInt()
				if i < 0 || uint64(i) >= uint64(cache.length) {
					return ErrBounds
				}
				if int(i) < cache.next {
					cache.iterator = cache.list.Iter()
					cache.next = 0
				}
				for {
					v, _ := cache.iterator.Next()
					cache.next++
					if cache.next == int(i)+1 {
						vm.write(f, op.Out[0], v)
						return nil
					}
				}
			}
		}
		r, err := readRecordCell(vm.read(f, op.In[0]), op.Record.Model)
		if err != nil {
			return err
		}
		if r.Missing() {
			return fmt.Errorf("missing %s record", op.Record.Name)
		}
		switch op.Kind {
		case ExtendedRecordListLen:
			vm.write(f, op.Out[0], values.Int(int64(op.listAccess.Get(r).Len())))
			return nil
		case ExtendedRecordListIndex:
			list := op.listAccess.Get(r)
			i := vm.read(f, op.In[1]).AsInt()
			if i < 0 || uint64(i) >= uint64(list.Len()) {
				return ErrBounds
			}
			vm.write(f, op.Out[0], list.At(int(i)))
			return nil
		case ExtendedRecordList:
			vm.nextRecordListGeneration()
			vm.write(f, op.Out[0], op.listAccess.Get(r).Value())
			return nil
		}
		if op.Kind == ExtendedGetChildKey {
			child := r.Child(op.step)
			if child.Missing() {
				return fmt.Errorf("missing %s record", recordDisplayName(op.Source))
			}
			vm.r[f.rb+int(op.Out[0].Slot)] = child.GetValue(op.ChildKey).R
			if second != nil {
				vm.r[f.rb+int(second.Out[0].Slot)] = child.GetValue(second.ChildKey).R
			}
			return nil
		}
		if op.childModel != nil {
			child := r.Child(op.step)
			v := values.Value{}
			if !child.Missing() {
				v = child.Value()
			}
			vm.write(f, op.Out[0], v)
			return nil
		}
		if op.Shape.Type.Repr() != values.Raw {
			return vm.recordField(f, op, &borrowedRecord{read: r}, false, b)
		}
		vm.r[f.rb+int(op.Out[0].Slot)] = r.GetValue(op.Index).R
		return nil
	}
	scope := b.recordScope()
	if scope == nil {
		return fmt.Errorf("KVO access outside record scope")
	}
	r := scope.lookup(values.Value{P: vm.p[f.pb+int(op.In[0].Slot)]})
	if r == nil || r.model != op.Record.Model {
		return fmt.Errorf("invalid or expired %s record", op.Record.Name)
	}
	writable := r.parent != nil || r.edit != nil
	if op.Record.Mutable != writable {
		return fmt.Errorf("wrong access mode for %s", op.Record.Name)
	}
	if op.childModel != nil {
		var site recordSite
		if vm.program.functions[f.function].localRead(f.pc - 1) {
			site = vm.recordSite(f)
		}
		v, child, err := scope.resultSlot(site, b)
		if err != nil {
			return err
		}
		child.model = op.childModel
		child.parent, child.step = r, op.step
		vm.p[f.pb+int(op.Out[0].Slot)] = v.P
		return nil
	}
	if op.Shape.Type.Repr() != values.Raw {
		return vm.recordField(f, op, r, writable, b)
	}
	var scratch [8]row2.Step
	scratch[0] = op.step
	edit, path, err := r.editPath(scratch[:1], b)
	if err != nil {
		return err
	}
	if op.Kind == ExtendedSetKey {
		return edit.TryFastSetPathValueWithWriter(path, values.Uint(vm.r[f.rb+int(op.In[1].Slot)]), &scope.writer)
	} else {
		vm.r[f.rb+int(op.Out[0].Slot)] = edit.GetPathValue(path).R
	}
	return nil
}

// An explicit Go struct copy stores its immutable record cell in VM memory.
// Ordinary read handles already carry the cell and take the fast path.
func recordCell(v values.Value) values.Value {
	if v.R == 0 && v.P != nil {
		return *(*values.Value)(v.P)
	}
	return v
}
func readRecordCell(v values.Value, model *kvo.Model) (kvo.Record, error) {
	v = recordCell(v)
	if v.P == nil {
		return kvo.Record{}, nil
	}
	r, ok := kvo.TryFromValue(v)
	if !ok || r.Model() != model {
		return kvo.Record{}, fmt.Errorf("incorrect record model")
	}
	return r, nil
}

func recordDisplayName(s *values.Shape) string {
	name := strings.TrimPrefix(s.DisplayName, "*")
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	return name
}
