package evm

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type Signature struct{ Params, Results []*values.Type }

// Layout allocates V pairs first, then independent R and P slots. Slots retains
// source order, so a wrapper can derive its ABI once during registration.
type Layout struct {
	Slots          []uint16
	RCount, PCount int
}

func LayoutOf(types []*values.Type) (Layout, error) {
	if len(types) > 65535 {
		return Layout{}, fmt.Errorf("too many values in signature")
	}
	l := Layout{Slots: make([]uint16, len(types))}
	for i, t := range types {
		if t == nil || t.Repr() < values.Raw || t.Repr() > values.Pair {
			return l, fmt.Errorf("invalid type in signature")
		}
		if t.Repr() == values.Pair {
			l.Slots[i] = uint16(l.RCount)
			l.RCount++
			l.PCount++
		}
	}
	for i, t := range types {
		switch t.Repr() {
		case values.Raw:
			l.Slots[i] = uint16(l.RCount)
			l.RCount++
		case values.Pointer:
			l.Slots[i] = uint16(l.PCount)
			l.PCount++
		}
	}
	return l, nil
}

// ParameterLayout reserves incoming V0 for a method's receiver. The explicit
// arguments use the ordinary packed layout starting at R1 and P1.
func ParameterLayout(params []*values.Type, method bool) (Layout, error) {
	if !method {
		return LayoutOf(params)
	}
	if len(params) == 0 || len(params) > 65535 || params[0] == nil || params[0].Repr() < values.Raw || params[0].Repr() > values.Pair {
		return Layout{}, fmt.Errorf("invalid method parameters")
	}
	args, err := LayoutOf(params[1:])
	if err != nil {
		return Layout{}, err
	}
	slots := make([]uint16, len(params))
	for i, slot := range args.Slots {
		slots[i+1] = slot + 1
	}
	return Layout{Slots: slots, RCount: args.RCount + 1, PCount: args.PCount + 1}, nil
}

type Function struct {
	// OwnershipSites describes compiler-emitted clone locations before linking.
	OwnershipSites []OwnershipSite
	// LocalRecordReads marks instructions whose result cannot outlive its next execution in this frame.
	LocalRecordReads map[int]bool
	Method           bool // The first parameter is the receiver.
	// RecoverPC is a bytecode PC plus one; zero means no recovery continuation.
	RecoverPC uint32
	// RecoverTransparent marks compiler-generated receiver forwarding wrappers.
	RecoverTransparent bool
	Captures           []CaptureSlot
	Internal           bool // Generated functions are callable by bytecode only.
	Name               string
	Position           SourcePosition
	Signature          Signature
	// RCount/PCount cover this function's parameters, results and locals.
	// Outgoing arguments/results follow them and are sized by the linker.
	RCount, PCount int
	Code           []Instruction
	// Lines is optional; when present it has one source line per instruction.
	Lines     []int
	Positions []SourcePosition
}
type Import struct {
	Ownership     *OwnershipEffects
	Receiver      *values.Type
	Package, Name string
	Signature     Signature
}

// JSONTypes identifies the concrete types a KVO JSON value can produce.
type JSONTypes struct{ Boolean, Integer, Unsigned, Float, String, Array, Object uint32 }

type Module struct {
	JSONTypes JSONTypes
	// NoOptimization disables optional linker rewrites. The compiler sets this for debug modules.
	NoOptimization bool
	// StrictCallWindows promises that callers only observe declared results after a script call.
	StrictCallWindows bool
	// Sources holds original text keyed by source-position filename for optional disassembly comments.
	Sources      map[string]string
	Defers       []DeferOp
	Types        []DynamicType // concrete interface types indexed by R; slot zero, if present, must be empty
	Invokes      []Invoke
	FunctionRefs []FunctionRef
	Closures     []Closure
	Signatures   []Signature
	Functions    []Function
	Imports      []Import
	Words        []uint64
	Strings      []string
	Conversions  []Conversion
	Shapes       []*values.Shape
	ExtendedOps  []ExtendedOp
	Globals      []*values.Shape
	Init         string
}

// Call borrows an in-place argument/result window for this native call. Read
// parameters before overwriting result slots. Do not retain Call or its slices,
// and do not reenter this VM. Independent VMs may run concurrently.
type Call struct {
	vm           *VM
	usedR, usedP int // live parent register span when entering a synchronous callback
	program      *Program
	output       func(string) error
	native       *linkedNative
	budget       *executionBudget
	Context      context.Context
	Position     SourcePosition
	R            []uint64
	P            []unsafe.Pointer
}

// Value reads a V pair. Access R-only and P-only arguments through their banks.
func (c *Call) Value(slot int) values.Value { return values.Value{R: c.R[slot], P: c.P[slot]} }

// SetValue writes a V pair into a result slot whose external type uses both banks.
func (c *Call) SetValue(slot int, v values.Value) { c.R[slot], c.P[slot] = v.R, v.P }

type Native func(*Call) error

// LinkName identifies the function in imports and disassembly.
func (f *ExternalFunction) LinkName() string {
	if f.On != nil {
		return f.On.Name + "." + f.Name
	}
	return f.Name
}

type ExternalFunction struct {
	Ownership *OwnershipEffects
	// ParamNames preserves declaration names for source frontends such as GoX.
	ParamNames []string
	Direct     DirectBinding
	// On exposes a receiver-free operation as pkg.Record.Operation(args).
	On       *RecordType
	panics   bool
	Variadic bool
	// DynamicTypes declares concrete interface results produced by this wrapper.
	DynamicTypes []*values.Type
	Name         string
	Signature    Signature
	Call         Native
}

// SharedType maps a host scalar to a type declared in Package.Source.
// Its methods are compiled from that same source.
type SharedType struct {
	Name   string
	Native reflect.Type
	Type   *values.Type
}

type Package struct {
	Source      string
	SharedTypes []SharedType
	Records     []*RecordType
	// EntryTypes are native concrete types that may enter in interface arguments to Run.
	EntryTypes []*values.Type
	Types      []TypeDecl
	Methods    []ExternalMethod
	Path       string
	Functions  []ExternalFunction
	Constants  []ExternalConstant
}

type linkedFunction struct {
	definedRaw bool
	localReads []uint64
	Function
	params, results Layout
	rSize, pSize    int
}

func (f *linkedFunction) localRead(pc int) bool {
	return f.localReads != nil && f.localReads[pc/64]&(uint64(1)<<uint(pc&63)) != 0
}

type linkedNative struct {
	form         *hostForm
	resultString bool
	record       *RecordType // scoped result of a direct call
	ExternalFunction
	params, results Layout
	rSize, pSize    int
}

func linkNative(fn ExternalFunction, params, results Layout) linkedNative {
	if fn.Direct.Call != nil && fn.Call == nil {
		fn.Call = directWindowCall
	}
	var form *hostForm
	stringResult := false
	if base := fn.Direct.Op.hostForm(); base != nil {
		actual := *base
		stringResult = base.result == values.Pair
		if len(fn.Signature.Results) > 0 {
			actual.result = fn.Signature.Results[0].Repr()
		}
		args := fn.Signature.Params
		if actual.method {
			actual.receiver = args[0].Repr()
			args = args[1:]
		}
		if len(args) > 0 {
			actual.first = args[0].Repr()
		}
		if len(args) > 1 {
			actual.second = args[1].Repr()
		}
		form = &actual
	}
	if fn.Ownership != nil {
		for _, source := range fn.Ownership.Borrowed {
			if source.Result == 0 {
				stringResult = false
			}
		}
	}
	return linkedNative{form: form, resultString: stringResult, ExternalFunction: fn, params: params, results: results, rSize: max(params.RCount, results.RCount), pSize: max(params.PCount, results.PCount)}
}

// Program is an immutable linked snapshot. Registration and symbol lookup stay
// outside the execution loop. Independent VMs may share a Program if its native
// wrappers support concurrent use.
type Program struct {
	jsonTypes              JSONTypes
	runtimeError, panicNil uint64
	nativeImports          int
	defers                 []DeferOp
	methodIDs              map[string]uint32
	requirements           map[*values.Shape][]methodRequirement
	shapeByType            map[*values.Type]*values.Shape
	types                  []linkedDynamicType
	typeShapes             *values.TypeTable
	typeIDs                map[*values.Type]uint32
	invokes                []linkedInvoke
	functionRefs           []FunctionRef
	closures               []Closure
	signatures             []linkedSignature
	functions              []linkedFunction
	natives                []linkedNative
	words                  []uint64
	strings                []values.Value
	entries                map[string]int
	conversions            []Conversion
	shapes                 []*values.Shape
	extendedOps            []ExtendedOp
	recordListSlots        int
	globals                []*values.Shape
	init                   int
	floatLoops             []floatLoop
}

func copySignature(s Signature) Signature {
	return Signature{slices.Clone(s.Params), slices.Clone(s.Results)}
}
func sameSignature(a, b Signature) bool {
	return slices.Equal(a.Params, b.Params) && slices.Equal(a.Results, b.Results)
}

func Link(m *Module, packages ...Package) (*Program, error) {
	packages = append([]Package{TimePackage()}, packages...)
	if m == nil {
		return nil, fmt.Errorf("nil module")
	}
	if len(m.Functions) == 0 || uint64(len(m.Functions)) > 1<<32 || uint64(len(m.Imports)) > 1<<32 {
		return nil, fmt.Errorf("invalid function/import count")
	}
	p := &Program{jsonTypes: m.JSONTypes, methodIDs: make(map[string]uint32), requirements: make(map[*values.Shape][]methodRequirement), typeIDs: make(map[*values.Type]uint32), words: slices.Clone(m.Words), entries: make(map[string]int), conversions: slices.Clone(m.Conversions)}
	p.init = -1
	p.nativeImports = len(m.Imports)
	seenShapes := make(map[*values.Shape]*values.Shape)
	for _, set := range []struct {
		from []*values.Shape
		to   *[]*values.Shape
	}{{m.Shapes, &p.shapes}, {m.Globals, &p.globals}} {
		for _, s := range set.from {
			c, err := snapshotShape(s, seenShapes)
			if err != nil {
				return nil, err
			}
			if c == nil {
				return nil, fmt.Errorf("nil storage shape")
			}
			*set.to = append(*set.to, c)
		}
	}
	if len(p.shapes) > 65536 {
		return nil, fmt.Errorf("too many storage shapes")
	}
	for _, op := range m.ExtendedOps {
		if op.Kind == ExtendedGetKey || op.Kind == ExtendedSetKey || op.Kind == ExtendedGetChildKey || op.Kind == ExtendedRecordList || op.Kind == ExtendedRecordListLen || op.Kind == ExtendedRecordListIndex {
			found := false
			for _, pkg := range packages {
				if slices.Contains(pkg.Records, op.Record) && op.Record != nil {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("missing KVO record declaration")
			}
			frozen := *op.Record
			op.Record = &frozen
			if frozen.Model == nil {
				return nil, fmt.Errorf("missing KVO model for %s", frozen.Name)
			}
			prop := frozen.Model.PropByCode(op.Index)
			if prop == nil {
				return nil, fmt.Errorf("unknown KVO property %d", op.Index)
			}
			flags := frozen.Model.PropFlags(op.Index)
			op.step = row2.Step{Key: op.Index, Options: kvo.WriteOptions(flags)}
			op.childModel = prop.TypeModel()
		}
		var err error
		op.Shape, err = snapshotShape(op.Shape, seenShapes)
		if err != nil {
			return nil, err
		}
		op.Source, err = snapshotShape(op.Source, seenShapes)
		if err != nil {
			return nil, err
		}
		op.In = slices.Clone(op.In)
		op.Out = slices.Clone(op.Out)
		if op.Kind == ExtendedRecordList || op.Kind == ExtendedRecordListLen || op.Kind == ExtendedRecordListIndex {
			if _, ok := op.Record.Model.PropByCode(op.Index).AnyType().(*kvo.ListType); !ok {
				return nil, fmt.Errorf("KVO property %d is not a list", op.Index)
			}
			op.listAccess = kvo.BindListAccess(op.Record.Model, op.Index)
		}
		if op.Kind == ExtendedRecordListIndex && op.Flags&RecordListDirect == 0 {
			op.listSlot = p.recordListSlots
			p.recordListSlots++
		}
		p.extendedOps = append(p.extendedOps, op)
	}
	shapeStates := make(map[*values.Shape]shapeValidationState)
	for _, s := range seenShapes {
		if !validateStorageShape(s, shapeStates) {
			return nil, fmt.Errorf("invalid storage layout for %s", s.Type.Name())
		}
	}
	if len(p.conversions) > 65536 {
		return nil, fmt.Errorf("too many numeric conversions")
	}
	for _, c := range p.conversions {
		if !c.From.valid() || !c.To.valid() {
			return nil, fmt.Errorf("invalid numeric conversion")
		}
	}
	for _, s := range m.Strings {
		p.strings = append(p.strings, values.String(s))
	}
	type exportKey struct {
		path, name string
		receiver   *values.Type
	}
	exports := make(map[exportKey]*ExternalFunction)
	if err := ValidateDirectBindings(packages); err != nil {
		return nil, err
	}
	var records map[*values.Type]*RecordType
	paths := make(map[string]bool)
	for i := range packages {
		pkg := &packages[i]
		if _, reserved := StandardLibrarySource(pkg.Path); reserved || pkg.Path == "evm/debug" {
			return nil, fmt.Errorf("reserved virtual package %q", pkg.Path)
		}
		if pkg.Path == "" || paths[pkg.Path] {
			return nil, fmt.Errorf("empty or duplicate package %q", pkg.Path)
		}
		paths[pkg.Path] = true
		for i := range pkg.Methods {
			m := &pkg.Methods[i]
			if !pkg.OwnsMethodReceiver(m.Receiver) {
				return nil, fmt.Errorf("method receiver is not declared by package %s", pkg.Path)
			}
			key := exportKey{pkg.Path, m.Name, m.Receiver}
			if m.Receiver == nil || m.Name == "" || m.Call == nil && m.Direct.Call == nil {
				return nil, fmt.Errorf("invalid native method")
			}
			if _, ok := exports[key]; ok {
				return nil, fmt.Errorf("duplicate native method")
			}
			sig := Signature{Params: append([]*values.Type{m.Receiver}, m.Signature.Params...), Results: m.Signature.Results}
			if m.Direct.Op != 0 && m.Direct.Call == nil {
				return nil, fmt.Errorf("unbound direct method %s.%s", pkg.Path, m.Name)
			}
			exports[key] = &ExternalFunction{Ownership: m.Ownership, Direct: m.Direct, Name: m.Name, Signature: sig, Variadic: m.Variadic, Call: m.Call, DynamicTypes: m.DynamicTypes}
		}

		for i := range pkg.Functions {
			f := &pkg.Functions[i]
			key := exportKey{pkg.Path, f.LinkName(), nil}
			if _, ok := exports[key]; ok || f.Name == "" {
				return nil, fmt.Errorf("invalid or duplicate external function %s.%s", pkg.Path, f.Name)
			}
			if f.Direct.Op != 0 && f.Direct.Call == nil {
				return nil, fmt.Errorf("unbound direct function %s.%s", pkg.Path, f.Name)
			}
			if f.Call == nil && f.Direct.Call == nil {
				return nil, fmt.Errorf("%s.%s needs a native binding", pkg.Path, f.Name)
			}
			exports[key] = f
		}
	}
	for i := range m.Imports {
		imp := &m.Imports[i]
		declaration, ok := exports[exportKey{imp.Package, imp.Name, imp.Receiver}]
		if !ok && imp.Receiver == nil {
			if f, found := BindStandardFunction(imp.Package, imp.Name, imp.Signature); found {
				if err := validateStandardSignature(imp, seenShapes); err != nil {
					return nil, err
				}
				declaration, ok = f, true
			}
		}
		if !ok {
			return nil, fmt.Errorf("unresolved import %s.%s", imp.Package, imp.Name)
		}
		// Copy the binding before snapshotting its signature; registration remains mutable.
		f := *declaration
		if err := f.Ownership.Validate(f.Signature); err != nil {
			return nil, fmt.Errorf("%s.%s: %w", imp.Package, imp.Name, err)
		}
		if !imp.Ownership.Equal(f.Ownership) {
			return nil, fmt.Errorf("ownership effects mismatch for %s.%s", imp.Package, imp.Name)
		}
		f.Ownership = f.Ownership.Clone()
		if !sameSignature(imp.Signature, f.Signature) {
			return nil, fmt.Errorf("signature mismatch for %s.%s", imp.Package, imp.Name)
		}
		f.Signature = copySignature(f.Signature)
		args, err := ParameterLayout(f.Signature.Params, imp.Receiver != nil)
		if err != nil {
			return nil, err
		}
		results, err := LayoutOf(f.Signature.Results)
		if err != nil {
			return nil, err
		}
		g := linkNative(f, args, results)
		if g.form != nil && (g.form.result == values.Pointer || g.form.result == values.Pair) {
			if records == nil {
				records = recordResults(packages)
			}
			if r := records[f.Signature.Results[0]]; r != nil {
				snapshot := *r
				g.record = &snapshot
				g.resultString = false
			}
		}
		p.natives = append(p.natives, g)
	}
	for _, f := range m.Functions {
		if _, ok := p.entries[f.Name]; ok || f.Name == "" {
			return nil, fmt.Errorf("empty or duplicate function %q", f.Name)
		}
		args, err := ParameterLayout(f.Signature.Params, f.Method)
		if err != nil {
			return nil, err
		}
		results, err := LayoutOf(f.Signature.Results)
		if err != nil {
			return nil, err
		}
		if f.RCount < max(args.RCount, results.RCount) || f.PCount < max(args.PCount, results.PCount) || f.RCount > 65536 || f.PCount > 65536 {
			return nil, fmt.Errorf("%s: invalid register counts", f.Name)
		}
		if uint64(f.RecoverPC) > uint64(len(f.Code)) {
			return nil, fmt.Errorf("invalid recover PC")
		}
		if len(f.Code) == 0 || (len(f.Lines) != 0 && len(f.Lines) != len(f.Code)) || (len(f.Positions) != 0 && len(f.Positions) != len(f.Code)) {
			return nil, fmt.Errorf("%s: invalid code/line count", f.Name)
		}
		f.Signature = copySignature(f.Signature)
		f.Code, f.Lines = slices.Clone(f.Code), slices.Clone(f.Lines)
		var localReads []uint64
		if len(f.LocalRecordReads) != 0 {
			localReads = make([]uint64, (len(f.Code)+63)/64)
			for pc, local := range f.LocalRecordReads {
				if local && pc >= 0 && pc < len(f.Code) {
					localReads[pc/64] |= 1 << uint(pc&63)
				}
			}
		}
		f.LocalRecordReads = nil
		f.Positions = slices.Clone(f.Positions)
		f.Captures = slices.Clone(f.Captures)
		p.entries[f.Name] = len(p.functions)
		p.functions = append(p.functions, linkedFunction{Function: f, localReads: localReads, params: args, results: results, rSize: f.RCount, pSize: f.PCount})
	}
	if err := p.linkFunctions(m); err != nil {
		return nil, err
	}
	if err := p.linkInterfaces(m, seenShapes); err != nil {
		return nil, err
	}
	if err := p.checkJSONTypes(); err != nil {
		return nil, err
	}
	shapeStates = make(map[*values.Shape]shapeValidationState)
	for _, s := range seenShapes {
		if !validateStorageShape(s, shapeStates) {
			return nil, fmt.Errorf("invalid storage layout for %s", s.Type.Name())
		}
	}
	if err := p.linkPanicTypes(); err != nil {
		return nil, err
	}
	types := make([]values.DynamicType, len(p.types))
	for id := 1; id < len(types); id++ {
		s := p.types[id].shape
		types[id] = values.DynamicType{Shape: s, Comparable: s.Comparable()}
	}
	p.typeShapes = values.NewTypeTable(types)
	for i := range p.extendedOps {
		op := &p.extendedOps[i]
		if op.Kind == ExtendedTypeAssert && op.Shape != nil {
			op.Index = uint64(p.typeIDs[op.Shape.Type])
		}
	}
	if err := p.linkDefers(m); err != nil {
		return nil, err
	}
	p.shapeByType = make(map[*values.Type]*values.Shape, len(seenShapes))
	for _, s := range seenShapes {
		p.shapeByType[s.Type] = s
		if s.Kind == values.InterfaceKind {
			for _, m := range s.Methods {
				p.requirements[s] = append(p.requirements[s], methodRequirement{p.methodID(m.ID), m})
			}
		}

	}
	for i := range p.functions {
		f := &p.functions[i]
		for pc, ins := range f.Code {
			switch ins.Op() {
			case OpCallFunc, OpCallHost, OpCallHostRR, OpCallHostRP, OpCallIndirect, OpCallVirt, OpDefer:
			default:
				if ins.Op().hostForm() == nil {
					continue
				}
			}
			r, ptr, ok := p.callSize(ins)
			if !ok {
				return nil, fmt.Errorf("%s pc %d: invalid call target", f.Name, pc)
			}
			f.rSize = max(f.rSize, f.RCount+r)
			f.pSize = max(f.pSize, f.PCount+ptr)
		}
		if f.rSize > 65536 || f.pSize > 65536 {
			return nil, fmt.Errorf("%s: locals and outgoing window exceed register address range", f.Name)
		}
		for pc, ins := range f.Code {
			if err := p.validate(f, pc, ins); err != nil {
				return nil, fmt.Errorf("%s pc %d: %w", f.Name, pc, err)
			}
			if !m.NoOptimization && pc > 0 && ins.Op() == OpExtended && f.Code[pc-1].Op() == OpExtended {
				first := f.Code[pc-1]
				left, right := &p.extendedOps[first.BC()], &p.extendedOps[ins.BC()]
				if left.Kind == ExtendedGetChildKey && right.Kind == ExtendedGetChildKey && left.Record.Model == right.Record.Model && left.Index == right.Index && left.In[0] == right.In[0] {
					f.Code[pc-1] = WithBC(opReadChildPair, 0, first.BC())
				}
			}
			if !m.NoOptimization && pc > 0 {
				prev := f.Code[pc-1]
				if prev.Op() == opCallDirect && prev.A() == ins.A() {
					repr := p.natives[prev.C()].form.result
					if repr == values.Raw && (ins.Op() == OpJz || ins.Op() == OpJnz) || repr == values.Pointer && (ins.Op() == OpJNil || ins.Op() == OpJNotNil) {
						f.Code[pc-1] = ABC(opTestDirect, prev.A(), prev.B(), prev.C())
					}
				}
			}
			// A direct jump to the second word must still execute the branch.
			if !m.NoOptimization && pc > 0 && (ins.Op() == OpJz || ins.Op() == OpJnz) {
				cmp := f.Code[pc-1]
				if cmp.A() == ins.A() {
					var op Opcode
					switch cmp.Op() {
					case OpLtI64:
						op = opBranchLtI64
					case OpLeI64:
						op = opBranchLeI64
					case OpLtF64:
						op = opBranchLtF64
					case OpEqR:
						op = opBranchEqR
					}
					if op != 0 {
						f.Code[pc-1] = ABC(op, cmp.A(), cmp.B(), cmp.C())
					}
				}
			}
			if ins.Op() == OpCallHostUU && p.natives[ins.C()].Direct.word != nil {
				f.Code[pc] = ABC(opCallRawUnary, ins.A(), ins.B(), ins.C())
			} else if ins.Op().hostForm() != nil {
				f.Code[pc] = ABC(opCallDirect, ins.A(), ins.B(), ins.C())
			}
		}
		if !m.NoOptimization {
			if m.StrictCallWindows {
				f.definedRaw = p.rawFrameDefined(f, m.Functions[i].Code)
			}
			p.linkFloatLoops(f)
		}
	}
	if m.Init != "" {
		i, ok := p.entries[m.Init]
		if !ok {
			return nil, fmt.Errorf("missing package init")
		}
		f := p.functions[i]
		if len(f.Signature.Params) != 0 || len(f.Signature.Results) != 0 {
			return nil, fmt.Errorf("invalid package init signature")
		}
		p.init = i
	}
	for i := range p.functions {
		f := &p.functions[i]
		if f.Internal || f.Name == m.Init {
			delete(p.entries, f.Name)
		}
	}
	return p, nil
}

// UnaryHostOpcode selects the fused argument move, external call and result move.
func UnaryHostOpcode(sig Signature) Opcode {
	if len(sig.Params) == 1 && len(sig.Results) == 1 && sig.Params[0].Repr() == values.Raw {
		switch sig.Results[0].Repr() {
		case values.Raw:
			return OpCallHostRR
		case values.Pointer:
			return OpCallHostRP
		}
	}
	return 0
}

func (p *Program) callSize(i Instruction) (r, ptr int, ok bool) {
	var params, results Layout
	if i.Op().hostForm() != nil {
		if int(i.C()) >= p.nativeImports {
			return 0, 0, false
		}
		g := &p.natives[i.C()]
		if i.Op() != g.Direct.Op {
			return 0, 0, false
		}
		return 0, 0, true
	}
	switch i.Op() {
	case OpDefer:
		if uint64(i.BC()) >= uint64(len(p.defers)) {
			return 0, 0, false
		}
		r, ptr := p.deferSize(&p.defers[i.BC()])
		return r, ptr, true
	case OpCallVirt:
		if uint64(i.BC()) >= uint64(len(p.invokes)) {
			return 0, 0, false
		}
		op := &p.invokes[i.BC()]
		return op.rSize, op.pSize, true
	case OpCallIndirect:
		if uint64(i.BC()) >= uint64(len(p.signatures)) {
			return 0, 0, false
		}
		s := p.signatures[i.BC()]
		params, results = s.params, s.results
	case OpCallFunc:
		if uint64(i.BC()) >= uint64(len(p.functions)) {
			return 0, 0, false
		}
		g := &p.functions[i.BC()]
		if len(g.Captures) > 0 {
			return 0, 0, false
		}
		params, results = g.params, g.results
	case OpCallHost, OpCallHostRR, OpCallHostRP:
		index := i.BC()
		if i.Op() != OpCallHost {
			index = uint32(i.C())
		}
		if uint64(index) >= uint64(p.nativeImports) {
			return 0, 0, false
		}
		g := &p.natives[index]
		if i.Op() != OpCallHost && i.Op() != UnaryHostOpcode(g.Signature) {
			return 0, 0, false
		}
		params, results = g.params, g.results
	default:
		return 0, 0, false
	}
	return max(params.RCount, results.RCount), max(params.PCount, results.PCount), true
}

func (p *Program) validate(f *linkedFunction, pc int, i Instruction) error {
	a, b, c := int(i.A()), int(i.B()), int(i.C())
	r := func(n int) bool { return n < f.rSize }
	ptr := func(n int) bool { return n < f.pSize }
	v := func(n int) bool { return r(n) && ptr(n) }
	valid := false
	switch i.Op() {
	case OpDeferStack:
		valid = r(a) && b == 0 && c == 0
	case OpRunDefers:
		valid = a == 0 && b == 0 && c == 0
	case OpPanic, OpRecover:
		valid = v(a) && b == 0 && c == 0
	case OpDefer:
		if uint64(i.BC()) < uint64(len(p.defers)) {
			d := &p.defers[i.BC()]
			valid = (!d.ExternalStack && a == 0 || d.ExternalStack && r(a)) && p.validDefer(f, d)
		}
	case OpCallVirt:
		valid = a == 0 && uint64(i.BC()) < uint64(len(p.invokes))
	case OpFunction:
		valid = v(a) && uint64(i.BC()) < uint64(len(p.functionRefs))
		if valid {
			ref := p.functionRefs[i.BC()]
			valid = ref.Native || len(p.functions[ref.Index].Captures) == 0
		}
	case OpClosure:
		if v(a) && uint64(i.BC()) < uint64(len(p.closures)) {
			valid = true
			for _, r := range p.closures[i.BC()].Captures {
				if r.Repr < values.Raw || r.Repr > values.Pair || r.Repr != values.Pointer && !rValid(r, f) || r.Repr != values.Raw && !pValid(r, f) {
					valid = false
				}
			}
		}
	case OpCallIndirect:
		_, _, ok := p.callSize(i)
		valid = v(a) && ok
	case OpCheckNil:
		valid = ptr(a) && b == 0 && c == 0
	case OpAlloc:
		valid = ptr(a) && b < len(p.shapes) && c == 0
	case OpGlobal:
		valid = ptr(a) && uint64(i.BC()) < uint64(len(p.globals))
	case OpMulAddF64, OpAddMulF64, OpSubAddF64:
		a, b, c, d := i.ABCD12()
		valid = r(int(a)) && r(int(b)) && r(int(c)) && r(int(d))
	case OpAddImmI64:
		valid = r(a) && r(b)
	case OpLoadIndexPtr:
		valid = ptr(a) && v(b) && r(c)
	case OpStoreIndexPtr:
		valid = v(a) && r(b) && ptr(c)
	case OpLoadIndexR, OpLoadIndexByte, OpLoadIndexWord:
		valid = r(a) && v(b) && r(c)
	case OpStoreIndexR, OpStoreIndexByte, OpStoreIndexWord:
		valid = v(a) && r(b) && r(c)
	case OpLeI64, OpLeU64:
		valid = r(a) && r(b) && r(c)
	case OpLoad, OpStore:
		if c < len(p.shapes) {
			repr := p.shapes[c].Type.Repr()
			valueReg, pointerReg := a, b
			if i.Op() == OpStore {
				valueReg, pointerReg = b, a
			}
			valid = ptr(pointerReg) && (repr == values.Raw && r(valueReg) || repr == values.Pointer && ptr(valueReg) || repr == values.Pair && v(valueReg))
		}
	case OpExtended:
		valid = a == 0 && uint64(i.BC()) < uint64(len(p.extendedOps)) && p.validateExtended(f, p.extendedOps[i.BC()])
	case OpConvert:
		valid = r(a) && r(b) && c < len(p.conversions)
	case OpCheckShift:
		valid = r(a) && b == 0 && c == 0
	case OpConcatStr:
		valid = v(a) && v(b) && v(c)
	case OpEqStr, OpLtStr:
		valid = r(a) && v(b) && v(c)
	case OpAddF32, OpSubF32, OpMulF32, OpDivF32, OpEqF32, OpLtF32:
		valid = r(a) && r(b) && r(c)
	case OpConst:
		valid = r(a) && uint64(i.BC()) < uint64(len(p.words))
	case OpString:
		valid = v(a) && uint64(i.BC()) < uint64(len(p.strings))
	case OpMoveR:
		valid = r(a) && r(b) && c == 0
	case OpMoveP:
		valid = ptr(a) && ptr(b) && c == 0
	case OpMoveV:
		valid = v(a) && v(b) && c == 0
	case OpClearP:
		valid = ptr(a) && b == 0 && c == 0
	case OpEqP:
		valid = r(a) && ptr(b) && ptr(c)
	case OpAddI64, OpSubI64, OpMulI64, OpDivI64, OpDivU64, OpRemI64, OpRemU64,
		OpAddF64, OpSubF64, OpMulF64, OpDivF64, OpAnd, OpOr, OpXor, OpShl,
		OpShrI64, OpShrU64, OpEqR, OpLtI64, OpLtU64, OpEqF64, OpLtF64:
		valid = r(a) && r(b) && r(c)
	case OpCallHostRR:
		valid = a < f.RCount && b < f.RCount
	case OpCallHostRP:
		valid = a < f.PCount && b < f.RCount
	case OpJNil, OpJNotNil:
		target := int64(pc) + 1 + int64(int32(i.BC()))
		valid = ptr(a) && target >= 0 && target < int64(len(f.Code))
	case OpLoopAddLtI64, OpLoopAddLeI64:
		a, b, c, _ := i.ABCD12()
		target := pc + 1 + i.SignedD12()
		valid = r(int(a)) && r(int(b)) && r(int(c)) && target >= 0 && target < len(f.Code)
	case OpLoopLtI64, OpLoopLeI64:
		target := pc + 1 + int(int16(i.C()))
		valid = r(a) && r(b) && target >= 0 && target < len(f.Code)
	case OpJump, OpJz, OpJnz:
		target := int64(pc) + 1 + int64(int32(i.BC()))
		valid = target >= 0 && target < int64(len(f.Code)) && ((i.Op() == OpJump && a == 0) || (i.Op() != OpJump && r(a)))
	case OpCallFunc, OpCallHost:
		_, _, ok := p.callSize(i)
		valid = a == 0 && ok
	case OpReturn:
		valid = a == 0 && b == 0 && c == 0
	default:
		if h := i.Op().hostForm(); h != nil {
			if c < len(p.natives) && p.natives[c].form != nil {
				h = p.natives[c].form
			}
			valid = h.validSlots(a, b, f)
		}
	}
	if !valid {
		return fmt.Errorf("invalid opcode or operands: %#016x", uint64(i))
	}
	if pc == len(f.Code)-1 && i.Op() != OpReturn && i.Op() != OpPanic && i.Op() != OpJump {
		return fmt.Errorf("execution can fall past end of function")
	}
	return nil
}
