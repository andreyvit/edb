package evmbind

import (
	"fmt"
	"io/fs"
	"reflect"
	"slices"
	"sort"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

type Mode = nativebind.Mode

const (
	Automatic  = nativebind.Automatic
	StaticOnly = nativebind.StaticOnly
)

type Wrapper func(*Frame)

// DirectFactory binds a generated register wrapper once per external receiver.
type DirectFactory func(host any) evm.DirectFunc

type Static struct {
	Fields      map[string]*nativebind.GeneratedFields
	Direct      map[string]DirectFactory
	Types       map[string]reflect.Type
	Fingerprint string
	Calls       map[string]Wrapper
}
type Options struct {
	Mode   Mode
	Static *Static
	// Host is the concrete receiver of package functions. Its methods take a
	// leading *evm.Call for access to the session and source position.
	Host reflect.Type
	// Types associate guest names with native Go declarations.
	Types []reflect.Type
	// Implementations are pointer types admitted into guest interfaces.
	Implementations []Implementation
}

// Implementation binds one concrete Go pointer type to its program-local ID.
// The conversion functions preserve the pointer without allocating an interface cell.
type Implementation struct {
	typ      reflect.Type
	load     func(unsafe.Pointer) any
	pointer  func(any) unsafe.Pointer
	receiver func(unsafe.Pointer, *evm.Call) any
}

func Pointer[T any]() Implementation {
	return Implementation{typ: reflect.TypeFor[*T](), load: func(p unsafe.Pointer) any { return (*T)(p) }, pointer: func(x any) unsafe.Pointer { return unsafe.Pointer(x.(*T)) }}
}

// PointerForCall lets a native receiver use the current external call (for
// example to narrow a budget). The interface value still carries the original
// pointer. The receiver returned by bind is used only for this method call.
func PointerForCall[T any](bind func(*T, *evm.Call) *T) Implementation {
	impl := Pointer[T]()
	impl.receiver = func(p unsafe.Pointer, c *evm.Call) any { return bind((*T)(p), c) }
	return impl
}

type nativePointer struct {
	Implementation

	vmType *values.Type
}
type entry struct {
	direct  DirectFactory
	op      *operation
	wrapper Wrapper
	pointer *nativePointer
}
type Schema struct {
	src            *source
	pkg            evm.Package
	funcs, methods []entry
	host           reflect.Type
	pointers       map[reflect.Type]*nativePointer
	reverse        map[*values.Type]*nativePointer
	static         bool
	fields         map[reflect.Type]*nativebind.Struct
	nativeTypes    map[reflect.Type]*values.Type
}

func (s *Schema) UsesStatic() bool     { return s.static }
func (s *Schema) Package() evm.Package { return s.pkg }
func (s *Schema) Type(name string) *values.Type {
	if n := s.src.nodes[name]; n != nil {
		return n.shape.Type
	}
	return nil
}
func New(path string, files fs.FS, opt Options) (schema *Schema, err error) {
	src, err := parse(path, files)
	if err != nil {
		return nil, err
	}
	defer func() {
		if x := recover(); x != nil {
			if e, ok := x.(schemaError); ok {
				schema = nil
				err = e
			} else {
				panic(x)
			}
		}
	}()
	s := &Schema{src: src, pkg: src.pkg, host: opt.Host, pointers: map[reflect.Type]*nativePointer{}, reverse: map[*values.Type]*nativePointer{}}
	if opt.Mode > StaticOnly {
		fail("invalid binding mode")
	}
	var expected []*operation
	expected = append(expected, src.ops...)
	var interfaces []*node
	for _, d := range src.pkg.Types {
		n := src.nodes[d.Name]
		if n != nil && n.kind == nodeInterface {
			interfaces = append(interfaces, n)
			expected = append(expected, n.methods...)
		}
	}
	native := resolveNativeTypes(src, opt)
	var fingerprint string
	compatible := opt.Static != nil
	if opt.Static != nil {
		fingerprint = opt.Static.Fingerprint
		compatible = len(opt.Static.Calls) == len(expected)
		validDirect := 0
		for _, op := range expected {
			if opt.Static.Direct[op.key] != nil && op.directOpcode() != 0 {
				validDirect++
			}
			if opt.Static.Calls[op.key] == nil {
				compatible = false
				break
			}
		}
		compatible = compatible && validDirect == len(opt.Static.Direct)
		for _, n := range src.nodes {
			if n.named && native[n] != opt.Static.Types[n.name] {
				compatible = false
				break
			}
		}
	}
	s.static = opt.Mode.Select(path, src.fingerprint, fingerprint, compatible)
	s.fields = make(map[reflect.Type]*nativebind.Struct)
	s.nativeTypes = make(map[reflect.Type]*values.Type)
	for n, t := range native {
		s.nativeTypes[t] = n.shape.Type
		n.shape.Native = nativebind.Layout(t, nil, s.static)
		if n.kind == nodeStruct {
			var g *nativebind.GeneratedFields
			if s.static {
				g = opt.Static.Fields[n.name]
			}
			s.fields[t] = nativebind.BindFields(t, g, opt.Mode)
			n.shape.Native = nativebind.Layout(t, s.fields[t], s.static)
		}
	}

	wrappers := map[string]Wrapper{}
	for _, op := range expected {
		// Validate the actual host ABI once, even when generated code is supplied.
		w := reflectWrapper(op, opt.Host, native)
		if s.static {
			w = opt.Static.Calls[op.key]
		}
		wrappers[op.key] = w
	}
	for _, op := range src.ops {
		e := entry{op: op, wrapper: wrappers[op.key]}
		var direct evm.DirectBinding
		if s.static && opt.Static.Direct[op.key] != nil {
			direct.Op = op.directOpcode()
			direct.Window = op.recv == nil
			e.direct = opt.Static.Direct[op.key]
		}
		if op.recv == nil {
			s.pkg.Functions = append(s.pkg.Functions, evm.ExternalFunction{Direct: direct, Name: op.name, Signature: op.sig()})
			s.funcs = append(s.funcs, e)
		} else {
			s.method(op, op.recv.shape.Type, e, direct)
		}
	}
	for i, impl := range opt.Implementations {
		s.addImplementation(i, impl, interfaces, native, wrappers)
	}

	return s, nil
}
func resolveNativeTypes(src *source, opt Options) map[*node]reflect.Type {
	reg := map[string]reflect.Type{}
	// Go cannot enumerate a package by name. Discover concrete data types from
	// host signatures. Explicit roots cover standalone types such as entry interfaces.
	seenTypes := map[reflect.Type]bool{}
	var collect func(reflect.Type)
	collect = func(t reflect.Type) {
		if t == nil || seenTypes[t] {
			return
		}
		seenTypes[t] = true
		if t.Kind() == reflect.Pointer {
			collect(t.Elem())
			return
		}
		if n := src.nodes[t.Name()]; n != nil && n.named {
			if prev := reg[n.name]; prev != nil && prev != t {
				fail("conflicting native type %s", n.name)
			}
			reg[n.name] = t
			if t.Kind() == reflect.Struct {
				for i := 0; i < t.NumField(); i++ {
					collect(t.Field(i).Type)
				}
			}
		}
	}
	for _, t := range opt.Types {
		collect(t)
	}
	for _, op := range src.ops {
		if op.recv == nil && opt.Host != nil {
			if m, ok := opt.Host.MethodByName(op.name); ok {
				for i := 2; i < m.Type.NumIn(); i++ {
					collect(m.Type.In(i))
				}
				for i := 0; i < m.Type.NumOut(); i++ {
					collect(m.Type.Out(i))
				}
			}
		}
	}
	if opt.Static != nil {
		for name, t := range opt.Static.Types {
			if reg[name] == nil {
				reg[name] = t
			}
		}
	}
	native := map[*node]reflect.Type{}
	for _, n := range src.nodes {
		nativeType(n, reg, native)
	}
	return native
}
func (s *Schema) addImplementation(i int, impl Implementation, interfaces []*node, native map[*node]reflect.Type, wrappers map[string]Wrapper) {

	t := impl.typ
	if t == nil || t.Kind() != reflect.Pointer || s.pointers[t] != nil {
		fail("invalid host pointer %v", t)
	}
	// An opaque pointer has no guest-accessible fields. Never describe the Go
	// object's memory as EVM cells.
	name := fmt.Sprintf("evmOpaque%d", i)
	if s.src.nodes[name] != nil {
		fail("reserved host type %s", name)
	}
	empty := &values.Shape{Type: values.NewType(s.pkg.Path+"."+name, values.Pointer), Kind: values.StructKind}
	ptr := &values.Shape{Type: values.NewType("*"+s.pkg.Path+"."+name, values.Pointer), Kind: values.PointerKind, Elem: empty, Cells: 1}
	np := &nativePointer{Implementation: impl, vmType: ptr.Type}
	s.pointers[t] = np
	s.reverse[ptr.Type] = np
	s.pkg.Types = append(s.pkg.Types, evm.TypeDecl{Name: name, Shape: empty}, evm.TypeDecl{Shape: ptr})
	s.pkg.EntryTypes = append(s.pkg.EntryTypes, ptr.Type)
	seen := map[string]bool{}
	matches := false
	for _, n := range interfaces {
		if t.Implements(native[n]) {
			matches = true
			for _, op := range n.methods {
				if !seen[op.name] {
					s.method(op, ptr.Type, entry{op: op, wrapper: wrappers[op.key], pointer: np}, evm.DirectBinding{})
					seen[op.name] = true
				}
			}
		}
	}
	if !matches {
		fail("%s implements no exposed interface", t)
	}

}
func (s *Schema) method(op *operation, recv *values.Type, e entry, direct evm.DirectBinding) {
	s.pkg.Methods = append(s.pkg.Methods, evm.ExternalMethod{Direct: direct, Name: op.name, Receiver: recv, Signature: op.sig()})
	s.methods = append(s.methods, e)
}
func (s *Schema) Bind(host any) evm.Package {
	if reflect.TypeOf(host) != s.host {
		panic(fmt.Sprintf("evmbind: expected host %s, got %T", s.host, host))
	}
	pkg := s.pkg
	pkg.Functions = slices.Clone(pkg.Functions)
	pkg.Methods = slices.Clone(pkg.Methods)
	for i, e := range s.funcs {
		pkg.Functions[i].Call = s.bind(e, host)
		if e.direct != nil {
			pkg.Functions[i].Direct.Call = e.direct(host)
		}
	}
	for i, e := range s.methods {
		pkg.Methods[i].Call = s.bind(e, host)
		if e.direct != nil {
			pkg.Methods[i].Direct.Call = e.direct(host)
		}
	}
	return pkg
}
func (s *Schema) bind(e entry, host any) evm.Native {
	borrowsCallback := slices.ContainsFunc(e.op.args, func(n *node) bool { return n.kind == nodeCallback })
	return func(c *evm.Call) (err error) {
		defer Catch(e.op.key, &err)
		f := Frame{Call: c, Host: host, s: s}
		if borrowsCallback {
			defer func() { f.Call = nil }()
		}
		if e.pointer != nil {
			p := c.Argument(0).P
			if e.pointer.receiver != nil {
				f.Receiver = e.pointer.receiver(p, c)
			} else {
				f.Receiver = restore(e.pointer, p)
			}
		}
		e.wrapper(&f)
		f.CopyBack()
		return nil
	}
}

// Catch converts a panic from a generated conversion or Go method into the
// same source-positioned failure as the reflection binding.
func Catch(name string, err *error) {
	if x := recover(); x != nil {
		if cause, ok := x.(error); ok {
			*err = fmt.Errorf("%s: %w", name, cause)
		} else {
			*err = fmt.Errorf("%s: %v", name, x)
		}
	}
}
func restore(t *nativePointer, p unsafe.Pointer) any {
	return t.load(p)
}

// Interface creates a program-local R+P argument. P is the original Go pointer,
// including a nil pointer in a non-nil interface.
func (s *Schema) Interface(p *evm.Program, x any) (values.Value, error) {
	if x == nil {
		return values.Value{}, nil
	}
	n := s.pointers[reflect.TypeOf(x)]
	if n == nil {
		return values.Value{}, fmt.Errorf("unregistered host pointer %T", x)
	}
	return p.PointerInterface(n.vmType, n.pointer(x))
}

// NativeInterface recovers an opaque host implementation registered by this schema.
func (s *Schema) NativeInterface(p *evm.Program, v values.Value) (any, error) {
	if v == (values.Value{}) {
		return nil, nil
	}
	n := s.reverse[p.InterfaceType(v)]
	if n == nil {
		return p.NamedFunction(v)
	}
	return restore(n, v.P), nil
}

func sortedNodes(s *source) []*node {
	var ns []*node
	for _, n := range s.nodes {
		ns = append(ns, n)
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i].name < ns[j].name })
	return ns
}

// NativeFields returns the shared live-field binding for an exposed Go struct.
// Generated mode and reflection use the same operations available to KVO.
func (s *Schema) NativeFields(t reflect.Type) *nativebind.Struct { return s.fields[t] }

// NativeType returns the existing value-type identity for an exposed Go type.
func (s *Schema) NativeType(t reflect.Type) *values.Type { return s.nativeTypes[t] }

// NativePointer borrows a registered data struct pointer for a guest argument.
// The VM keeps the original object alive. Guest writes and native methods act
// on that object immediately; concurrent access requires caller synchronization.
func (s *Schema) NativePointer(x any) (values.Value, error) {
	v := reflect.ValueOf(x)
	if !v.IsValid() || v.Kind() != reflect.Pointer || s.nativeTypes[v.Type()] == nil || s.fields[v.Type().Elem()] == nil {
		return values.Value{}, fmt.Errorf("unregistered native struct pointer %T", x)
	}
	return values.Value{R: values.NativeAddress, P: v.UnsafePointer()}, nil
}
