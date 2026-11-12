// Package evmbind builds a reusable EVM host schema from Go declarations.
package evmbind

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

// nodeKind classifies Go types for binding traversal and code generation.
// Aliases such as byte and rune use their underlying scalar kind; node.name
// retains their Go spelling for emitted declarations.
type nodeKind uint8

const (
	nodeInvalid nodeKind = iota
	nodeBool
	nodeInt
	nodeInt8
	nodeInt16
	nodeInt32
	nodeInt64
	nodeUint
	nodeUint8
	nodeUint16
	nodeUint32
	nodeUint64
	nodeFloat32
	nodeFloat64
	nodeString
	nodeCallback
	nodeStrings
	nodeJSONObject
	nodeStruct
	nodePointer
	nodeInterface
)

func (k nodeKind) String() string {
	switch k {
	case nodeInvalid:
		return "invalid"
	case nodeBool:
		return "bool"
	case nodeInt:
		return "int"
	case nodeInt8:
		return "int8"
	case nodeInt16:
		return "int16"
	case nodeInt32:
		return "int32"
	case nodeInt64:
		return "int64"
	case nodeUint:
		return "uint"
	case nodeUint8:
		return "uint8"
	case nodeUint16:
		return "uint16"
	case nodeUint32:
		return "uint32"
	case nodeUint64:
		return "uint64"
	case nodeFloat32:
		return "float32"
	case nodeFloat64:
		return "float64"
	case nodeString:
		return "string"
	case nodeCallback:
		return "callback"
	case nodeStrings:
		return "strings"
	case nodeJSONObject:
		return "jsonObject"
	case nodeStruct:
		return "struct"
	case nodePointer:
		return "ptr"
	case nodeInterface:
		return "interface"
	default:
		return "nodeKind(" + strconv.Itoa(int(k)) + ")"
	}
}

func basicNodeKind(k types.BasicKind) nodeKind {
	switch k {
	case types.Bool:
		return nodeBool
	case types.Int:
		return nodeInt
	case types.Int8:
		return nodeInt8
	case types.Int16:
		return nodeInt16
	case types.Int32:
		return nodeInt32
	case types.Int64:
		return nodeInt64
	case types.Uint:
		return nodeUint
	case types.Uint8:
		return nodeUint8
	case types.Uint16:
		return nodeUint16
	case types.Uint32:
		return nodeUint32
	case types.Uint64:
		return nodeUint64
	case types.Float32:
		return nodeFloat32
	case types.Float64:
		return nodeFloat64
	case types.String:
		return nodeString
	default:
		return nodeInvalid
	}
}

type node struct {
	name    string
	shape   *values.Shape
	kind    nodeKind
	fields  []field
	elem    *node
	named   bool
	methods []*operation
}
type field struct {
	name   string
	t      *node
	offset uint64
}
type operation struct {
	name, key  string
	recv       *node
	args, outs []*node
}
type source struct {
	pkg         evm.Package
	nodes       map[string]*node
	ops         []*operation
	fingerprint string
	name        string
}

func spelling(t types.Type) string {
	return types.TypeString(t, func(*types.Package) string { return "" })
}
func parse(path string, files fs.FS) (s *source, err error) {
	// Schema errors originate in recursive type traversal; return them at this boundary.
	defer func() {
		if x := recover(); x != nil {
			if e, ok := x.(schemaError); ok {
				err = e
				s = nil
			} else {
				panic(x)
			}
		}
	}()
	names, err := fs.Glob(files, "*.go")
	if err != nil {
		return nil, err
	}
	var syntax []*ast.File
	fset := token.NewFileSet()
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, e := fs.ReadFile(files, name)
		if e != nil {
			return nil, e
		}
		f, e := parser.ParseFile(fset, name, b, parser.SkipObjectResolution)
		if e != nil {
			return nil, e
		}
		syntax = append(syntax, f)
	}
	if len(syntax) == 0 {
		return nil, fmt.Errorf("missing Go declarations")
	}
	pkg, err := (&types.Config{GoVersion: "go1.23", Sizes: types.SizesFor("gc", "amd64"), Importer: evmcompiler.SourceImporter(fset)}).Check(path, fset, syntax, nil)
	if err != nil {
		return nil, err
	}
	s = &source{name: pkg.Name(), pkg: evm.Package{Path: path}, nodes: map[string]*node{}}
	var abi []string
	for _, name := range pkg.Scope().Names() {
		obj := pkg.Scope().Lookup(name)
		if !obj.Exported() {
			continue
		}
		switch o := obj.(type) {
		case *types.TypeName:
			n := s.node(o.Type())
			abi = append(abi, name+" "+spelling(o.Type().Underlying()))
			named, ok := o.Type().(*types.Named)
			if !ok {
				fail("unsupported alias %s", name)
			}
			if n.kind == nodeInterface {
				for _, op := range n.methods {
					abi = append(abi, op.abi())
				}
			} else {
				for i := 0; i < named.NumMethods(); i++ {
					m := named.Method(i)
					if m.Exported() {
						sig := m.Type().(*types.Signature)
						op := s.op(m, s.node(sig.Recv().Type()))
						s.ops = append(s.ops, op)
						abi = append(abi, op.abi())
					}
				}
			}
		case *types.Func:
			op := s.op(o, nil)
			s.ops = append(s.ops, op)
			abi = append(abi, op.abi())
		case *types.Const:
			if b, ok := o.Type().(*types.Basic); ok && b.Info()&types.IsUntyped != 0 {
				s.pkg.Constants = append(s.pkg.Constants, evm.ExternalConstant{Name: name, Untyped: o.Val(), Rune: b.Kind() == types.UntypedRune})
				abi = append(abi, "const "+name+" "+b.Name()+" "+o.Val().ExactString())
				continue
			}
			t := types.Default(o.Type())
			n := s.node(t)
			var v values.Value
			switch n.shape.Kind {
			case values.IntKind:
				x, ok := constant.Int64Val(o.Val())
				if !ok {
					fail("constant overflow: %s", name)
				}
				v = values.Int(x)
			case values.UintKind:
				x, ok := constant.Uint64Val(o.Val())
				if !ok {
					fail("constant overflow: %s", name)
				}
				v = values.Uint(x)
			case values.FloatKind:
				if n.shape.Bits == 32 {
					x, _ := constant.Float32Val(o.Val())
					v = values.Uint(uint64(math.Float32bits(x)))
				} else {
					x, _ := constant.Float64Val(o.Val())
					v = values.Float(x)
				}
			case values.BoolKind:
				v = values.Bool(constant.BoolVal(o.Val()))
			case values.StringKind:
				v = values.String(constant.StringVal(o.Val()))
			default:
				fail("unsupported constant %s", name)
			}
			s.pkg.Constants = append(s.pkg.Constants, evm.ExternalConstant{Name: name, Type: n.shape.Type, Value: v})
			abi = append(abi, "const "+name+" "+n.name+" "+o.Val().ExactString())
		default:
			fail("unsupported export %s", name)
		}
	}
	sort.Strings(abi)
	s.fingerprint = fmt.Sprintf("%x", sha256.Sum256([]byte("evmbind2\n"+path+"\n"+strings.Join(abi, "\n"))))
	return s, nil
}

type schemaError struct{ error }

func fail(f string, a ...any) { panic(schemaError{fmt.Errorf(f, a...)}) }
func (op *operation) abi() string {
	var p, r []string
	for _, n := range op.args {
		p = append(p, n.name)
	}
	for _, n := range op.outs {
		r = append(r, n.name)
	}
	return op.key + "(" + strings.Join(p, ",") + ")(" + strings.Join(r, ",") + ")"
}
func (s *source) op(f *types.Func, recv *node) *operation {
	sig := f.Type().(*types.Signature)
	if sig.Variadic() || sig.TypeParams().Len() > 0 {
		fail("unsupported signature %s", f.Name())
	}
	op := &operation{name: f.Name(), key: f.Name(), recv: recv}
	if recv != nil {
		op.key = recv.name + "." + op.name
	}
	for i := 0; i < sig.Params().Len(); i++ {
		op.args = append(op.args, s.node(sig.Params().At(i).Type()))
	}
	for i := 0; i < sig.Results().Len(); i++ {
		n := s.node(sig.Results().At(i).Type())
		if n.inputOnly() {
			fail("%s: %s is only supported as a host input", f.Name(), n.name)
		}
		op.outs = append(op.outs, n)
	}
	return op
}
func nodesTypes(ns []*node) []*values.Type {
	var out []*values.Type
	for _, n := range ns {
		out = append(out, n.shape.Type)
	}
	return out
}
func (op *operation) directOpcode() evm.Opcode {
	var receiver values.Repr
	if op.recv != nil {
		receiver = directBank(op.recv.shape)
		if receiver != values.Pointer {
			return 0
		}
	}
	args, results := make([]values.Repr, len(op.args)), make([]values.Repr, len(op.outs))
	for i, n := range op.args {
		args[i] = directBank(n.shape)
	}
	for i, n := range op.outs {
		results[i] = directBank(n.shape)
	}
	return directOpcode(receiver, args, results)
}
func (op *operation) sig() evm.Signature {
	return evm.Signature{Params: nodesTypes(op.args), Results: nodesTypes(op.outs)}
}

var basics = map[nodeKind]*values.Type{
	nodeFloat32: values.Float32Type,
	nodeFloat64: values.Float64,
	nodeString:  values.StringType,
	nodeBool:    values.Boolean,
	nodeInt:     values.IntType,
	nodeInt8:    values.Int8,
	nodeInt16:   values.Int16,
	nodeInt32:   values.Int32,
	nodeInt64:   values.Int64,
	nodeUint:    values.UintType,
	nodeUint8:   values.Uint8,
	nodeUint16:  values.Uint16,
	nodeUint32:  values.Uint32,
	nodeUint64:  values.Uint64,
}

func (n *node) inputOnly() bool {
	return n.kind == nodeStrings || n.kind == nodeJSONObject || n.kind == nodeCallback
}

func (s *source) node(t types.Type) *node {
	name := spelling(t)
	if n := s.nodes[name]; n != nil {
		return n
	}
	n := &node{name: name, shape: &values.Shape{Cells: 1}}
	s.nodes[name] = n
	named, ok := t.(*types.Named)
	n.named = ok
	if ok && (!named.Obj().Exported() || named.TypeParams().Len() > 0) {
		fail("unsupported native binding type %s", name)
	}
	sh := n.shape
	repr := values.Raw
	switch u := t.Underlying().(type) {
	case *types.Basic:
		n.kind = basicNodeKind(u.Kind())
		base := basics[n.kind]
		if base == nil {
			fail("unsupported scalar %s", name)
		}
		sh.Type = base
		repr = base.Repr()
		switch {
		case u.Info()&types.IsString != 0:
			sh.Kind = values.StringKind
		case u.Info()&types.IsBoolean != 0:
			sh.Kind = values.BoolKind
		case u.Info()&types.IsFloat != 0:
			sh.Kind = values.FloatKind
		case u.Info()&types.IsUnsigned != 0:
			sh.Kind = values.UintKind
		default:
			sh.Kind = values.IntKind
		}
		if u.Info()&(types.IsInteger|types.IsFloat) != 0 {
			sh.Bits = uint8(types.SizesFor("gc", "amd64").Sizeof(u) * 8)
		}
	case *types.Signature:
		if n.named || u.Params().Len() != 0 || u.Results().Len() != 0 {
			fail("unsupported host callback %s", name)
		}
		n.kind, repr, sh.Kind = nodeCallback, values.Pair, values.FunctionKind
	case *types.Slice:
		if n.named || !types.Identical(u.Elem(), types.Typ[types.String]) {
			fail("unsupported host boundary type %s", name)
		}
		n.kind = nodeStrings
		repr = values.Pair
		sh.Kind = values.SliceKind
		sh.Elem = s.node(u.Elem()).shape
	case *types.Map:
		if n.named || !types.Identical(u.Key(), types.Typ[types.String]) || !types.Identical(u.Elem(), types.Universe.Lookup("any").Type()) {
			fail("unsupported host boundary type %s", name)
		}
		n.kind = nodeJSONObject
		repr = values.Pointer
		sh.Kind = values.MapKind
		sh.Key = s.node(u.Key()).shape
		sh.Elem = &values.Shape{Type: values.AnyType, Kind: values.InterfaceKind, Cells: 1}
	case *types.Struct:
		if !n.named {
			fail("anonymous struct %s", name)
		}
		repr = values.Pointer
		n.kind = nodeStruct
		sh.Kind = values.StructKind
		sh.Cells = 0
		for i := 0; i < u.NumFields(); i++ {
			f := u.Field(i)
			if !f.Exported() || f.Embedded() {
				fail("unsupported field %s.%s", name, f.Name())
			}
			c := s.node(f.Type())
			if c.kind == nodeInterface || c.inputOnly() {
				fail("reference field %s.%s is not supported", name, f.Name())
			}
			n.fields = append(n.fields, field{f.Name(), c, sh.Cells})
			sh.Fields = append(sh.Fields, values.Field{Name: f.Name(), Tag: u.Tag(i), Shape: c.shape, Offset: sh.Cells})
			sh.Cells += c.shape.Cells
		}
	case *types.Pointer:
		if n.named {
			fail("named pointer type %s is unsupported", name)
		}
		repr = values.Pair
		n.kind = nodePointer
		sh.Kind = values.PointerKind
		n.elem = s.node(u.Elem())
		sh.Elem = n.elem.shape
		if n.elem.kind != nodeStruct && basics[n.elem.kind] == nil {
			fail("expected pointer to data struct or scalar: %s", name)
		}
	case *types.Interface:
		repr = values.Pair
		n.kind = nodeInterface
		sh.Kind = values.InterfaceKind
		if !u.IsMethodSet() || !n.named {
			fail("expected named method interface: %s", name)
		}
		for i := 0; i < u.NumMethods(); i++ {
			m := u.Method(i)
			if !m.Exported() {
				fail("unexported interface method %s", m.Name())
			}
			op := s.op(m, n)
			n.methods = append(n.methods, op)
			fs := &values.Shape{Type: values.NewType(op.key, values.Pair), Kind: values.FunctionKind, Cells: 1}
			for _, x := range op.args {
				fs.Params = append(fs.Params, x.shape)
			}
			for _, x := range op.outs {
				fs.Results = append(fs.Results, x.shape)
			}
			sh.Methods = append(sh.Methods, values.Method{ID: m.Name(), Signature: fs})
		}
	default:
		fail("unsupported host boundary type %s", name)
	}
	if n.named || sh.Type == nil {
		if n.kind == nodeJSONObject {
			sh.Type = evm.JSONObjectType
		} else {
			sh.Type = values.NewType(s.pkg.Path+"."+name, repr)
		}
		decl := evm.TypeDecl{Shape: sh}
		if n.named {
			decl.Name = name
			decl.ScalarBase = basics[n.kind]
		}
		s.pkg.Types = append(s.pkg.Types, decl)
	}
	return n
}
