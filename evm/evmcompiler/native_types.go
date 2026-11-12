package evmcompiler

import (
	"fmt"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"go/token"
	"go/types"
	"strings"
)

type importKey struct {
	pkg, name string
	receiver  *values.Type
}
type nativeBinding struct {
	key      importKey
	function *evm.ExternalFunction
}
type nativeType struct {
	pkg  string
	decl evm.TypeDecl
}

func (e *typeEnvironment) goSignature(sig evm.Signature, variadic bool, pkg *types.Package) (*types.Signature, error) {
	params, err := e.goTuple(sig.Params, pkg)
	if err != nil {
		return nil, err
	}
	results, err := e.goTuple(sig.Results, pkg)
	if err != nil {
		return nil, err
	}
	if variadic {
		if params.Len() == 0 {
			return nil, fmt.Errorf("variadic function has no parameters")
		}
		if _, ok := params.At(params.Len() - 1).Type().Underlying().(*types.Slice); !ok {
			return nil, fmt.Errorf("variadic parameter must be a slice")
		}
	}
	return types.NewSignatureType(nil, nil, nil, params, results, variadic), nil
}
func (e *typeEnvironment) goShape(s *values.Shape, pkg *types.Package) (types.Type, error) {
	if s == nil || s.Type == nil {
		return nil, fmt.Errorf("missing native storage shape")
	}
	if e.buildingShapes[s] {
		return nil, fmt.Errorf("recursive unnamed native shape")
	}
	e.buildingShapes[s] = true
	defer delete(e.buildingShapes, s)
	child := func(s *values.Shape) (types.Type, error) {
		if s == nil {
			return nil, fmt.Errorf("missing child shape")
		}
		if t := e.toGo[s.Type]; t != nil {
			return t, nil
		}
		if _, ok := e.nativeTypes[s.Type]; ok {
			return e.goType(s.Type, pkg)
		}
		t, err := e.goShape(s, pkg)
		if err == nil {
			if err := e.bindNative(s.Type, t); err != nil {
				return nil, err
			}
		}
		return t, err
	}
	switch s.Kind {
	case values.TimestampKind:
		return types.NewStruct(nil, nil), nil
	case values.BoolKind:
		return types.Typ[types.Bool], nil
	case values.IntKind, values.UintKind, values.FloatKind, values.ComplexKind:
		var k types.BasicKind
		switch s.Kind {
		case values.IntKind:
			switch s.Bits {
			case 8:
				k = types.Int8
			case 16:
				k = types.Int16
			case 32:
				k = types.Int32
			case 64:
				k = types.Int64
			}
		case values.UintKind:
			switch s.Bits {
			case 8:
				k = types.Uint8
			case 16:
				k = types.Uint16
			case 32:
				k = types.Uint32
			case 64:
				k = types.Uint64
			}
		case values.FloatKind:
			if s.Bits == 32 {
				k = types.Float32
			} else if s.Bits == 64 {
				k = types.Float64
			}
		case values.ComplexKind:
			if s.Bits == 64 {
				k = types.Complex64
			} else if s.Bits == 128 {
				k = types.Complex128
			}
		}
		if k == types.Invalid {
			return nil, fmt.Errorf("invalid numeric shape")
		}
		return types.Typ[k], nil
	case values.StringKind:
		return types.Typ[types.String], nil
	case values.PointerKind, values.SliceKind, values.ArrayKind:
		elem, err := child(s.Elem)
		if err != nil {
			return nil, err
		}
		switch s.Kind {
		case values.PointerKind:
			return types.NewPointer(elem), nil
		case values.SliceKind:
			return types.NewSlice(elem), nil
		default:
			if s.Len > 1<<32 {
				return nil, fmt.Errorf("array too large")
			}
			return types.NewArray(elem, int64(s.Len)), nil
		}
	case values.MapKind:
		key, err := child(s.Key)
		if err != nil {
			return nil, err
		}
		elem, err := child(s.Elem)
		if err != nil {
			return nil, err
		}
		if !types.Comparable(key) {
			return nil, fmt.Errorf("invalid map key")
		}
		return types.NewMap(key, elem), nil
	case values.StructKind:
		fields := make([]*types.Var, len(s.Fields))
		tags := make([]string, len(s.Fields))
		names := map[string]bool{}
		for i := range s.Fields {
			f := &s.Fields[i]
			if !token.IsIdentifier(f.Name) || f.Name != "_" && names[f.Name] {
				return nil, fmt.Errorf("invalid or duplicate field %q", f.Name)
			}
			names[f.Name] = true
			t, err := child(f.Shape)
			if err != nil {
				return nil, err
			}
			fields[i] = types.NewField(token.NoPos, pkg, f.Name, t, f.Embedded)
			tags[i] = f.Tag
		}
		return types.NewStruct(fields, tags), nil
	case values.FunctionKind:
		var sig evm.Signature
		for _, p := range s.Params {
			if _, err := child(p); err != nil {
				return nil, err
			}
			sig.Params = append(sig.Params, p.Type)
		}
		for _, r := range s.Results {
			if _, err := child(r); err != nil {
				return nil, err
			}
			sig.Results = append(sig.Results, r.Type)
		}
		return e.goSignature(sig, s.Variadic, pkg)
	case values.InterfaceKind:
		methods := make([]*types.Func, len(s.Methods))
		ids := map[string]bool{}
		for i := range s.Methods {
			m := &s.Methods[i]
			if ids[m.ID] {
				return nil, fmt.Errorf("duplicate interface method")
			}
			ids[m.ID] = true
			name := m.ID
			mp := pkg
			if j := strings.LastIndexByte(name, '.'); j >= 0 {
				path := name[:j]
				name = name[j+1:]
				if path != pkg.Path() {
					return nil, fmt.Errorf("unexported method belongs to another package")
				}
			}
			if !token.IsIdentifier(name) || name == "_" {
				return nil, fmt.Errorf("invalid method name")
			}
			// Different methods can share a Go signature. The method descriptor
			// does not declare a distinct type for that signature.
			t, err := e.goShape(m.Signature, pkg)
			if err != nil {
				return nil, err
			}
			sig, ok := t.Underlying().(*types.Signature)
			if !ok {
				return nil, fmt.Errorf("invalid method signature")
			}
			if m.ID != types.Id(mp, name) {
				return nil, fmt.Errorf("method identity must be %s", types.Id(mp, name))
			}
			methods[i] = types.NewFunc(token.NoPos, mp, name, sig)
		}
		return types.NewInterfaceType(methods, nil).Complete(), nil
	}
	return nil, fmt.Errorf("invalid native shape")
}
func (e *typeEnvironment) addNativeMethods(pkg *types.Package, decl *evm.Package) error {
	for i := range decl.Methods {
		m := &decl.Methods[i]
		if !decl.OwnsMethodReceiver(m.Receiver) {
			return fmt.Errorf("method receiver is not declared by package")
		}
		recv, err := e.goType(m.Receiver, pkg)
		if err != nil {
			return err
		}
		base := recv
		if p, ok := base.(*types.Pointer); ok {
			base = p.Elem()
		}
		named, ok := base.(*types.Named)
		if !ok || named.Obj().Pkg() != pkg {
			return fmt.Errorf("method receiver must be declared in package")
		}
		if _, ok := named.Underlying().(*types.Pointer); ok {
			return fmt.Errorf("method receiver base cannot be a pointer type")
		}
		if _, ok := named.Underlying().(*types.Interface); ok {
			return fmt.Errorf("interface receiver cannot have native method")
		}
		if !token.IsIdentifier(m.Name) || m.Name == "_" {
			return fmt.Errorf("invalid method name")
		}
		for i := 0; i < named.NumMethods(); i++ {
			if named.Method(i).Name() == m.Name {
				return fmt.Errorf("duplicate method %s", m.Name)
			}
		}
		if field, _, _ := types.LookupFieldOrMethod(recv, true, pkg, m.Name); field != nil {
			return fmt.Errorf("method %s conflicts with an existing selector", m.Name)
		}
		for _, r := range decl.Records {
			if r.Pointer == m.Receiver && r.Model.PropBySelector(m.Name) != nil {
				return fmt.Errorf("method %s conflicts with a KVO property", m.Name)
			}
		}
		sig, err := e.goSignature(m.Signature, m.Variadic, pkg)
		if err != nil {
			return err
		}
		sig = types.NewSignatureType(types.NewVar(token.NoPos, pkg, "", recv), nil, nil, sig.Params(), sig.Results(), sig.Variadic())
		obj := types.NewFunc(token.NoPos, pkg, m.Name, sig)
		named.AddMethod(obj)
		full := evm.Signature{Params: append([]*values.Type{m.Receiver}, m.Signature.Params...), Results: m.Signature.Results}
		e.natives[obj] = nativeBinding{importKey{pkg.Path(), m.Name, m.Receiver}, &evm.ExternalFunction{Ownership: m.Ownership, Direct: m.Direct, Name: m.Name, Signature: full, Variadic: m.Variadic, Call: m.Call, DynamicTypes: m.DynamicTypes}}
	}
	return nil
}

func sameNativeLayout(a, b *values.Shape) bool {
	return equalNativeShape(a, b, make(map[[2]*values.Shape]bool))
}
func equalNativeShape(a, b *values.Shape, seen map[[2]*values.Shape]bool) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Type == b.Type && equalNativeLayout(a, b, seen)
}

func equalNativeLayout(a, b *values.Shape, seen map[[2]*values.Shape]bool) bool {
	if a.Type == nil || b.Type == nil {
		return false
	}
	pair := [2]*values.Shape{a, b}
	if seen[pair] {
		return true
	}
	seen[pair] = true
	numeric := a.Kind == values.IntKind || a.Kind == values.UintKind || a.Kind == values.FloatKind || a.Kind == values.ComplexKind
	if a.Type.Repr() != b.Type.Repr() || a.Kind != b.Kind || a.Cells != b.Cells || numeric && a.Bits != b.Bits || a.Len != b.Len || a.Variadic != b.Variadic || len(a.Fields) != len(b.Fields) || len(a.Params) != len(b.Params) || len(a.Results) != len(b.Results) || len(a.Methods) != len(b.Methods) {
		return false
	}
	if !equalNativeShape(a.Elem, b.Elem, seen) || !equalNativeShape(a.Key, b.Key, seen) {
		return false
	}
	for i := range a.Fields {
		f, g := &a.Fields[i], &b.Fields[i]
		if f.Name != g.Name || f.Offset != g.Offset || f.Embedded != g.Embedded || f.Tag != g.Tag || !equalNativeShape(f.Shape, g.Shape, seen) {
			return false
		}
	}
	for i := range a.Params {
		if !equalNativeShape(a.Params[i], b.Params[i], seen) {
			return false
		}
	}
	for i := range a.Results {
		if !equalNativeShape(a.Results[i], b.Results[i], seen) {
			return false
		}
	}
	methods := make(map[string]*values.Shape, len(b.Methods))
	for _, m := range b.Methods {
		methods[m.ID] = m.Signature
	}
	for _, m := range a.Methods {
		// A method signature describes the call; only its arguments and
		// results have independently declared native identities.
		other := methods[m.ID]
		if other == nil || m.Signature == nil || m.Signature.Kind != values.FunctionKind || !equalNativeLayout(m.Signature, other, seen) {
			return false
		}
	}
	return true
}

func (e *typeEnvironment) bindNative(t *values.Type, gt types.Type) error {
	if existing, ok := e.fromGo.At(gt).(*values.Type); ok && existing != t {
		return fmt.Errorf("Go type %s has multiple native type identities", gt)
	}
	e.toGo[t] = gt
	e.fromGo.Set(gt, t)
	return nil
}
