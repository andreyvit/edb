package evmcompiler

import (
	"fmt"
	"go/constant"
	"go/token"
	"go/types"
	"math"
	"path"
	"slices"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/types/typeutil"
)

var goBasicValues = map[types.BasicKind]*values.Type{
	types.Bool: values.Boolean, types.Int: values.IntType, types.Int8: values.Int8, types.Int16: values.Int16, types.Int32: values.Int32, types.Int64: values.Int64,
	types.Uint: values.UintType, types.Uint8: values.Uint8, types.Uint16: values.Uint16, types.Uint32: values.Uint32, types.Uint64: values.Uint64, types.Uintptr: values.UintptrType,
	types.Float32: values.Float32Type, types.Float64: values.Float64, types.String: values.StringType,
}

type typeEnvironment struct {
	nativeLayouts    map[*values.Type]*values.NativeLayout
	recordOperations map[string]string // internal identifier to public type selector
	recordFacade     bool
	guest            *types.Package
	recordFields     map[*types.Var]recordProperty
	recordMethods    map[*types.Func]recordProperty
	files            *token.FileSet
	standard         map[string]*standardPackage
	shapeOrder       []*values.Shape
	buildingShapes   map[*values.Shape]bool
	recordOwners     map[*values.Type]string
	nativeTypes      map[*values.Type]nativeType
	natives          map[*types.Func]nativeBinding
	resolving        map[*values.Type]bool
	packages         map[string]*evm.Package
	imported         map[string]*types.Package
	toGo             map[*values.Type]types.Type
	fromGo           typeutil.Map
	shapes           typeutil.Map
}

func newTypeEnvironment(packages []evm.Package) (*typeEnvironment, error) {
	packages = append([]evm.Package{evm.TimePackage()}, packages...)
	if err := evm.ValidateDirectBindings(packages); err != nil {
		return nil, err
	}
	e := &typeEnvironment{nativeLayouts: map[*values.Type]*values.NativeLayout{}, recordOwners: make(map[*values.Type]string), recordMethods: make(map[*types.Func]recordProperty), recordFields: make(map[*types.Var]recordProperty), standard: make(map[string]*standardPackage), buildingShapes: make(map[*values.Shape]bool), nativeTypes: make(map[*values.Type]nativeType), natives: make(map[*types.Func]nativeBinding), resolving: make(map[*values.Type]bool), packages: make(map[string]*evm.Package), imported: make(map[string]*types.Package), toGo: make(map[*values.Type]types.Type)}
	for k, v := range goBasicValues {
		e.toGo[v] = types.Typ[k]
		e.fromGo.Set(types.Typ[k], v)
	}
	e.toGo[values.AnyType] = types.NewInterfaceType(nil, nil).Complete()
	e.fromGo.Set(e.toGo[values.AnyType], values.AnyType)
	e.toGo[values.ErrorType] = types.Universe.Lookup("error").Type()
	e.fromGo.Set(e.toGo[values.ErrorType], values.ErrorType)
	for i := range packages {
		p := &packages[i]
		if _, reserved := evm.StandardLibrarySource(p.Path); reserved || p.Path == "evm/debug" {
			return nil, fmt.Errorf("reserved virtual package %q", p.Path)
		}
		if p.Path == "" {
			return nil, fmt.Errorf("empty external package path")
		}
		if _, ok := e.packages[p.Path]; ok {
			return nil, fmt.Errorf("duplicate package %q", p.Path)
		}
		for i := range p.Functions {
			f := &p.Functions[i]
			if _, err := evm.LayoutOf(f.Signature.Params); err != nil {
				return nil, fmt.Errorf("%s.%s parameters: %w", p.Path, f.Name, err)
			}
			if _, err := evm.LayoutOf(f.Signature.Results); err != nil {
				return nil, fmt.Errorf("%s.%s results: %w", p.Path, f.Name, err)
			}
		}
		for _, r := range p.Records {
			if r == nil {
				return nil, fmt.Errorf("nil record declaration")
			}
			if _, ok := e.recordOwners[r.Pointer]; ok {
				return nil, fmt.Errorf("duplicate record pointer declaration")
			}
			e.recordOwners[r.Pointer] = p.Path
		}
		for _, d := range p.Types {
			if d.Shape == nil || d.Shape.Type == nil {
				return nil, fmt.Errorf("missing native type shape")
			}
			if err := evm.ValidateShape(d.Shape); err != nil {
				return nil, err
			}
			t := d.Shape.Type
			if old, exists := e.nativeTypes[t]; exists {
				if d.Name == "" && old.decl.Name == "" && sameNativeLayout(old.decl.Shape, d.Shape) {
					continue
				}
				return nil, fmt.Errorf("duplicate native type")
			}
			if e.toGo[t] != nil {
				return nil, fmt.Errorf("cannot redefine builtin type")
			}
			e.nativeTypes[t] = nativeType{p.Path, d}
		}
		var collect func(*values.Shape)
		seen := map[*values.Shape]bool{}
		collect = func(s *values.Shape) {
			if s == nil || seen[s] {
				return
			}
			seen[s] = true
			if s.Native != nil {
				e.nativeLayouts[s.Type] = s.Native
			}
			for _, f := range s.Fields {
				collect(f.Shape)
			}
			collect(s.Elem)
			collect(s.Key)
		}
		for _, d := range p.Types {
			collect(d.Shape)
		}
		e.packages[p.Path] = p
	}
	return e, nil
}
func (e *typeEnvironment) Import(p string) (*types.Package, error) {
	if pkg := e.imported[p]; pkg != nil {
		return pkg, nil
	}
	decl, ok := e.packages[p]
	if !ok {
		if source, exists := evm.StandardLibrarySource(p); exists {
			return e.importStandard(p, source)
		}
		return nil, fmt.Errorf("package %q is not exposed", p)
	}
	if decl.Source != "" {
		pkg, err := e.importStandard(p, decl.Source)
		if err != nil {
			return nil, err
		}
		for _, shared := range decl.SharedTypes {
			obj, ok := pkg.Scope().Lookup(shared.Name).(*types.TypeName)
			if !ok || shared.Native == nil || shared.Type == nil {
				return nil, fmt.Errorf("invalid shared type %s.%s", p, shared.Name)
			}
			basic, ok := obj.Type().Underlying().(*types.Basic)
			if !ok || basic.Name() != shared.Native.Kind().String() {
				return nil, fmt.Errorf("shared type %s.%s must match native scalar %v", p, shared.Name, shared.Native)
			}
			e.toGo[shared.Type] = obj.Type()
			e.fromGo.Set(obj.Type(), shared.Type)
		}
		return pkg, nil
	}
	name := path.Base(p)
	if !token.IsIdentifier(name) {
		return nil, fmt.Errorf("invalid package name %q", name)
	}
	pkg := types.NewPackage(p, name)
	e.imported[p] = pkg
	insert := func(obj types.Object) error {
		if !token.IsIdentifier(obj.Name()) || obj.Name() == "_" {
			return fmt.Errorf("invalid export name %q", obj.Name())
		}
		if pkg.Scope().Insert(obj) != nil {
			return fmt.Errorf("duplicate export %s.%s", p, obj.Name())
		}
		return nil
	}
	if err := e.declareRecords(pkg, decl.Records); err != nil {
		return nil, err
	}
	for _, d := range decl.Types {
		if d.Name != "" {
			obj := types.NewTypeName(token.NoPos, pkg, d.Name, nil)
			named := types.NewNamed(obj, nil, nil)
			if err := insert(obj); err != nil {
				return nil, err
			}
			e.toGo[d.Shape.Type] = named
			e.fromGo.Set(named, d.Shape.Type)
		}
	}
	for _, d := range decl.Types {
		t, err := e.goShape(d.Shape, pkg)
		if err != nil {
			return nil, err
		}
		if d.Name != "" {
			if d.ScalarBase != nil {
				base := e.toGo[d.ScalarBase]
				if _, ok := base.(*types.Basic); !ok {
					return nil, fmt.Errorf("invalid scalar base for %s", d.Name)
				}
				baseShape, err := e.shape(base)
				if err != nil || baseShape.Kind != d.Shape.Kind || baseShape.Bits != d.Shape.Bits && (baseShape.Kind == values.IntKind || baseShape.Kind == values.UintKind || baseShape.Kind == values.FloatKind) {
					return nil, fmt.Errorf("incompatible scalar base for %s", d.Name)
				}
				t = base
			}
			e.toGo[d.Shape.Type].(*types.Named).SetUnderlying(t.Underlying())
		} else {
			if err := e.bindNative(d.Shape.Type, t); err != nil {
				return nil, err
			}
		}
	}
	for _, c := range decl.Constants {
		if c.Untyped != nil {
			k := map[constant.Kind]types.BasicKind{constant.Bool: types.UntypedBool, constant.String: types.UntypedString, constant.Int: types.UntypedInt, constant.Float: types.UntypedFloat, constant.Complex: types.UntypedComplex}[c.Untyped.Kind()]
			if c.Rune && k == types.UntypedInt {
				k = types.UntypedRune
			}
			if k == types.Invalid {
				return nil, fmt.Errorf("invalid untyped constant %s", c.Name)
			}
			if err := insert(types.NewConst(token.NoPos, pkg, c.Name, types.Typ[k], c.Untyped)); err != nil {
				return nil, err
			}
			continue
		}
		t, err := e.goType(c.Type, pkg)
		if err != nil {
			return nil, err
		}
		var value constant.Value
		switch b := t.Underlying().(type) {
		case *types.Basic:
			switch {
			case b.Info()&types.IsBoolean != 0:
				value = constant.MakeBool(c.Value.R != 0)
			case b.Info()&types.IsString != 0:
				value = constant.MakeString(c.Value.AsString())
			case b.Info()&types.IsFloat != 0:
				x := c.Value.AsFloat()
				if b.Kind() == types.Float32 {
					x = float64(math.Float32frombits(uint32(c.Value.R)))
				}
				if math.IsNaN(x) || math.IsInf(x, 0) {
					return nil, fmt.Errorf("non-finite constant %s", c.Name)
				}
				value = constant.MakeFloat64(x)
			case b.Info()&types.IsUnsigned != 0:
				value = constant.MakeUint64(c.Value.R)
			case b.Info()&types.IsInteger != 0:
				value = constant.MakeInt64(c.Value.AsInt())
			}
		}
		if value == nil {
			return nil, fmt.Errorf("unsupported constant %s", c.Name)
		}
		if err := insert(types.NewConst(token.NoPos, pkg, c.Name, t, value)); err != nil {
			return nil, err
		}
	}
	if err := e.addNativeMethods(pkg, decl); err != nil {
		return nil, err
	}
	for i := range decl.Functions {
		f := &decl.Functions[i]
		sig, err := e.goSignature(f.Signature, f.Variadic, pkg)
		if err != nil {
			return nil, err
		}
		if len(f.ParamNames) != 0 {
			if len(f.ParamNames) != sig.Params().Len() {
				return nil, fmt.Errorf("parameter name count for %s", f.Name)
			}
			vars := make([]*types.Var, len(f.ParamNames))
			for i, name := range f.ParamNames {
				vars[i] = types.NewVar(token.NoPos, pkg, name, sig.Params().At(i).Type())
			}
			sig = types.NewSignatureType(nil, nil, nil, types.NewTuple(vars...), sig.Results(), sig.Variadic())
		}
		name := f.Name
		if f.On != nil {
			if !token.IsIdentifier(name) || !token.IsExported(name) || !slices.Contains(decl.Records, f.On) {
				return nil, fmt.Errorf("invalid record operation %s", f.LinkName())
			}
			name = operationIdent(p, f)
			if e.recordOperations == nil {
				e.recordOperations = make(map[string]string)
			}
			e.recordOperations[name] = f.LinkName()
		}
		obj := types.NewFunc(token.NoPos, pkg, name, sig)
		if f.On == nil {
			if err := insert(obj); err != nil {
				return nil, err
			}
		} else if pkg.Scope().Insert(obj) != nil {
			return nil, fmt.Errorf("duplicate record operation %s", f.LinkName())
		}
		e.natives[obj] = nativeBinding{importKey{p, f.LinkName(), nil}, f}
	}

	for _, d := range decl.Types {
		actual, err := e.shape(e.toGo[d.Shape.Type])
		if err != nil {
			return nil, err
		}
		if !sameNativeLayout(d.Shape, actual) {
			return nil, fmt.Errorf("native storage declaration differs from Go layout for %s", d.Shape.Type.Name())
		}
	}
	pkg.MarkComplete()
	return pkg, nil
}
func (e *typeEnvironment) goTuple(ts []*values.Type, pkg *types.Package) (*types.Tuple, error) {
	vars := make([]*types.Var, len(ts))
	for i, t := range ts {
		gt, err := e.goType(t, pkg)
		if err != nil {
			return nil, err
		}
		vars[i] = types.NewVar(token.NoPos, pkg, "", gt)
	}
	return types.NewTuple(vars...), nil
}
func (e *typeEnvironment) goType(t *values.Type, pkg *types.Package) (types.Type, error) {
	if gt := e.toGo[t]; gt != nil {
		return gt, nil
	}
	if t == nil {
		return nil, fmt.Errorf("nil external type")
	}
	for path, p := range e.packages {
		for _, shared := range p.SharedTypes {
			if shared.Type == t {
				if _, err := e.Import(path); err != nil {
					return nil, err
				}
				return e.toGo[t], nil
			}
		}
	}
	if owner, ok := e.recordOwners[t]; ok {
		if _, err := e.Import(owner); err != nil {
			return nil, err
		}
		if gt := e.toGo[t]; gt != nil {
			return gt, nil
		}
		return nil, fmt.Errorf("unresolved record type %s", t.Name())
	}
	if d, ok := e.nativeTypes[t]; ok {
		if e.resolving[t] {
			return nil, fmt.Errorf("recursive unnamed native type")
		}
		e.resolving[t] = true
		defer delete(e.resolving, t)
		owner, err := e.Import(d.pkg)
		if err != nil {
			return nil, err
		}
		if gt := e.toGo[t]; gt != nil {
			return gt, nil
		}
		gt, err := e.goShape(d.decl.Shape, owner)
		if err != nil {
			return nil, err
		}
		if err := e.bindNative(t, gt); err != nil {
			return nil, err
		}
		return gt, nil
	}
	if t.Repr() != values.Raw {
		return nil, fmt.Errorf("external type %s needs an explicit definition", t.Name())
	}
	name := t.Name()
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	if !token.IsIdentifier(name) {
		return nil, fmt.Errorf("invalid type name %s", name)
	}
	obj := types.NewTypeName(token.NoPos, pkg, name, nil)
	gt := types.NewNamed(obj, types.Typ[types.Int64], nil)
	if pkg.Scope().Insert(obj) != nil {
		return nil, fmt.Errorf("duplicate type name %s", name)
	}
	e.toGo[t] = gt
	e.fromGo.Set(gt, t)
	return gt, nil
}
func (e *typeEnvironment) valueType(t types.Type) (*values.Type, error) {
	s, err := e.shape(t)
	if err != nil {
		return nil, err
	}
	return s.Type, nil
}
func (e *typeEnvironment) signature(sig *types.Signature) (evm.Signature, error) {
	var result evm.Signature
	if sig.Recv() != nil {
		t, err := e.valueType(sig.Recv().Type())
		if err != nil {
			return result, err
		}
		result.Params = append(result.Params, t)
	}
	for i := 0; i < sig.Params().Len(); i++ {
		t, err := e.valueType(sig.Params().At(i).Type())
		if err != nil {
			return result, err
		}
		result.Params = append(result.Params, t)
	}
	for i := 0; i < sig.Results().Len(); i++ {
		t, err := e.valueType(sig.Results().At(i).Type())
		if err != nil {
			return result, err
		}
		result.Results = append(result.Results, t)
	}
	return result, nil
}
func numericType(t types.Type) (evm.NumericType, bool) {
	b, ok := t.Underlying().(*types.Basic)
	if !ok {
		return evm.NumericType{}, false
	}
	n := evm.NumericType{Bits: 64}
	switch b.Kind() {
	case types.Int8, types.Uint8:
		n.Bits = 8
	case types.Int16, types.Uint16:
		n.Bits = 16
	case types.Int32, types.Uint32, types.Float32:
		n.Bits = 32
	}
	switch {
	case b.Info()&types.IsFloat != 0:
		n.Kind = evm.NumericFloat
	case b.Info()&types.IsUnsigned != 0:
		n.Kind = evm.NumericUnsigned
	case b.Info()&types.IsInteger != 0:
		n.Kind = evm.NumericSigned
	default:
		return n, false
	}
	return n, true
}
