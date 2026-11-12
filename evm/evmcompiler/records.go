package evmcompiler

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"reflect"
	"strconv"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/ssa"
)

type recordProperty struct {
	owner *evm.RecordType
	prop  kvo.PropImpl
}

func (p recordProperty) method() string { return fmt.Sprintf("Evm$key%d", p.prop.Code()) }

// The public type check sees KVO properties. SSA sees opaque handles and
// typed address intrinsics. Go itself schedules assignment evaluation,
// compound assignments and increments before the intrinsics are lowered.
func (e *typeEnvironment) declareRecords(pkg *types.Package, records []*evm.RecordType) error {
	type identity struct {
		model   *kvo.Model
		mutable bool
	}
	byModel := map[identity]*evm.RecordType{}
	names := map[*evm.RecordType]*types.Named{}
	for _, r := range records {
		if r == nil || r.Model == nil || r.Pointer == nil || r.Pointer.Repr() != recordRepr(r) || !token.IsExported(r.Name) || !token.IsIdentifier(r.Name) {
			return fmt.Errorf("invalid KVO record declaration")
		}
		key := identity{r.Model, r.Mutable}
		if byModel[key] != nil {
			return fmt.Errorf("duplicate KVO model %s", r.Name)
		}
		byModel[key] = r
		obj := types.NewTypeName(token.NoPos, pkg, r.Name, nil)
		if pkg.Scope().Insert(obj) != nil {
			return fmt.Errorf("duplicate type %s", r.Name)
		}
		named := types.NewNamed(obj, nil, nil)
		names[r] = named
		if err := e.bindNative(r.Pointer, types.NewPointer(named)); err != nil {
			return err
		}
	}
	for _, r := range records {
		named := names[r]
		sealType := types.Type(types.NewPointer(types.Typ[types.Uint8]))
		if !r.Mutable {
			sealType = types.NewInterfaceType(nil, nil).Complete()
		}
		seal := types.NewField(token.NoPos, pkg, "seal"+r.Name, sealType, false)
		fields := []*types.Var{seal}
		selectors := map[string]bool{seal.Name(): true}
		var fieldType func(kvo.AnyType) types.Type
		fieldType = func(k kvo.AnyType) types.Type {
			if child := k.Model(); child != nil {
				if decl := byModel[identity{child, r.Mutable}]; decl != nil {
					return e.toGo[decl.Pointer]
				}
				return nil
			}
			for path, pkg := range e.packages {
				for _, shared := range pkg.SharedTypes {
					if native, ok := k.(interface{ NativeType() reflect.Type }); ok && shared.Native == native.NativeType() {
						if _, err := e.Import(path); err != nil {
							return nil
						}
						return e.toGo[shared.Type]
					}
				}
			}
			if scalar := evm.RecordScalarType(k); scalar != nil {
				t, _ := e.goType(scalar, pkg)
				return t
			}
			switch k := k.(type) {
			case *kvo.ScalarDataType:
				if k.NativeType().Kind() == reflect.Slice {
					return types.NewSlice(types.Typ[types.Uint8])
				}
			case *kvo.MapType:
				a, b := fieldType(k.MapKeyType()), fieldType(k.ItemType())
				if a != nil && b != nil {
					return types.NewMap(a, b)
				}
			case *kvo.ListType:
				if elem := fieldType(k.ItemType()); elem != nil {
					return types.NewSlice(elem)
				}
			}
			return nil
		}
		selectorTypes := make(map[kvo.PropCode]types.Type)
		for _, prop := range r.Model.AllProps() {
			t := fieldType(prop.AnyType())
			if t == nil {
				if r.SchemaView {
					continue
				}
				return fmt.Errorf("%s.%s: unsupported KVO property type %s", r.Name, prop.Name(), prop.AnyType().Name())
			}
			if prop.Code() >= evm.VMFieldBase || !token.IsIdentifier(prop.Name()) || prop.Name() == "_" || prop.Name() == seal.Name() {
				return fmt.Errorf("invalid KVO property %s.%s", r.Name, prop.Name())
			}
			property := recordProperty{r, prop}
			if e.recordFacade {
				selectorTypes[prop.Code()] = t
			} else {
				recv := types.NewVar(token.NoPos, pkg, "", e.toGo[r.Pointer])
				res := types.NewVar(token.NoPos, pkg, "", types.NewPointer(t))
				sig := types.NewSignatureType(recv, nil, nil, nil, types.NewTuple(res), false)
				fn := types.NewFunc(token.NoPos, pkg, property.method(), sig)
				named.AddMethod(fn)
				e.recordMethods[fn] = property
			}
		}
		if e.recordFacade {
			for name, prop := range r.Model.Selectors() {
				t := selectorTypes[prop.Code()]
				if t == nil { // Unsupported properties are omitted from schema views.
					continue
				}
				if selectors[name] {
					return fmt.Errorf("conflicting KVO selector %s.%s", r.Name, name)
				}
				selectors[name] = true
				field := types.NewField(token.NoPos, e.guest, name, t, false)
				fields = append(fields, field)
				e.recordFields[field] = recordProperty{r, prop}
			}
		}
		named.SetUnderlying(types.NewStruct(fields, nil))
	}
	return nil
}
func hasJSON(t kvo.AnyType) bool {
	switch t := t.(type) {
	case *kvo.JSONType:
		return true
	case *kvo.ListType:
		return hasJSON(t.ItemType())
	case *kvo.MapType:
		return hasJSON(t.ItemType())
	}
	return false
}
func (c *typedCompiler) registerJSONTypes(pos token.Pos) {
	if c.module.JSONTypes.Object != 0 {
		return
	}
	anyT := c.env.toGo[values.AnyType]
	id := func(t types.Type) uint32 { return c.dynamicType(t, pos) }
	c.module.JSONTypes = evm.JSONTypes{Boolean: id(types.Typ[types.Bool]), Integer: id(types.Typ[types.Int64]), Unsigned: id(types.Typ[types.Uint64]), Float: id(types.Typ[types.Float64]), String: id(types.Typ[types.String]), Array: id(types.NewSlice(anyT)), Object: id(types.NewMap(types.Typ[types.String], anyT))}
}

func rewriteRecordFields(file *ast.File, info *types.Info, fields map[*types.Var]recordProperty) {
	astutil.Apply(file, nil, func(c *astutil.Cursor) bool {
		s, ok := c.Node().(*ast.SelectorExpr)
		if !ok {
			return true
		}
		sel := info.Selections[s]
		if sel == nil {
			return true
		}
		field, _ := sel.Obj().(*types.Var)
		prop, ok := fields[field]
		if !ok {
			return true
		}
		s.Sel = &ast.Ident{NamePos: s.Sel.Pos(), Name: prop.method()}
		c.Replace(&ast.StarExpr{Star: s.Pos(), X: &ast.CallExpr{Fun: s}})
		return true
	})
}
func (f *lowerer) recordRef(v ssa.Value) (*ssa.Call, recordProperty, bool) {
	call, ok := v.(*ssa.Call)
	if !ok {
		return nil, recordProperty{}, false
	}
	fn := call.Call.StaticCallee()
	if fn == nil {
		return nil, recordProperty{}, false
	}
	obj, _ := fn.Object().(*types.Func)
	p, ok := f.c.env.recordMethods[obj]
	return call, p, ok
}
func (f *lowerer) recordInstruction(ins ssa.Instruction) bool {
	var address ssa.Value
	var in, out []vreg
	var typ types.Type
	kind := evm.ExtendedGetKey
	switch i := ins.(type) {
	case *ssa.Call:
		_, _, ok := f.recordRef(i)
		if !ok {
			return false
		}
		for _, use := range *i.Referrers() {
			switch u := use.(type) {
			case *ssa.UnOp:
				if u.Op == token.MUL {
					continue
				}
			case *ssa.Store:
				if u.Addr == i && u.Val != i {
					continue
				}
			}
			f.c.fail(use.Pos(), "cannot take the address of a KVO property")
		}
		return true
	case *ssa.Store:
		address = i.Addr
		kind = evm.ExtendedSetKey
		typ = i.Val.Type()
	case *ssa.UnOp:
		if i.Op != token.MUL {
			return false
		}
		address = i.X
		typ = i.Type()
		out = f.regs[i]
	default:
		return false
	}
	ref, p, ok := f.recordRef(address)
	if !ok {
		return false
	}
	in = []vreg{f.regs[ref.Call.Args[0]][0]}
	if v, ok := ins.(ssa.Value); ok {
		if _, ok := f.recordLists[v]; ok {
			if f.directRecordList(v) == nil {
				f.recordListOp(evm.ExtendedRecordList, v, in, out)
			}
			return true
		}
	}
	if store, ok := ins.(*ssa.Store); ok {
		if !p.owner.Mutable || p.prop.TypeModel() != nil {
			f.c.fail(store.Pos(), "cannot assign %s.%s; write scalar properties of a mutable record", p.owner.Name, p.prop.Name())
			return true
		}
		in = append(in, f.regs[store.Val][0])
	}
	if f.optimizing() && kind == evm.ExtendedGetKey && !p.owner.Mutable && evm.RecordWordType(p.prop) != nil && ins.Block() == ref.Block() && f.foldChildWord(ref, p, typ, out[0]) {
		return true
	}
	if p.owner.Mutable && p.prop.TypeModel() != nil {
		f.markLocalRead(ins.(ssa.Value))
	}
	if hasJSON(p.prop.AnyType()) {
		f.c.registerJSONTypes(ins.Pos())
	}
	f.extended(kind, f.shape(typ), in, out, p.prop.Code(), 0, nil)
	ops := f.c.module.ExtendedOps
	ops[len(ops)-1].Record = p.owner
	return true
}

// Fuse adjacent reads only when the intermediate handle has exactly one use.
// The fused word keeps the position of the child read.
func (f *lowerer) foldChildWord(ref *ssa.Call, leaf recordProperty, typ types.Type, dst vreg) bool {
	child, ok := ref.Call.Args[0].(*ssa.UnOp)
	if !ok || child.Op != token.MUL || child.Block() != ref.Block() || len(*child.Referrers()) != 1 || len(*ref.Referrers()) != 1 {
		return false
	}
	pc := len(f.code.Code) - 1
	if pc < 0 || f.code.Code[pc].Op() != evm.OpExtended {
		return false
	}
	op := &f.c.module.ExtendedOps[f.code.Code[pc].BC()]
	if op.Kind != evm.ExtendedGetKey || op.Record.Mutable || op.Shape.Kind != values.TypedPairKind || op.Out[0] != operand(f.regs[child][0]) {
		return false
	}
	op.Kind, op.ChildKey = evm.ExtendedGetChildKey, leaf.prop.Code()
	op.Source, op.Shape, op.Out[0] = op.Shape, f.shape(typ), operand(dst)
	delete(f.code.LocalRecordReads, pc)
	return true
}

func (f *lowerer) isReadRecord(t *values.Type) bool {
	for _, p := range f.c.env.packages {
		for _, r := range p.Records {
			if r.Pointer == t {
				return !r.Mutable
			}
		}
	}
	return false
}

// A nonescaping row may occupy one slot per producing instruction and call
// depth. Every observer must finish before that instruction runs again.
func (f *lowerer) markLocalRead(v ssa.Value) {
	if !f.optimizing() || !f.readUsesLocal(v) {
		return
	}
	if f.code.LocalRecordReads == nil {
		f.code.LocalRecordReads = make(map[int]bool)
	}
	f.code.LocalRecordReads[len(f.code.Code)] = true
}

func (f *lowerer) readUsesLocal(v ssa.Value) bool {
	uses := *v.Referrers()
	// The common "for c := Next(); c != nil; c = Next()" joins its two
	// producers at the loop header. Only forward a final result directly
	// through the jump: running user code before the jump could observe the
	// previous iteration's row after its slot had already been overwritten.
	if len(uses) == 1 {
		if phi, ok := uses[0].(*ssa.Phi); ok {
			block := v.(ssa.Instruction).Block()
			code := block.Instrs
			if len(code) < 2 || code[len(code)-2] != v.(ssa.Instruction) || len(block.Succs) != 1 || block.Succs[0] != phi.Block() {
				return false
			}
			uses = *phi.Referrers()
			v = phi
		}
	}
	for _, use := range uses {
		switch u := use.(type) {
		case *ssa.Call:
			// A mutable grandchild can retain this handle as its parent.
			if _, p, ok := f.recordRef(u); ok && u.Call.Args[0] == v && (!p.owner.Mutable || p.prop.TypeModel() == nil) {
				continue
			}
		case *ssa.BinOp:
			if u.Op == token.EQL || u.Op == token.NEQ {
				other := u.X
				if other == v {
					other = u.Y
				}
				if c, ok := other.(*ssa.Const); ok && c.IsNil() {
					continue
				}
			}
		}
		return false
	}
	return true
}

// The private identifier cannot be spelled in guest source. The public
// spelling names a type, so ordinary selectors on variables stay untouched.
func operationIdent(path string, f *evm.ExternalFunction) string {
	return fmt.Sprintf("Evm$%x$%s$%s", path, f.On.Name, f.Name)
}

func (e *typeEnvironment) rewriteRecordOperations(file *ast.File, info *types.Info, aliases map[string]string) bool {
	if info == nil {
		return false
	}
	changed := false
	astutil.Apply(file, nil, func(c *astutil.Cursor) bool {
		sel, ok := c.Node().(*ast.SelectorExpr)
		if !ok {
			return true
		}
		tv := info.Types[sel.X]
		if !tv.IsType() {
			return true
		}
		named, ok := types.Unalias(tv.Type).(*types.Named)
		if !ok || named.Obj().Pkg() == nil {
			return true
		}
		path := named.Obj().Pkg().Path()
		pkg := e.packages[path]
		if pkg == nil {
			return true
		}
		for i := range pkg.Functions {
			f := &pkg.Functions[i]
			if f.On == nil || f.On.Name != named.Obj().Name() || f.Name != sel.Sel.Name {
				continue
			}
			name := operationIdent(path, f)
			ident := &ast.Ident{NamePos: sel.Sel.Pos(), Name: name}
			if t, ok := sel.X.(*ast.SelectorExpr); ok {
				if id, ok := t.X.(*ast.Ident); ok {
					if owner, ok := info.Uses[id].(*types.PkgName); ok && owner.Imported().Path() == path {
						sel.X = id
						sel.Sel = ident
						changed = true
						return true
					}
				}
			}
			dotImported := false
			for _, imp := range file.Imports {
				importPath, _ := strconv.Unquote(imp.Path.Value)
				if importPath == path && imp.Name != nil && imp.Name.Name == "." {
					dotImported = true
					break
				}
			}
			if !dotImported {
				aliases[name] = path
			}
			c.Replace(ident)
			changed = true
			break
		}
		return true
	})
	return changed
}

func recordRepr(r *evm.RecordType) values.Repr {
	if r.Mutable {
		return values.Pointer
	}
	return values.Pair
}
func (e *typeEnvironment) isReadRecord(t *values.Type) bool {
	for _, p := range e.packages {
		for _, r := range p.Records {
			if r.Pointer == t {
				return !r.Mutable
			}
		}
	}
	return false
}
