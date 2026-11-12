package evmcompiler

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"math"
	"slices"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/types/typeutil"
)

func genericTemplate(f *ssa.Function) bool {
	return (f.TypeParams().Len() > 0 || f.Signature.RecvTypeParams().Len() > 0) && len(f.TypeArgs()) == 0
}
func (c *typedCompiler) discoverFunctions(prog *ssa.Program, file *ast.File, info *types.Info, needsInit bool) []*ssa.Function {
	var roots []*ssa.Function
	for _, m := range c.pkg.Members {
		if f, ok := m.(*ssa.Function); ok && (f.Name() != "init" || needsInit) && !genericTemplate(f) {
			roots = append(roots, f)
		}
	}
	for _, d := range file.Decls {
		if d, ok := d.(*ast.FuncDecl); ok && d.Recv != nil {
			f := prog.FuncValue(info.Defs[d.Name].(*types.Func))
			if f != nil && !genericTemplate(f) {
				roots = append(roots, f)
			}
		}
	}
	slices.SortFunc(roots, func(a, b *ssa.Function) int { return strings.Compare(a.String(), b.String()) })
	formatting := false
	var formatCandidates []types.Type
	nextFormatCandidate := 0
	seen := map[*ssa.Function]bool{}
	var queue []*ssa.Function
	var hostTypes []types.Type
	seenHostTypes := map[*values.Type]bool{}
	add := func(f *ssa.Function) {
		if f != nil && f.Name() == "init" && f.Pkg != nil && f.Pkg != c.pkg {
			return
		}
		if f != nil && !seen[f] && len(f.Blocks) > 0 && !genericTemplate(f) {
			if f.Pkg != nil && f.Pkg.Pkg.Path() == "fmt" && c.env.standard["fmt"] != nil {
				formatting = true
			}
			seen[f] = true
			queue = append(queue, f)
		}
	}
	for _, f := range roots {
		add(f)
	}
	var addFormatted func(types.Type)
	var addHost func(*ssa.Function)
	addHost = func(fn *ssa.Function) {
		if fn == nil {
			return
		}
		obj, _ := fn.Object().(*types.Func)
		b, ok := c.env.natives[obj]
		if !ok || b.function == nil {
			return
		}
		for _, t := range b.function.DynamicTypes {
			if seenHostTypes[t] {
				continue
			}
			seenHostTypes[t] = true
			gt, err := c.env.goType(t, obj.Pkg())
			if err != nil {
				c.fail(fn.Pos(), "%v", err)
				continue
			}
			hostTypes = append(hostTypes, gt)
			formatCandidates = append(formatCandidates, gt)
			set := prog.MethodSets.MethodSet(gt)
			for i := 0; i < set.Len(); i++ {
				fn := prog.MethodValue(set.At(i))
				add(fn)
				addHost(fn)
			}
		}
	}
	var formatted typeutil.Map
	addFormatted = func(t types.Type) {
		t = types.Unalias(t)
		if formatted.At(t) != nil {
			return
		}
		formatted.Set(t, true)
		if _, ok := t.Underlying().(*types.Interface); ok {
			return
		}
		hostTypes = append(hostTypes, t)
		set := prog.MethodSets.MethodSet(t)
		for i := 0; i < set.Len(); i++ {
			fn := prog.MethodValue(set.At(i))
			add(fn)
			addHost(fn)
		}
		switch u := t.Underlying().(type) {
		case *types.Struct:
			for i := 0; i < u.NumFields(); i++ {
				addFormatted(u.Field(i).Type())
			}
		case *types.Array:
			addFormatted(u.Elem())
		case *types.Slice:
			addFormatted(u.Elem())
		case *types.Pointer:
			addFormatted(u.Elem())
		case *types.Map:
			addFormatted(u.Key())
			addFormatted(u.Elem())
		}
	}
	// Entry arguments have no producing host call from which to discover their methods.
	for _, pkg := range c.pkg.Pkg.Imports() {
		if decl := c.env.packages[pkg.Path()]; decl != nil {
			for _, t := range decl.EntryTypes {
				gt, err := c.env.goType(t, pkg)
				if err != nil {
					c.fail(token.NoPos, "%v", err)
					continue
				}
				hostTypes = append(hostTypes, gt)
				set := prog.MethodSets.MethodSet(gt)
				for i := 0; i < set.Len(); i++ {
					fn := prog.MethodValue(set.At(i))
					add(fn)
					addHost(fn)
				}
			}
		}
	}
	for n := 0; n < len(queue); n++ {
		if len(queue) > 65535 {
			c.fail(queue[n].Pos(), "too many generated functions")
			break
		}
		for _, block := range queue[n].Blocks {
			for _, ins := range block.Instrs {
				if mi, ok := ins.(*ssa.MakeInterface); ok {
					formatCandidates = append(formatCandidates, mi.X.Type())
					set := prog.MethodSets.MethodSet(mi.X.Type())
					for i := 0; i < set.Len(); i++ {
						fn := prog.MethodValue(set.At(i))
						add(fn)
						addHost(fn)
					}
				}

				if ci, ok := ins.(ssa.CallInstruction); ok && !ci.Common().IsInvoke() {
					add(ci.Common().StaticCallee())
					addHost(ci.Common().StaticCallee())
				}
				for _, p := range ins.Operands(nil) {
					if p == nil || *p == nil {
						continue
					}
					if f, ok := (*p).(*ssa.Function); ok {
						add(f)
						addHost(f)
					}
				}
			}
		}
		if formatting {
			for nextFormatCandidate < len(formatCandidates) {
				t := formatCandidates[nextFormatCandidate]
				nextFormatCandidate++
				addFormatted(t)
			}
		}
	}
	c.hostDynamic = hostTypes
	return queue
}
func (c *typedCompiler) functionName(f *ssa.Function, index int) (string, bool) {
	if f.Pkg == c.pkg && f.Parent() == nil && f.Signature.Recv() == nil && len(f.TypeArgs()) == 0 {
		return f.Name(), f.Name() == "init" || strings.HasPrefix(f.Name(), "init#")
	}
	return fmt.Sprintf("%s$%d", f.String(), index), true
}
func (c *typedCompiler) target(f *ssa.Function, pos token.Pos) (evm.FunctionRef, evm.Signature, bool) {
	if i, ok := c.functions[f]; ok {
		return evm.FunctionRef{Index: i}, c.module.Functions[i].Signature, true
	}
	obj := f.Object()
	if obj == nil || obj.Pkg() == nil {
		c.fail(pos, "unknown function %s", f)
		return evm.FunctionRef{}, evm.Signature{}, false
	}
	binding, ok := c.env.natives[obj.(*types.Func)]
	if ok {
		key, external := binding.key, binding.function
		if external == nil {
			sig, err := c.env.signature(f.Signature)
			if err != nil {
				c.fail(pos, "%v", err)
				return evm.FunctionRef{}, evm.Signature{}, false
			}
			bound, ok := evm.BindStandardFunction(key.pkg, key.name, sig)
			if !ok {
				c.fail(pos, "unknown virtual primitive %s.%s", key.pkg, key.name)
				return evm.FunctionRef{}, sig, false
			}
			external = bound
		}
		sig := evm.Signature{Params: slices.Clone(external.Signature.Params), Results: slices.Clone(external.Signature.Results)}
		idx, ok := c.imports[key]
		if !ok {
			idx = uint32(len(c.module.Imports))
			c.imports[key] = idx
			c.module.Imports = append(c.module.Imports, evm.Import{Package: key.pkg, Name: key.name, Receiver: key.receiver, Signature: sig, Ownership: external.Ownership.Clone()})
		}
		return evm.FunctionRef{Native: true, Index: idx}, sig, true
	}
	c.fail(pos, "unknown external function %s", f)
	return evm.FunctionRef{}, evm.Signature{}, false
}
func (c *typedCompiler) functionRef(fn *ssa.Function, pos token.Pos) uint32 {
	if idx, ok := c.refs[fn]; ok {
		return idx
	}
	target, _, ok := c.target(fn, pos)
	if !ok {
		return 0
	}
	idx := uint32(len(c.module.FunctionRefs))
	c.refs[fn] = idx
	c.module.FunctionRefs = append(c.module.FunctionRefs, target)
	return idx
}
func (f *lowerer) indirectSignature(sig *types.Signature) (uint32, evm.Signature, bool) {
	shape := f.shape(sig)
	if shape == nil {
		return 0, evm.Signature{}, false
	}
	if idx, ok := f.c.callSignatures[shape]; ok {
		return idx, f.c.module.Signatures[idx], true
	}
	s, err := f.c.env.signature(sig)
	if err != nil {
		f.c.fail(f.pos, "%v", err)
		return 0, s, false
	}
	i := uint32(len(f.c.module.Signatures))
	f.c.callSignatures[shape] = i
	f.c.module.Signatures = append(f.c.module.Signatures, s)
	return i, s, true
}
func (f *lowerer) makeClosure(i *ssa.MakeClosure) {
	desc := evm.Closure{Function: f.c.functionRef(i.Fn.(*ssa.Function), i.Pos())}
	for _, v := range i.Bindings {
		desc.Captures = append(desc.Captures, operand(f.regs[v][0]))
	}
	idx := uint32(len(f.c.module.Closures))
	f.c.module.Closures = append(f.c.module.Closures, desc)
	f.emit(evm.WithBC(evm.OpClosure, f.regs[i][0].slot, idx))
}

// A schema declares the register convention before it is bound to a host.
func (f *lowerer) directOpcode(call *ssa.Call) evm.Opcode {
	if !f.optimizing() {
		return 0
	}
	fn, ok := call.Call.Value.(*ssa.Function)
	if !ok {
		return 0
	}
	if _, local := f.c.functions[fn]; local {
		fn = f.forwardedHost(fn)
		if fn == nil {
			return 0
		}
	}
	obj, _ := fn.Object().(*types.Func)
	n := f.c.env.natives[obj].function
	if n == nil {
		return 0
	}
	return n.Direct.Op
}

// A void wrapper that only forwards its parameters to a direct host binding
// needs no script frame. Retain all other wrappers, including defer/recover,
// conversions, reordered arguments and value-returning calls.
func (f *lowerer) forwardedHost(fn *ssa.Function) *ssa.Function {
	if !f.optimizing() || len(fn.FreeVars) != 0 || len(fn.Blocks) != 1 || fn.Signature.Recv() != nil || fn.Signature.Results().Len() != 0 {
		return nil
	}
	body := fn.Blocks[0].Instrs
	if len(body) != 2 {
		return nil
	}
	call, ok := body[0].(*ssa.Call)
	if !ok || call.Call.IsInvoke() || len(call.Call.Args) != len(fn.Params) {
		return nil
	}
	if ret, ok := body[1].(*ssa.Return); !ok || len(ret.Results) != 0 {
		return nil
	}
	for i, param := range fn.Params {
		if call.Call.Args[i] != param {
			return nil
		}
	}
	target := call.Call.StaticCallee()
	if target == nil || target.Signature.Results().Len() != 0 {
		return nil
	}
	obj, _ := target.Object().(*types.Func)
	if n := f.c.env.natives[obj].function; n != nil && n.Direct.Op != 0 {
		return target
	}
	return nil
}

func (f *lowerer) emitCall(call *ssa.Call) {
	common := &call.Call
	if b, ok := common.Value.(*ssa.Builtin); ok {
		if b.Name() == "print" || b.Name() == "println" {
			f.printBuiltin(call, b.Name() == "println")
			return
		}
		if b.Name() == "ssa:deferstack" {
			f.emit(evm.ABC(evm.OpDeferStack, f.regs[call][0].slot, 0, 0))
			return
		}
		if b.Name() == "recover" {
			f.emit(evm.ABC(evm.OpRecover, f.regs[call][0].slot, 0, 0))
			return
		}

		if b.Name() == "ssa:wrapnilchk" {
			arg := f.regs[common.Args[0]][0]
			f.emit(evm.ABC(evm.OpCheckNil, arg.slot, 0, 0))
			f.move(f.regs[call][0], arg)
			return
		}
		if f.builtin(call, b.Name()) {
			return
		}
		f.c.fail(call.Pos(), "builtin %s is not implemented yet", b.Name())
		return
	}
	if common.IsInvoke() {
		f.invoke(call)
		return
	}
	var ref evm.FunctionRef
	var sig evm.Signature
	var index uint32
	var opcode evm.Opcode
	var a uint16
	var method bool
	if fn, ok := common.Value.(*ssa.Function); ok {
		if fn.Name() == "init" && fn.Pkg != nil && fn.Pkg != f.c.pkg {
			return
		}
		if target := f.forwardedHost(fn); target != nil {
			fn = target
		}
		var ok bool
		ref, sig, ok = f.c.target(fn, call.Pos())
		if !ok {
			return
		}
		if f.inlineAt[call] {
			f.inlineLeaf(fn, call)
			return
		}
		method = fn.Signature.Recv() != nil
		index = ref.Index
		opcode = evm.OpCallFunc
		if ref.Native {
			opcode = evm.OpCallHost
		}
	} else {
		var ok bool
		index, sig, ok = f.indirectSignature(common.Signature())
		if !ok {
			return
		}
		opcode = evm.OpCallIndirect
		a = f.regs[common.Value][0].slot
	}
	params, err := evm.ParameterLayout(sig.Params, method)
	if err != nil {
		f.c.fail(call.Pos(), "%v", err)
		return
	}
	results, err := evm.LayoutOf(sig.Results)
	if err != nil {
		f.c.fail(call.Pos(), "%v", err)
		return
	}
	if len(params.Slots) != len(common.Args) {
		f.c.fail(call.Pos(), "call argument layout mismatch")
		return
	}
	if opcode == evm.OpCallHost && index <= math.MaxUint16 {
		if op := f.directOpcode(call); op != 0 {
			var a uint16
			if len(sig.Results) != 0 {
				a = f.regs[call][0].slot
			}
			var b uint16
			if j, tied := op.TiedHostArgument(); tied {
				src := f.regs[common.Args[j]][0]
				if len(sig.Results) == 0 {
					a = src.slot
				} else if src.slot != a {
					f.move(vreg{a, src.typ}, src)
				}
				b = f.regs[common.Args[1-j]][0].slot
			} else if len(common.Args) > 0 {
				b = f.regs[common.Args[0]][0].slot
			}
			f.emit(evm.ABC(op, a, b, uint16(index)))
			return
		}
	}
	f.outR = max(f.outR, params.RCount, results.RCount)
	f.outP = max(f.outP, params.PCount, results.PCount)
	var fused evm.Opcode
	if f.optimizing() && opcode == evm.OpCallHost && index <= math.MaxUint16 && len(*call.Referrers()) != 0 {
		fused = evm.UnaryHostOpcode(sig)
	}
	if fused == 0 {
		for j, arg := range common.Args {
			if f.optimizing() && j == 0 && f.forwardWordArgument(call, arg, params.Slots[j]) {
				continue
			}
			f.outMove(params.Slots[j], f.regs[arg][0], false)
		}
	}
	if fused != 0 {
		f.emit(evm.ABC(fused, f.regs[call][0].slot, f.regs[common.Args[0]][0].slot, uint16(index)))
		return
	}
	f.emit(evm.WithBC(opcode, a, index))
	// Calls always run, even when the caller ignores some or all results.
	uses := *call.Referrers()
	if len(uses) == 0 {
		return
	}
	var used []bool
	if len(results.Slots) > 1 {
		used = make([]bool, len(results.Slots))
		for _, use := range uses {
			e, ok := use.(*ssa.Extract)
			if !ok {
				used = nil
				break
			}
			used[e.Index] = len(*e.Referrers()) != 0 || used[e.Index]
		}
	}
	for j, r := range f.regs[call] {
		if used == nil || used[j] {
			f.outMove(results.Slots[j], r, true)
		}
	}
}
