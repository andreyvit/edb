package evmcompiler

import (
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"go/token"
	"go/types"
	"golang.org/x/tools/go/ssa"
)

func withoutReceiver(sig *types.Signature) *types.Signature {
	return types.NewSignatureType(nil, nil, nil, sig.Params(), sig.Results(), sig.Variadic())
}
func (c *typedCompiler) dynamicType(t types.Type, pos token.Pos) uint32 {
	s, err := c.env.shape(t)
	if err != nil {
		c.fail(pos, "%v", err)
		return 0
	}
	if i, ok := c.dynamic[s]; ok {
		return i
	}
	if len(c.module.Types) == 0 {
		c.module.Types = append(c.module.Types, evm.DynamicType{}) // ID zero is the nil interface.
	}
	i := uint32(len(c.module.Types))
	c.dynamic[s] = i
	c.module.Types = append(c.module.Types, evm.DynamicType{Shape: s})
	set := c.pkg.Prog.MethodSets.MethodSet(t)
	var methods []evm.DynamicMethod
	for n := 0; n < set.Len(); n++ {
		sel := set.At(n)
		obj := sel.Obj().(*types.Func)
		// Synthetic property addresses exist only during selector lowering.
		if _, intrinsic := c.env.recordMethods[obj]; intrinsic {
			continue
		}
		fn := c.pkg.Prog.MethodValue(sel)
		target, _, ok := c.target(fn, pos)
		if !ok {
			continue
		}
		sig := withoutReceiver(obj.Type().(*types.Signature))
		vs, err := c.env.signature(sig)
		if err != nil {
			c.fail(pos, "%v", err)
			continue
		}
		methods = append(methods, evm.DynamicMethod{ID: obj.Id(), Signature: vs, Variadic: sig.Variadic(), Function: target})
	}
	c.module.Types[i].Methods = methods
	return i
}
func (f *lowerer) makeInterface(i *ssa.MakeInterface) {
	s := f.shape(i.X.Type())
	if s == nil {
		return
	}
	idx := f.c.dynamicType(i.X.Type(), i.Pos())
	f.extended(evm.ExtendedMakeInterface, s, f.regs[i.X], f.regs[i], uint64(idx), 0, nil)
}
func (f *lowerer) typeAssert(i *ssa.TypeAssert) {
	s := f.shape(i.AssertedType)
	if s != nil {
		f.extended(evm.ExtendedTypeAssert, s, f.regs[i.X], f.regs[i], 0, 0, nil)
	}
}
func (f *lowerer) invoke(call *ssa.Call) {
	idx, params, results, ok := f.invokeDescriptor(&call.Call)
	if !ok {
		return
	}
	f.outMove(0, f.regs[call.Call.Value][0], false)
	for j, arg := range call.Call.Args {
		f.outMove(params.Slots[j]+1, f.regs[arg][0], false)
	}
	f.emit(evm.WithBC(evm.OpCallVirt, 0, idx))
	for j, r := range f.regs[call] {
		f.outMove(results.Slots[j], r, true)
	}
}
func (f *lowerer) invokeDescriptor(common *ssa.CallCommon) (uint32, evm.Layout, evm.Layout, bool) {
	sig := withoutReceiver(common.Method.Type().(*types.Signature))
	shape := f.shape(sig)
	if shape == nil {
		return 0, evm.Layout{}, evm.Layout{}, false
	}
	desc := evm.Invoke{Method: values.Method{ID: common.Method.Id(), Signature: shape}, InterfaceName: displayTypeName(common.Value.Type())}
	s, err := f.c.env.signature(sig)
	if err != nil {
		f.c.fail(f.pos, "%v", err)
		return 0, evm.Layout{}, evm.Layout{}, false
	}
	result, err := evm.LayoutOf(s.Results)
	if err != nil {
		f.c.fail(f.pos, "%v", err)
		return 0, evm.Layout{}, evm.Layout{}, false
	}
	params, err := evm.LayoutOf(s.Params)
	if err != nil {
		f.c.fail(f.pos, "%v", err)
		return 0, evm.Layout{}, evm.Layout{}, false
	}
	f.outR = max(f.outR, params.RCount+1, result.RCount)
	f.outP = max(f.outP, params.PCount+1, result.PCount)
	idx, exists := f.c.invokes[desc]
	if !exists {
		idx = uint32(len(f.c.module.Invokes))
		f.c.module.Invokes = append(f.c.module.Invokes, desc)
		f.c.invokes[desc] = idx
	}
	return idx, params, result, true
}
