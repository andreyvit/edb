package evmcompiler

import (
	"go/token"
	"go/types"

	"golang.org/x/tools/go/ssa"
)

// Permit only straight-line, total operations on scalar inputs. In particular,
// never inline a memory read, a trapping integer divide or a nested call.
func scalarLeaf(fn *ssa.Function) bool {
	if len(fn.Blocks) != 1 || len(fn.FreeVars) != 0 || fn.Signature.Recv() != nil || fn.Signature.Results().Len() != 1 {
		return false
	}
	body := fn.Blocks[0].Instrs
	if len(body) > 6 {
		return false
	}
	scalar := func(t types.Type) bool {
		b, ok := t.Underlying().(*types.Basic)
		return ok && b.Info()&(types.IsInteger|types.IsFloat|types.IsBoolean) != 0
	}
	if !scalar(fn.Signature.Results().At(0).Type()) {
		return false
	}
	for _, p := range fn.Params {
		if !scalar(p.Type()) {
			return false
		}
	}
	for _, ins := range body {
		switch i := ins.(type) {
		case *ssa.Return:
		case *ssa.BinOp:
			if !scalar(i.X.Type()) {
				return false
			}
			switch i.Op {
			case token.ADD, token.SUB, token.MUL, token.AND, token.OR, token.XOR, token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
			default:
				return false
			}
		case *ssa.Convert:
			if !scalar(i.X.Type()) || !scalar(i.Type()) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// Choose sites before emitting the prologue so inlined literals load once.
func (f *lowerer) planInline(call *ssa.Call) {
	fn, ok := call.Call.Value.(*ssa.Function)
	if !ok {
		return
	}
	if _, local := f.c.functions[fn]; !local || !scalarLeaf(fn) {
		return
	}
	body := fn.Blocks[0].Instrs
	// At most two constant loads and four scalar instructions per expression.
	// This is a conservative bound on inserted words, before redundant moves disappear.
	cost := 1 + 6*(len(body)-1)
	if cost > 64-f.inlineWords {
		return
	}
	f.inlineWords += cost
	if f.inlineAt == nil {
		f.inlineAt = make(map[*ssa.Call]bool)
	}
	f.inlineAt[call] = true
	for _, ins := range body {
		var imm *ssa.Const
		if bin, ok := ins.(*ssa.BinOp); ok {
			_, imm, _ = immediateAdd(bin)
		}
		var buf [4]*ssa.Value
		for _, operand := range ins.Operands(buf[:0]) {
			if k, ok := (*operand).(*ssa.Const); ok && k != imm {
				f.register(k)
			}
		}
	}
}

// Scratch registers die at the call boundary and are reused by the next site.
func (f *lowerer) inlineLeaf(fn *ssa.Function, call *ssa.Call) {
	body := fn.Blocks[0].Instrs
	in := lowerer{fn: fn, c: f.c, code: f.code, r: f.r, p: f.p, regs: make(map[ssa.Value][]vreg), pos: call.Pos()}
	for j, p := range fn.Params {
		in.regs[p] = f.regs[call.Call.Args[j]]
	}
	result := body[len(body)-1].(*ssa.Return).Results[0]
	for _, ins := range body {
		in.pos = ins.Pos()
		var buf [4]*ssa.Value
		for _, operand := range ins.Operands(buf[:0]) {
			if k, ok := (*operand).(*ssa.Const); ok {
				in.regs[k] = f.regs[k]
			}
		}
		if v, ok := ins.(ssa.Value); ok {
			if v == result {
				in.regs[v] = f.regs[call]
			} else {
				in.register(v)
			}
			in.instruction(ins)
		} else {
			in.move(f.regs[call][0], in.regs[result][0])
		}
	}
	f.inlinePeak = max(f.inlinePeak, in.r)
}
