package evmcompiler

import (
	"go/token"

	"golang.org/x/tools/go/ssa"
)

// A loop header always runs on entry. Lift total arithmetic into its unique
// preheader when both operands are already available there. Keep the compare
// at the header so the latch can reuse it as a counted back edge.
func (f *lowerer) planInvariantBounds() {
	for _, header := range f.fn.Blocks {
		if len(header.Preds) != 2 || len(header.Succs) != 2 || len(header.Instrs) < 3 {
			continue
		}
		pre, latch := header.Preds[0], header.Preds[1]
		if header.Dominates(pre) {
			pre, latch = latch, pre
		}
		if header.Dominates(pre) || !header.Dominates(latch) || len(pre.Succs) != 1 {
			continue
		}
		for _, ins := range header.Instrs[:len(header.Instrs)-2] {
			bin, ok := ins.(*ssa.BinOp)
			if !ok {
				continue
			}
			switch bin.Op {
			case token.ADD, token.SUB, token.MUL, token.AND, token.OR, token.XOR:
			default:
				continue
			}
			if _, ok := numericType(bin.Type()); !ok {
				continue
			}
			available := func(v ssa.Value) bool {
				switch x := v.(type) {
				case *ssa.Parameter, *ssa.FreeVar, *ssa.Const:
					return true
				case ssa.Instruction:
					return x.Block().Dominates(pre) || (x.Block() == header && f.invariant[x])
				}
				return false
			}
			if !available(bin.X) || !available(bin.Y) {
				continue
			}
			if f.onEntry == nil {
				f.onEntry = make(map[*ssa.BasicBlock][]*ssa.BinOp)
				f.invariant = make(map[ssa.Instruction]bool)
			}
			f.invariant[bin] = true
			f.onEntry[pre] = append(f.onEntry[pre], bin)
		}
	}
}
