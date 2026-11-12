package evmcompiler

import (
	"go/token"

	"golang.org/x/tools/go/ssa"
)

// The last arithmetic instruction may overwrite a parameter in the return
// window. Its inputs have been read and the only instruction left is return.
func (f *lowerer) scalarReturnSlot(v ssa.Value) (uint16, bool) {
	if len(f.result.Slots) != 1 {
		return 0, false
	}
	var ins ssa.Instruction
	switch i := v.(type) {
	case *ssa.BinOp:
		ins = i
	case *ssa.Convert:
		ins = i
	case *ssa.UnOp:
		if i.Op != token.NOT && i.Op != token.SUB && i.Op != token.XOR {
			return 0, false
		}
		ins = i
	default:
		return 0, false
	}
	b := ins.Block()
	if len(*v.Referrers()) != 1 || len(b.Instrs) < 2 || b.Instrs[len(b.Instrs)-2] != ins {
		return 0, false
	}
	ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return)
	return f.result.Slots[0], ok && len(ret.Results) == 1 && ret.Results[0] == v
}
