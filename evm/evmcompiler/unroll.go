package evmcompiler

import (
	"go/constant"
	"go/token"
	"slices"

	"github.com/andreyvit/edb/evm"
	"golang.org/x/tools/go/ssa"
)

// The fused back edge already proves a signed unit increment with one exit.
// Duplicate only a single arithmetic block reached with a constant counter.
func (f *lowerer) unroll(header, body, latch *ssa.BasicBlock, cmp, add *ssa.BinOp, entry, pc int) {
	if body != latch || len(latch.Preds) != 1 || len(header.Preds) != 2 || pc <= entry || f.code.Code[pc].Op() != evm.OpLoopLtI64 && f.code.Code[pc].Op() != evm.OpLoopLeI64 {
		return
	}
	phi, ok := cmp.X.(*ssa.Phi)
	if !ok {
		return
	}
	back := slices.Index(header.Preds, latch)
	if back < 0 || phi.Edges[back] != add {
		return
	}
	word := func(v ssa.Value) (int64, bool) {
		c, ok := v.(*ssa.Const)
		if !ok || c.Value == nil {
			return 0, false
		}
		return constant.Int64Val(c.Value)
	}
	lo, ok := word(phi.Edges[1-back])
	if !ok || lo != int64(int16(lo)) {
		return
	}
	end, ok := word(cmp.Y)
	if !ok || end != int64(int16(end)) {
		return
	}
	if cmp.Op == token.LEQ {
		end++
	}
	trips := int(end - lo)
	size := pc - entry
	if trips < 2 || trips > 4 || size > 16 || (size+1)*(trips-1) > 48-f.unrollGrowth {
		return
	}
	counter := f.code.Code[pc].A()
	needIndex := false
	for _, i := range f.code.Code[entry:pc] {
		if i.A() == counter {
			return
		}
		switch i.Op() {
		case evm.OpAddI64:
			needIndex = needIndex || i.B() == counter && i.C() == counter
		case evm.OpConst:
		case evm.OpAddImmI64, evm.OpMoveR, evm.OpConvert:
			needIndex = needIndex || i.B() == counter
		case evm.OpSubI64, evm.OpMulI64, evm.OpAnd, evm.OpOr, evm.OpXor, evm.OpShrI64, evm.OpShrU64, evm.OpShl,
			evm.OpLtI64, evm.OpLtU64, evm.OpLeI64, evm.OpLeU64, evm.OpEqR,
			evm.OpAddF64, evm.OpSubF64, evm.OpMulF64, evm.OpDivF64:
			needIndex = needIndex || i.B() == counter || i.C() == counter
		default:
			return
		}
	}
	// This fence also excludes calls, pointers, traps and branch patches.
	words := slices.Clone(f.code.Code[entry:pc])
	positions := slices.Clone(f.code.Positions[entry:pc])
	increment := f.code.Positions[pc]
	f.code.Code = f.code.Code[:entry]
	f.code.Positions = f.code.Positions[:entry]
	emit := func(i evm.Instruction, p evm.SourcePosition) {
		f.code.Code = append(f.code.Code, i)
		f.code.Positions = append(f.code.Positions, p)
	}
	for index := lo; index < end; index++ {
		if needIndex && index != lo {
			emit(evm.WithBC(evm.OpConst, counter, f.c.word(uint64(index))), increment)
		}
		for j, i := range words {
			if i.Op() == evm.OpAddI64 && (i.B() == counter) != (i.C() == counter) {
				other := i.B()
				if other == counter {
					other = i.C()
				}
				i = evm.ABC(evm.OpAddImmI64, i.A(), other, uint16(index))
			}
			emit(i, positions[j])
		}
	}
	// A use after the loop may observe its final induction variable.
	emit(evm.WithBC(evm.OpConst, counter, f.c.word(uint64(end))), increment)
	f.unrollGrowth += len(f.code.Code) - pc - 1
}
