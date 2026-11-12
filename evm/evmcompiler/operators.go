package evmcompiler

import (
	"go/constant"
	"go/token"
	"go/types"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ssa"
)

// Branch on equality with reversed successors when nobody needs the != value.
func branchUsesInequality(i *ssa.BinOp) bool {
	if i.Op != token.NEQ || len(*i.Referrers()) != 1 {
		return false
	}
	_, ok := (*i.Referrers())[0].(*ssa.If)
	return ok
}

// immediateAdd returns the nonliteral operand and the signed addend. Narrow
// integer arithmetic still goes through the ordinary narrowing conversion.
func immediateAdd(i *ssa.BinOp) (ssa.Value, *ssa.Const, int16) {
	if i.Op != token.ADD && i.Op != token.SUB {
		return nil, nil, 0
	}
	n, ok := numericType(i.Type())
	if !ok || n.Kind == evm.NumericFloat || n.Bits != 64 {
		return nil, nil, 0
	}
	v, k := i.X, i.Y
	c, ok := k.(*ssa.Const)
	if !ok && i.Op == token.ADD {
		v, k = i.Y, i.X
		c, ok = k.(*ssa.Const)
	}
	if !ok || c.Value == nil || c.Value.Kind() != constant.Int {
		return nil, nil, 0
	}
	word, ok := constant.Int64Val(c.Value)
	if i.Op == token.SUB {
		word = -word
	}
	if !ok || word != int64(int16(word)) {
		return nil, nil, 0
	}
	return v, c, int16(word)
}

func (f *lowerer) binary(i *ssa.BinOp) {
	if x, c, imm := immediateAdd(i); f.optimizing() && c != nil {
		f.emit(evm.ABC(evm.OpAddImmI64, f.regs[i][0].slot, f.regs[x][0].slot, uint16(imm)))
		return
	}
	dst, x, y := f.regs[i][0], f.regs[i.X][0], f.regs[i.Y][0]
	if i.Op == token.EQL || i.Op == token.NEQ || i.Op == token.LSS || i.Op == token.GTR || i.Op == token.LEQ || i.Op == token.GEQ {
		op := i.Op
		if f.optimizing() && branchUsesInequality(i) {
			op = token.EQL
		}
		f.compare(dst, x, y, i.X.Type(), op)
		return
	}
	if s := f.shape(i.Type()); s != nil && s.Kind == values.ComplexKind {
		flag := uint8(0)
		switch i.Op {
		case token.ADD:
			flag = '+'
		case token.SUB:
			flag = '-'
		case token.MUL:
			flag = '*'
		case token.QUO:
			flag = '/'
		}
		f.extended(evm.ExtendedComplexBinary, s, []vreg{x, y}, []vreg{dst}, 0, flag, nil)
		return
	}
	n, numeric := numericType(i.X.Type())
	var op evm.Opcode
	if b, ok := i.X.Type().Underlying().(*types.Basic); ok && b.Info()&types.IsString != 0 && i.Op == token.ADD {
		op = evm.OpConcatStr
	} else if numeric && n.Kind == evm.NumericFloat {
		if n.Bits == 32 {
			switch i.Op {
			case token.ADD:
				op = evm.OpAddF32
			case token.SUB:
				op = evm.OpSubF32
			case token.MUL:
				op = evm.OpMulF32
			case token.QUO:
				op = evm.OpDivF32
			}
		} else {
			switch i.Op {
			case token.ADD:
				op = evm.OpAddF64
			case token.SUB:
				op = evm.OpSubF64
			case token.MUL:
				op = evm.OpMulF64
			case token.QUO:
				op = evm.OpDivF64
			}
		}
	} else if numeric {
		switch i.Op {
		case token.ADD:
			op = evm.OpAddI64
		case token.SUB:
			op = evm.OpSubI64
		case token.MUL:
			op = evm.OpMulI64
		case token.QUO:
			op = evm.OpDivI64
			if n.Kind == evm.NumericUnsigned {
				op = evm.OpDivU64
			}
		case token.REM:
			op = evm.OpRemI64
			if n.Kind == evm.NumericUnsigned {
				op = evm.OpRemU64
			}
		case token.AND:
			op = evm.OpAnd
		case token.OR:
			op = evm.OpOr
		case token.XOR:
			op = evm.OpXor
		case token.AND_NOT:
			mask := f.literal(^uint64(0))
			tmp := f.alloc(y.typ)
			f.emit(evm.ABC(evm.OpXor, tmp.slot, y.slot, mask.slot))
			y = tmp
			op = evm.OpAnd
		case token.SHL, token.SHR:
			if count, ok := numericType(i.Y.Type()); ok && count.Kind == evm.NumericSigned {
				f.emit(evm.ABC(evm.OpCheckShift, y.slot, 0, 0))
			}
			op = evm.OpShl
			if i.Op == token.SHR {
				op = evm.OpShrI64
				if n.Kind == evm.NumericUnsigned {
					op = evm.OpShrU64
				}
			}
		}
	}
	if op == 0 {
		f.c.fail(i.Pos(), "operator %s on %s is not implemented yet", i.Op, i.X.Type())
		return
	}
	f.emit(evm.ABC(op, dst.slot, x.slot, y.slot))
	if numeric && n.Kind != evm.NumericFloat && n.Bits < 64 {
		f.numericConvert(dst, dst, evm.NumericType{Kind: n.Kind, Bits: 64}, n)
	}
}
func (f *lowerer) compare(dst, x, y vreg, t types.Type, op token.Token) {
	if f.extendedCompare(dst, x, y, t, op) {
		return
	}
	eq, lt := evm.OpEqR, evm.OpLtI64
	if _, ok := t.Underlying().(*types.Pointer); ok || x.typ.Repr() != values.Raw && f.shape(t).Kind != values.StringKind && f.shape(t).Kind != values.FunctionKind {
		eq = evm.OpEqP
	} else if b, ok := t.Underlying().(*types.Basic); ok {
		switch {
		case b.Info()&types.IsString != 0:
			eq, lt = evm.OpEqStr, evm.OpLtStr
		case b.Kind() == types.Float32:
			eq, lt = evm.OpEqF32, evm.OpLtF32
		case b.Kind() == types.Float64:
			eq, lt = evm.OpEqF64, evm.OpLtF64
		case b.Info()&types.IsUnsigned != 0:
			lt = evm.OpLtU64
		}
	}
	switch op {
	case token.EQL, token.NEQ:
		f.emit(evm.ABC(eq, dst.slot, x.slot, y.slot))
		if op == token.NEQ {
			z := f.literal(0)
			f.emit(evm.ABC(evm.OpEqR, dst.slot, dst.slot, z.slot))
		}
	case token.LSS:
		f.emit(evm.ABC(lt, dst.slot, x.slot, y.slot))
	case token.GTR:
		f.emit(evm.ABC(lt, dst.slot, y.slot, x.slot))
	case token.LEQ, token.GEQ:
		if op == token.GEQ {
			x, y = y, x
		}
		if lt == evm.OpLtI64 || lt == evm.OpLtU64 {
			le := evm.OpLeI64
			if lt == evm.OpLtU64 {
				le = evm.OpLeU64
			}
			f.emit(evm.ABC(le, dst.slot, x.slot, y.slot))
			return
		}
		tmp := f.alloc(values.Boolean)
		f.emit(evm.ABC(eq, tmp.slot, x.slot, y.slot))
		f.emit(evm.ABC(lt, dst.slot, x.slot, y.slot))
		f.emit(evm.ABC(evm.OpOr, dst.slot, dst.slot, tmp.slot))
	}
}
func (f *lowerer) unary(i *ssa.UnOp) {
	dst, x := f.regs[i][0], f.regs[i.X][0]
	if i.Op == token.MUL {
		if f.isReadRecord(x.typ) {
			f.extended(evm.ExtendedRecordCopy, f.shape(i.Type()), []vreg{x}, []vreg{dst}, 0, 0, nil)
			return
		}
		f.load(dst, x, i.Type())
		return
	}
	if s := f.shape(i.Type()); s != nil && s.Kind == values.ComplexKind {
		f.extended(evm.ExtendedComplexNegate, s, []vreg{x}, []vreg{dst}, 0, 0, nil)
		return
	}
	switch i.Op {
	case token.NOT:
		z := f.literal(0)
		f.emit(evm.ABC(evm.OpEqR, dst.slot, x.slot, z.slot))
	case token.SUB:
		if n, ok := numericType(i.Type()); ok && n.Kind == evm.NumericFloat {
			mask := uint64(1) << 63
			if n.Bits == 32 {
				mask = 1 << 31
			}
			m := f.literal(mask)
			f.emit(evm.ABC(evm.OpXor, dst.slot, x.slot, m.slot))
		} else {
			z := f.literal(0)
			f.emit(evm.ABC(evm.OpSubI64, dst.slot, z.slot, x.slot))
			f.normalize(dst, i.Type())
		}
	case token.XOR:
		m := f.literal(^uint64(0))
		f.emit(evm.ABC(evm.OpXor, dst.slot, x.slot, m.slot))
		f.normalize(dst, i.Type())
	default:
		f.c.fail(i.Pos(), "unary operator %s is not implemented yet", i.Op)
	}
}
func (f *lowerer) normalize(dst vreg, t types.Type) {
	if n, ok := numericType(t); ok && n.Kind != evm.NumericFloat && n.Bits < 64 {
		f.numericConvert(dst, dst, evm.NumericType{Kind: n.Kind, Bits: 64}, n)
	}
}
func (f *lowerer) convert(dst, src vreg, from, to types.Type) {
	aShape, bShape := f.shape(from), f.shape(to)
	if aShape != nil && bShape != nil && (aShape.Kind == values.StringKind || bShape.Kind == values.StringKind || aShape.Kind == values.ComplexKind && bShape.Kind == values.ComplexKind) {
		f.extended(evm.ExtendedConvert, bShape, []vreg{src}, []vreg{dst}, 0, 0, aShape)
		return
	}
	a, ok := numericType(from)
	b, ok2 := numericType(to)
	if !ok || !ok2 {
		f.c.fail(f.pos, "conversion from %s to %s is not implemented yet", from, to)
		return
	}
	f.numericConvert(dst, src, a, b)
}
func (f *lowerer) numericConvert(dst, src vreg, from, to evm.NumericType) {
	conv := evm.Conversion{From: from, To: to}
	index, ok := f.c.conversions[conv]
	if !ok {
		if len(f.c.module.Conversions) >= 65536 {
			f.c.fail(f.pos, "too many numeric conversions")
			return
		}
		index = uint16(len(f.c.module.Conversions))
		f.c.conversions[conv] = index
		f.c.module.Conversions = append(f.c.module.Conversions, conv)
	}
	f.emit(evm.ABC(evm.OpConvert, dst.slot, src.slot, index))
}
