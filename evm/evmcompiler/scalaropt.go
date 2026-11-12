package evmcompiler

import (
	"go/constant"
	"go/token"
	"go/types"
	"slices"

	"golang.org/x/tools/go/ssa"
)

// Value numbers are local to a basic block. An SSA operand identifies an
// immutable value. Numeric slice reads use a separate table, invalidated by
// any instruction that could write through an alias.
type exprKey struct {
	op   token.Token
	typ  types.Type
	x, y ssa.Value
	fn   *ssa.Function
}

func (c *typedCompiler) simplifyScalars(fn *ssa.Function) {
	var literals map[literalKey]ssa.Value
	canonical := func(v ssa.Value) ssa.Value {
		k, ok := v.(*ssa.Const)
		if !ok || k.Value == nil {
			return v
		}
		key := literalKey{k.Type(), k.Value.ExactString()}
		if old := literals[key]; old != nil {
			return old
		}
		if literals == nil {
			literals = make(map[literalKey]ssa.Value)
		}
		literals[key] = v
		return v
	}
	for _, b := range fn.Blocks {
		numbers := map[exprKey]ssa.Value{}
		var reads map[[2]ssa.Value]ssa.Value
		keep := b.Instrs[:0]
		for _, ins := range b.Instrs {
			var key exprKey
			var replacement ssa.Value
			switch i := ins.(type) {
			case *ssa.BinOp:
				if _, ok := numericType(i.X.Type()); ok {
					replacement = foldInteger(i)
					switch i.Op {
					case token.ADD, token.SUB, token.MUL, token.AND, token.OR, token.XOR, token.AND_NOT, token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ:
						key = exprKey{op: i.Op, typ: i.Type(), x: canonical(i.X), y: canonical(i.Y)}
					}
				}
			case *ssa.IndexAddr, *ssa.Phi, *ssa.ChangeType, *ssa.Convert:
			case *ssa.UnOp:
				// An earlier successful read checked the bounds. Keep that read and
				// remove the redundant load along with its single-use address.
				addr, ok := i.X.(*ssa.IndexAddr)
				if !ok || i.Op != token.MUL || len(keep) == 0 || keep[len(keep)-1] != addr || len(*addr.Referrers()) != 1 {
					break
				}
				if _, ok := addr.X.Type().Underlying().(*types.Slice); !ok {
					break
				}
				if _, ok := numericType(i.Type()); !ok {
					break
				}
				k := [2]ssa.Value{addr.X, canonical(addr.Index)}
				if replacement = reads[k]; replacement != nil {
					keep = keep[:len(keep)-1]
					forgetInputs(addr)
				} else {
					if reads == nil {
						reads = make(map[[2]ssa.Value]ssa.Value)
					}
					reads[k] = i
				}
			case *ssa.Call:
				clear(reads)
				target := i.Call.StaticCallee()
				if target != nil && len(i.Call.Args) <= 2 {
					if _, local := c.functions[target]; local && scalarLeaf(target) {
						key.fn, key.typ = target, i.Type()
						if len(i.Call.Args) > 0 {
							key.x = canonical(i.Call.Args[0])
						}
						if len(i.Call.Args) > 1 {
							key.y = canonical(i.Call.Args[1])
						}
					}
				}
			default:
				clear(reads)
			}
			if replacement == nil && key.typ != nil {
				replacement = numbers[key]
				// Keep the trailing induction update available to fuse with its back edge.
				if replacement != nil && key.op == token.ADD && b.Instrs[len(b.Instrs)-2] == ins &&
					len(b.Succs) == 1 && b.Succs[0].Dominates(b) {
					replacement = nil
				}
				if replacement == nil {
					numbers[key] = ins.(ssa.Value)
				}
			}
			if replacement == nil {
				keep = append(keep, ins)
				continue
			}
			replaceScalar(ins.(ssa.Value), replacement, ins)
		}
		clear(b.Instrs[len(keep):])
		b.Instrs = keep
	}
}

func replaceScalar(v, replacement ssa.Value, ins ssa.Instruction) {
	var buf [4]*ssa.Value
	for _, use := range *v.Referrers() {
		for _, operand := range use.Operands(buf[:0]) {
			if *operand == v {
				*operand = replacement
				if refs := replacement.Referrers(); refs != nil {
					*refs = append(*refs, use)
				}
			}
		}
	}
	*v.Referrers() = nil
	forgetInputs(ins)
}

func forgetInputs(ins ssa.Instruction) {
	var buf [4]*ssa.Value
	for _, operand := range ins.Operands(buf[:0]) {
		if refs := (*operand).Referrers(); refs != nil {
			*refs = slices.DeleteFunc(*refs, func(use ssa.Instruction) bool { return use == ins })
		}
	}
}

func integerWord(v ssa.Value) (uint64, bool) {
	k, ok := v.(*ssa.Const)
	if !ok || k.Value == nil || k.Value.Kind() != constant.Int {
		return 0, false
	}
	if n, ok := constant.Int64Val(k.Value); ok {
		return uint64(n), true
	}
	return constant.Uint64Val(k.Value)
}

func foldInteger(i *ssa.BinOp) ssa.Value {
	t, ok := i.Type().Underlying().(*types.Basic)
	if !ok || t.Info()&types.IsInteger == 0 {
		return nil
	}
	x, cx := integerWord(i.X)
	y, cy := integerWord(i.Y)
	// These identities preserve overflow and cannot discard a trapping operation.
	// Producers, including calls used in x*0, retain their own instruction.
	switch i.Op {
	case token.ADD, token.OR, token.XOR:
		if cy && y == 0 {
			return i.X
		}
		if cx && x == 0 {
			return i.Y
		}
	case token.SUB, token.AND_NOT:
		if cy && y == 0 {
			return i.X
		}
	case token.MUL:
		if cy && y == 1 || cx && x == 0 {
			return i.X
		}
		if cx && x == 1 || cy && y == 0 {
			return i.Y
		}
	case token.AND:
		if cy && y == 0 {
			return i.Y
		}
		if cx && x == 0 {
			return i.X
		}
	}
	if i.X == i.Y {
		switch i.Op {
		case token.AND, token.OR:
			return i.X
		case token.SUB, token.XOR, token.AND_NOT:
			return ssa.NewConst(constant.MakeInt64(0), i.Type())
		}
	}
	if !cx || !cy {
		return nil
	}
	var result uint64
	switch i.Op {
	case token.ADD:
		result = x + y
	case token.SUB:
		result = x - y
	case token.MUL:
		result = x * y
	case token.AND:
		result = x & y
	case token.OR:
		result = x | y
	case token.XOR:
		result = x ^ y
	case token.AND_NOT:
		result = x &^ y
	default:
		return nil
	}
	n, _ := numericType(i.Type())
	shift := 64 - n.Bits
	if t.Info()&types.IsUnsigned == 0 {
		return ssa.NewConst(constant.MakeInt64(int64(result<<shift)>>shift), i.Type())
	}
	return ssa.NewConst(constant.MakeUint64(result<<shift>>shift), i.Type())
}
