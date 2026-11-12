package evmcompiler

import (
	"go/constant"
	"go/token"
	"go/types"
	"math"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ssa"
)

var iteratorType = values.NewType("range iterator", values.Pointer)

func (f *lowerer) shape(t types.Type) *values.Shape {
	s, err := f.c.env.shape(t)
	if err != nil {
		f.c.fail(f.pos, "%v", err)
	}
	return s
}
func (f *lowerer) shapeIndex(s *values.Shape) uint16 {
	if i, ok := f.c.shapes[s]; ok {
		return i
	}
	if len(f.c.module.Shapes) >= 65536 {
		f.c.fail(f.pos, "too many storage shapes")
		return 0
	}
	i := uint16(len(f.c.module.Shapes))
	f.c.module.Shapes = append(f.c.module.Shapes, s)
	f.c.shapes[s] = i
	return i
}
func operand(r vreg) evm.Operand { return evm.Operand{Slot: r.slot, Repr: r.typ.Repr()} }
func (f *lowerer) extended(kind evm.ExtendedKind, s *values.Shape, in, out []vreg, index uint64, flags uint8, source *values.Shape) {
	op := evm.ExtendedOp{Kind: kind, Shape: s, Source: source, Index: index, Flags: flags}
	for _, r := range in {
		op.In = append(op.In, operand(r))
	}
	for _, r := range out {
		op.Out = append(op.Out, operand(r))
	}
	idx := uint32(len(f.c.module.ExtendedOps))
	f.c.module.ExtendedOps = append(f.c.module.ExtendedOps, op)
	f.emit(evm.WithBC(evm.OpExtended, 0, idx))
}
func (f *lowerer) extendedInstruction(i ssa.Instruction) bool {
	reg := func(v ssa.Value) vreg { return f.regs[v][0] }
	switch i := i.(type) {
	case *ssa.Alloc:
		s := f.shape(i.Type().Underlying().(*types.Pointer).Elem())
		if s.NativeStorage() && reg(i).typ.Repr() == values.Pair {
			f.extended(evm.ExtendedNativeAlloc, s, nil, f.regs[i], 0, 0, nil)
			return true
		}
		if f.packed[i] {
			f.extended(evm.ExtendedPackedArray, s, nil, f.regs[i], 0, 0, nil)
		} else if s != nil {
			f.emit(evm.ABC(evm.OpAlloc, reg(i).slot, f.shapeIndex(s), 0))
			if reg(i).typ.Repr() == values.Pair {
				f.emit(evm.WithBC(evm.OpConst, reg(i).slot, f.c.word(0)))
			}
		}
	case *ssa.Store:
		s := f.shape(i.Val.Type())
		if f.packed[i.Addr] {
			f.extended(evm.ExtendedPackedStore, s, []vreg{reg(i.Addr), reg(i.Val)}, nil, 0, 0, nil)
		} else if s != nil {
			if reg(i.Addr).typ.Repr() == values.Pair {
				f.extended(evm.ExtendedAddressStore, s, []vreg{reg(i.Addr), reg(i.Val)}, nil, 0, 0, nil)
				return true
			}
			f.emit(evm.ABC(evm.OpStore, reg(i.Addr).slot, reg(i.Val).slot, f.shapeIndex(s)))
		}
	case *ssa.UnOp:
		if i.Op != token.MUL || !f.packed[i.X] {
			return false
		}
		f.extended(evm.ExtendedPackedLoad, f.shape(i.Type()), []vreg{reg(i.X)}, f.regs[i], 0, 0, nil)
	case *ssa.FieldAddr:
		s := f.shape(i.X.Type().Underlying().(*types.Pointer).Elem())
		f.extended(evm.ExtendedFieldAddr, s, []vreg{reg(i.X)}, f.regs[i], evm.VMFieldBase|uint64(i.Field), 0, nil)
	case *ssa.Field:
		s := f.shape(i.X.Type())
		f.extended(evm.ExtendedField, s, []vreg{reg(i.X)}, f.regs[i], evm.VMFieldBase|uint64(i.Field), 0, nil)
	case *ssa.IndexAddr:
		t := i.X.Type()
		if ptr, ok := t.Underlying().(*types.Pointer); ok {
			t = ptr.Elem()
		}
		s := f.shape(t)
		f.extended(evm.ExtendedIndexAddr, s, []vreg{reg(i.X), reg(i.Index)}, f.regs[i], 0, 0, nil)
		if f.packed[i.X] {
			f.markPacked()
		}
	case *ssa.Index:
		s := f.shape(i.X.Type())
		f.extended(evm.ExtendedIndex, s, []vreg{reg(i.X), reg(i.Index)}, f.regs[i], 0, 0, nil)
		if f.packed[i.X] {
			f.markPacked()
		}
	case *ssa.MakeSlice:
		s := f.shape(i.Type())
		f.extended(evm.ExtendedMakeSlice, s, []vreg{reg(i.Len), reg(i.Cap)}, f.regs[i], 0, 0, nil)
		if f.packed[i] {
			f.markPacked()
		}
	case *ssa.Slice:
		t := i.X.Type()
		if ptr, ok := t.Underlying().(*types.Pointer); ok {
			t = ptr.Elem()
		}
		s := f.shape(t)
		inputs := []vreg{reg(i.X)}
		flags := uint8(0)
		for j, v := range []ssa.Value{i.Low, i.High, i.Max} {
			if v != nil {
				inputs = append(inputs, reg(v))
			} else {
				inputs = append(inputs, f.literal(0))
				if j == 1 {
					flags |= 1
				}
				if j == 2 {
					flags |= 2
				}
			}
		}
		f.extended(evm.ExtendedSlice, s, inputs, f.regs[i], 0, flags, nil)
		if f.packed[i.X] {
			f.markPacked()
		}
	case *ssa.SliceToArrayPointer:
		f.extended(evm.ExtendedSliceToArray, f.shape(i.Type()), []vreg{reg(i.X)}, f.regs[i], 0, 0, nil)
	case *ssa.MakeMap:
		var args []vreg
		if i.Reserve != nil {
			args = []vreg{reg(i.Reserve)}
		}
		f.extended(evm.ExtendedMakeMap, f.shape(i.Type()), args, f.regs[i], 0, 0, nil)
	case *ssa.Lookup:
		f.extended(evm.ExtendedLookup, f.shape(i.X.Type()), []vreg{reg(i.X), reg(i.Index)}, f.regs[i], 0, 0, nil)
	case *ssa.MapUpdate:
		f.extended(evm.ExtendedMapUpdate, f.shape(i.Map.Type()), []vreg{reg(i.Map), reg(i.Key), reg(i.Value)}, nil, 0, 0, nil)
	case *ssa.Range:
		f.extended(evm.ExtendedRange, f.shape(i.X.Type()), []vreg{reg(i.X)}, f.regs[i], 0, 0, nil)
	case *ssa.Next:
		flags := uint8(0)
		if i.IsString {
			flags = 1
		}
		f.extended(evm.ExtendedNext, f.shape(i.Iter.(*ssa.Range).X.Type()), []vreg{reg(i.Iter)}, f.regs[i], 0, flags, nil)
	default:
		return false
	}
	return true
}
func (f *lowerer) builtin(call *ssa.Call, name string) bool {
	args := make([]vreg, len(call.Call.Args))
	for j, a := range call.Call.Args {
		args[j] = f.regs[a][0]
	}
	var s *values.Shape
	if len(args) > 0 {
		s = f.shape(call.Call.Args[0].Type())
	}
	if (name == "len" || name == "cap") && len(call.Call.Args) == 1 {
		if _, ok := f.recordLists[call.Call.Args[0]]; ok {
			f.recordListOp(evm.ExtendedRecordListLen, call.Call.Args[0], args, f.regs[call])
			return true
		}
	}
	var kind evm.ExtendedKind
	flags := uint8(0)
	switch name {
	case "min", "max":
		kind = evm.ExtendedMinMax
		if name == "max" {
			flags = 1
		}
	case "len":
		if f.optimizing() {
			dst := f.regs[call][0]
			switch s.Kind {
			case values.StringKind:
				f.emit(evm.ABC(evm.OpMoveR, dst.slot, args[0].slot, 0))
				return true
			case values.SliceKind:
				f.numericConvert(dst, args[0], evm.NumericType{Kind: evm.NumericUnsigned, Bits: 64}, evm.NumericType{Kind: evm.NumericUnsigned, Bits: 32})
				return true
			}
		}
		kind = evm.ExtendedLen
	case "cap":
		kind = evm.ExtendedCap
	case "append":
		kind = evm.ExtendedAppend
		if len(args) == 2 && f.shape(call.Call.Args[1].Type()).Kind == values.StringKind {
			flags = 1
		}
	case "copy":
		kind = evm.ExtendedCopy
		if f.shape(call.Call.Args[1].Type()).Kind == values.StringKind {
			flags = 1
		}
	case "delete":
		kind = evm.ExtendedDelete
	case "clear":
		kind = evm.ExtendedClear
	case "complex":
		kind = evm.ExtendedComplex
		s = f.shape(call.Type())
	case "real":
		kind = evm.ExtendedReal
	case "imag":
		kind = evm.ExtendedImag
	default:
		return false
	}
	f.extended(kind, s, args, f.regs[call], 0, flags, nil)
	switch kind {
	case evm.ExtendedAppend, evm.ExtendedCopy, evm.ExtendedClear:
		if f.packed[call] || f.packed[call.Call.Args[0]] || len(args) == 2 && f.packed[call.Call.Args[1]] {
			f.markPacked()
		}
	}
	return true
}
func (f *lowerer) complexConstant(c *ssa.Const, dst vreg) {
	realpart, _ := constant.Float64Val(constant.Real(c.Value))
	imagpart, _ := constant.Float64Val(constant.Imag(c.Value))
	s := f.shape(c.Type())
	if s.Bits == 64 {
		real32, _ := constant.Float32Val(constant.Real(c.Value))
		imag32, _ := constant.Float32Val(constant.Imag(c.Value))
		f.emit(evm.WithBC(evm.OpConst, dst.slot, f.c.word(uint64(math.Float32bits(real32))|uint64(math.Float32bits(imag32))<<32)))
		return
	}
	x, y := f.literal(math.Float64bits(realpart)), f.literal(math.Float64bits(imagpart))
	f.extended(evm.ExtendedComplex, s, []vreg{x, y}, []vreg{dst}, 0, 0, nil)
}
func (f *lowerer) load(dst, src vreg, t types.Type) {
	s := f.shape(t)
	if s != nil {
		if src.typ.Repr() == values.Pair {
			f.extended(evm.ExtendedAddressLoad, s, []vreg{src}, []vreg{dst}, 0, 0, nil)
			return
		}
		f.emit(evm.ABC(evm.OpLoad, dst.slot, src.slot, f.shapeIndex(s)))
	}
}
func (f *lowerer) extendedCompare(dst, x, y vreg, t types.Type, op token.Token) bool {
	s := f.shape(t)
	if s == nil {
		return true
	}
	if s.Kind != values.StructKind && s.Kind != values.ArrayKind && s.Kind != values.ComplexKind && s.Kind != values.InterfaceKind && s.Kind != values.TypedPairKind && s.Kind != values.TimestampKind {
		return false
	}
	f.extended(evm.ExtendedEqual, s, []vreg{x, y}, []vreg{dst}, 0, 0, nil)
	if op == token.NEQ {
		z := f.literal(0)
		f.emit(evm.ABC(evm.OpEqR, dst.slot, dst.slot, z.slot))
	}
	return true
}
