package evmcompiler

import (
	"go/constant"
	"go/token"
	"go/types"
	"math"
	"slices"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ssa"
)

type vreg struct {
	slot uint16
	typ  *values.Type
}
type literalKey struct {
	typ   types.Type
	value string
}

type branchPatch struct {
	pc     int
	target int
}
type flowEdge struct {
	from, to *ssa.BasicBlock
	label    int
}
type outgoingPatch struct {
	pc   int
	read bool
}
type lowerer struct {
	ownership               *ownershipPlan
	c                       *typedCompiler
	fn                      *ssa.Function
	code                    *evm.Function
	regs                    map[ssa.Value][]vreg
	packed                  map[ssa.Value]bool
	recordLists             map[ssa.Value]recordProperty
	constants               []*ssa.Const
	literals                map[literalKey][]vreg
	globals                 []*ssa.Global
	functionValues          []*ssa.Function
	r, p                    int
	outR, outP              int
	inlineWords, inlinePeak int
	unrollGrowth            int
	inlineAt                map[*ssa.Call]bool
	onEntry                 map[*ssa.BasicBlock][]*ssa.BinOp
	invariant               map[ssa.Instruction]bool
	outgoing                []outgoingPatch
	branches                []branchPatch
	labels                  map[int]int
	edges                   []flowEdge
	result                  evm.Layout
	pos                     token.Pos
}

func (f *lowerer) optimizing() bool { return !f.c.module.NoOptimization }

func (c *typedCompiler) lowerFunction(fn *ssa.Function) {
	f := &lowerer{c: c, fn: fn, code: &c.module.Functions[c.functions[fn]], regs: make(map[ssa.Value][]vreg), labels: make(map[int]int), pos: fn.Pos()}
	params, err := evm.ParameterLayout(f.code.Signature.Params, f.code.Method)
	if err != nil {
		c.fail(fn.Pos(), "%v", err)
		return
	}
	f.result, err = evm.LayoutOf(f.code.Signature.Results)
	if err != nil {
		c.fail(fn.Pos(), "%v", err)
		return
	}
	f.r, f.p = max(params.RCount, f.result.RCount), max(params.PCount, f.result.PCount)
	for i, p := range fn.Params {
		f.regs[p] = []vreg{{params.Slots[i], f.code.Signature.Params[i]}}
	}
	for _, v := range fn.FreeVars {
		if c.deferValues[v] {
			r := f.alloc(deferStackType)
			f.regs[v] = []vreg{r}
			f.code.Captures = append(f.code.Captures, evm.CaptureSlot{Type: deferStackType, Slot: r.slot})
			continue
		}
		t, err := c.env.valueType(v.Type())
		if err != nil {
			c.fail(v.Pos(), "%v", err)
			return
		}
		r := f.alloc(t)
		f.regs[v] = []vreg{r}
		f.code.Captures = append(f.code.Captures, evm.CaptureSlot{Type: t, Slot: r.slot})
	}
	f.ownership = f.planOwnership()
	for _, b := range fn.Blocks {
		for _, i := range b.Instrs {
			if call, ok := i.(*ssa.Call); ok && f.optimizing() && len(f.ownership.before[i]) == 0 && len(f.ownership.after[i]) == 0 {
				f.planInline(call)
			}
			if v, ok := i.(ssa.Value); ok {
				f.register(v)
			}
			var immediate *ssa.Const
			if bin, ok := i.(*ssa.BinOp); ok && f.optimizing() {
				_, immediate, _ = immediateAdd(bin)
			}
			for _, operand := range i.Operands(nil) {
				if operand == nil || *operand == nil {
					continue
				}
				switch v := (*operand).(type) {
				case *ssa.Const:
					if v != immediate {
						f.register(v)
					}
				case *ssa.Global:
					f.register(v)
				case *ssa.Function:
					if call, ok := i.(ssa.CallInstruction); ok && operand == &call.Common().Value {
						continue
					}
					if closure, ok := i.(*ssa.MakeClosure); ok && operand == &closure.Fn {
						continue
					}
					if v.Name() != "init" || v.Pkg == c.pkg {
						f.register(v)
					}
				}
			}
		}
	}
	if len(c.errors) > 0 {
		return
	}
	if f.optimizing() {
		f.reuseLoopSlots()
		f.packed = f.findPackedSlices()
		f.recordLists = f.findRecordLists()
		f.planInvariantBounds()
	}
	for _, fn := range f.functionValues {
		f.emit(evm.WithBC(evm.OpFunction, f.regs[fn][0].slot, c.functionRef(fn, f.pos)))
	}
	for _, g := range f.globals {
		f.emit(evm.WithBC(evm.OpGlobal, f.regs[g][0].slot, c.globals[g]))
		if f.regs[g][0].typ.Repr() == values.Pair {
			tag := uint64(0)
			if f.shape(g.Type().Underlying().(*types.Pointer).Elem()).NativeStorage() {
				tag = values.NativeAddress
			}
			f.emit(evm.WithBC(evm.OpConst, f.regs[g][0].slot, c.word(tag)))
		}
	}
	for _, v := range f.constants {
		f.loadConstant(v, f.regs[v][0])
	}
	for _, b := range fn.Blocks {
		f.labels[b.Index] = len(f.code.Code)
		for j := 0; j < len(b.Instrs); j++ {
			i := b.Instrs[j]
			if f.invariant[i] {
				continue
			}
			f.pos = i.Pos()
			if f.pos == token.NoPos {
				f.pos = fn.Pos()
			}
			if f.optimizing() && j+1 < len(b.Instrs) && !f.invariant[b.Instrs[j+1]] && len(f.ownership.before[i]) == 0 && len(f.ownership.after[i]) == 0 && len(f.ownership.before[b.Instrs[j+1]]) == 0 && len(f.ownership.after[b.Instrs[j+1]]) == 0 && f.fuseAdjacent(i, b.Instrs[j+1]) {
				j++
				continue
			}
			f.emitOwnership(f.ownership.before[i])
			f.instruction(i)
			f.emitOwnership(f.ownership.after[i])
		}
	}
	for _, edge := range f.edges {
		f.labels[edge.label] = len(f.code.Code)
		f.copyPhis(edge.from, edge.to)
		f.jump(evm.OpJump, 0, edge.to.Index)
	}
	if fn.Recover != nil {
		f.code.RecoverPC = uint32(f.labels[fn.Recover.Index] + 1)
	}
	f.r = max(f.r, f.inlinePeak)
	f.code.RCount, f.code.PCount = f.r, f.p
	if f.r+f.outR > 65536 || f.p+f.outP > 65536 {
		c.fail(fn.Pos(), "locals and outgoing window exceed register address range")
	}
	for _, p := range f.outgoing {
		ins := f.code.Code[p.pc]
		base := f.r
		if ins.Op() == evm.OpMoveP {
			base = f.p
		}
		// Outgoing writes use A; outgoing result reads use B.
		if p.read {
			f.code.Code[p.pc] = evm.ABC(ins.Op(), ins.A(), uint16(base+int(ins.B())), 0)
		} else {
			f.code.Code[p.pc] = evm.ABC(ins.Op(), uint16(base+int(ins.A())), ins.B(), ins.C())
		}
	}
	for _, p := range f.branches {
		ins := f.code.Code[p.pc]
		delta := int64(f.labels[p.target]) - int64(p.pc) - 1
		if delta < math.MinInt32 || delta > math.MaxInt32 {
			c.fail(fn.Pos(), "branch exceeds instruction range")
		}
		f.code.Code[p.pc] = evm.WithBC(ins.Op(), ins.A(), uint32(int32(delta)))
	}
}
func (f *lowerer) alloc(t *values.Type) vreg { return f.allocBanks(t, t.Repr()) }
func (f *lowerer) allocBanks(t *values.Type, banks values.Repr) vreg {
	var n int
	switch banks {
	case values.Raw:
		n = f.r
		f.r++
	case values.Pointer:
		n = f.p
		f.p++
	case values.Pair:
		n = max(f.r, f.p)
		f.r, f.p = n+1, n+1
	}
	if max(f.r, f.p) > 65536 {
		f.c.fail(f.pos, "too many registers")
	}
	return vreg{uint16(n), t}
}
func (f *lowerer) register(v ssa.Value) {
	if _, ok := f.regs[v]; ok {
		return
	}
	if extract, ok := v.(*ssa.Extract); ok {
		f.register(extract.Tuple)
		if regs := f.regs[extract.Tuple]; regs != nil {
			f.regs[v] = regs[extract.Index : extract.Index+1]
		}
		return
	}
	if f.c.deferValues[v] {
		f.regs[v] = []vreg{f.alloc(deferStackType)}
		return
	}
	if _, ok := v.(*ssa.Range); ok {
		f.regs[v] = []vreg{f.alloc(iteratorType)}
		return
	}
	// SSA may give separate names to equal literals. Immutable scalar and
	// string constants of the same Go type share one prologue register.
	var key literalKey
	if c, ok := v.(*ssa.Const); f.optimizing() && ok && c.Value != nil && c.Value.Kind() != constant.Complex {
		key.typ = c.Type()
		if c.Value.Kind() == constant.String {
			key.value = constant.StringVal(c.Value)
		} else {
			key.value = c.Value.ExactString()
		}
		if regs := f.literals[key]; regs != nil {
			f.regs[v] = regs
			return
		}
	}
	var ts []types.Type
	if tuple, ok := v.Type().(*types.Tuple); ok {
		for i := 0; i < tuple.Len(); i++ {
			t := tuple.At(i).Type()
			if t == types.Typ[types.Invalid] {
				if next, ok := v.(*ssa.Next); ok && !next.IsString {
					m := next.Iter.(*ssa.Range).X.Type().Underlying().(*types.Map)
					if i == 1 {
						t = m.Key()
					} else {
						t = m.Elem()
					}
				} else {
					t = types.Typ[types.Int]
				}
			}
			ts = append(ts, t)
		}
	} else {
		ts = []types.Type{v.Type()}
	}
	var regs []vreg
	for _, t := range ts {
		vt, err := f.c.env.valueType(t)
		if err != nil {
			f.c.fail(v.Pos(), "%v", err)
			return
		}
		banks := vt.Repr()
		if f.optimizing() && banks == values.Raw {
			if slot, ok := f.scalarReturnSlot(v); ok {
				regs = append(regs, vreg{slot, vt})
				continue
			}
		}
		if call, ok := v.(*ssa.Call); ok {
			if j, tied := f.directOpcode(call).TiedHostArgument(); tied {
				input := call.Call.Args[j]
				src := f.regs[input]
				if len(src) == 0 {
					f.register(input)
					src = f.regs[input]
				}
				banks |= src[0].typ.Repr()
				// A parameter is fresh on entry. Inside a loop, only an
				// instruction that runs again in this block is fresh. Phi
				// and tuple slots can also have aliases under other SSA names.
				fresh := false
				switch def := input.(type) {
				case *ssa.Parameter:
					fresh = call.Block() == f.fn.Blocks[0]
				case *ssa.Phi, *ssa.Extract:
				case ssa.Instruction:
					fresh = def.Block() == call.Block()
				}
				if fresh && len(*input.Referrers()) == 1 && banks == src[0].typ.Repr() {
					regs = append(regs, vreg{src[0].slot, vt})
					continue
				}
			}
		}
		regs = append(regs, f.allocBanks(vt, banks))
	}
	f.regs[v] = regs
	if fn, ok := v.(*ssa.Function); ok {
		f.functionValues = append(f.functionValues, fn)
	}
	if g, ok := v.(*ssa.Global); ok {
		f.globals = append(f.globals, g)
	}
	if c, ok := v.(*ssa.Const); ok {
		f.constants = append(f.constants, c)
		if key.typ != nil {
			if f.literals == nil {
				f.literals = make(map[literalKey][]vreg)
			}
			f.literals[key] = regs
		}
	}
}
func (f *lowerer) emit(ins evm.Instruction) {
	f.code.Code = append(f.code.Code, ins)
	var pos evm.SourcePosition
	if f.pos.IsValid() {
		pos = f.c.position(f.pos)
	}
	f.code.Positions = append(f.code.Positions, pos)
}
func (f *lowerer) move(to, from vreg) {
	if to == from {
		return
	}
	switch to.typ.Repr() {
	case values.Raw:
		f.emit(evm.ABC(evm.OpMoveR, to.slot, from.slot, 0))
	case values.Pointer:
		f.emit(evm.ABC(evm.OpMoveP, to.slot, from.slot, 0))
	case values.Pair:
		f.emit(evm.ABC(evm.OpMoveV, to.slot, from.slot, 0))
	}
}

// On a back edge, a scalar update may write straight to its loop variable.
// Only reuse it after the last read of the old value, including reads by
// other phis on the same edge. The new value must not escape this block.
func (f *lowerer) reuseLoopSlots() {
	for _, b := range f.fn.Blocks {
		if len(b.Succs) != 1 || !b.Succs[0].Dominates(b) {
			continue
		}
		header := b.Succs[0]
		pred := slices.Index(header.Preds, b)
		lastRead := map[ssa.Value]int{}
		index := map[ssa.Instruction]int{}
		var operands []*ssa.Value
		// A loop body can branch to an early exit before reaching the latch.
		// Include that body only when its sole entry is the loop header.
		blocks := []*ssa.BasicBlock{b}
		if len(b.Preds) == 1 {
			body := b.Preds[0]
			if body != header && len(body.Preds) == 1 && body.Preds[0] == header {
				blocks = []*ssa.BasicBlock{body, b}
			}
		}
		n := 0
		for _, block := range blocks {
			for _, ins := range block.Instrs {
				index[ins] = n
				operands = ins.Operands(operands[:0])
				for _, v := range operands {
					if v != nil {
						lastRead[*v] = n
					}
				}
				n++
			}
		}
		for _, ins := range header.Instrs {
			phi, ok := ins.(*ssa.Phi)
			if !ok {
				break
			}
			lastRead[phi.Edges[pred]] = n
		}
		for _, ins := range header.Instrs {
			phi, ok := ins.(*ssa.Phi)
			if !ok {
				break
			}
			// A conditional update joins the old/new values in a latch phi.
			// It may reuse the header slot once all old-value reads are done.
			if join, ok := phi.Edges[pred].(*ssa.Phi); ok && b != header && join.Block() == b &&
				len(*join.Referrers()) == 1 && f.regs[join][0].typ.Repr() == values.Raw && lastRead[phi] < n {
				live := false
				for _, use := range *phi.Referrers() {
					if _, edge := use.(*ssa.Phi); !edge && use.Block() == b {
						live = true
						break
					}
				}
				if !live {
					f.regs[join] = f.regs[phi]
				}
				continue
			}
			v, ok := phi.Edges[pred].(*ssa.BinOp)
			if !ok || !slices.Contains(blocks, v.Block()) || f.regs[v][0].typ.Repr() != values.Raw || lastRead[phi] > index[v] {
				continue
			}
			switch v.Op {
			case token.ADD, token.SUB, token.MUL, token.QUO, token.REM, token.AND, token.OR, token.XOR:
			default:
				continue
			}
			local := true
			if v.Block() != b {
				// The early exit can still see the old value, including as a
				// phi input on a different incoming edge of the next iteration.
				for _, use := range *phi.Referrers() {
					if _, edge := use.(*ssa.Phi); edge || use.Block() != header && !slices.Contains(blocks, use.Block()) {
						local = false
						break
					}
				}
			}
			for _, use := range *v.Referrers() {
				if use == phi {
					continue
				}
				if _, edge := use.(*ssa.Phi); edge || use.Block() != v.Block() {
					local = false
					break
				}
			}
			if local {
				f.regs[v] = f.regs[phi]
			}
		}
	}
}

// An edge or return assigns all its destinations simultaneously. Save a
// source only if an earlier destination in this schedule overwrites its bank.
func (f *lowerer) parallelCopy(to, from []vreg) {
	if len(to) > 1 {
		written := make(map[uint16]values.Repr)
		for i, src := range from {
			if src.slot == to[i].slot {
				continue
			}
			banks := src.typ.Repr()
			if written[src.slot]&banks != 0 {
				tmp := f.alloc(src.typ)
				f.move(tmp, src)
				from[i] = tmp
			}
			dst := to[i]
			written[dst.slot] |= dst.typ.Repr()
		}
	}
	for i, dst := range to {
		if dst.slot != from[i].slot {
			f.move(dst, from[i])
		}
	}
}

func (f *lowerer) copyPhis(from, to *ssa.BasicBlock) {
	if _, ok := to.Instrs[0].(*ssa.Phi); !ok {
		return
	}
	pred := -1
	for i, p := range to.Preds {
		if p == from {
			pred = i
			break
		}
	}
	var dst, src []vreg
	for _, i := range to.Instrs {
		phi, ok := i.(*ssa.Phi)
		if !ok {
			break
		}
		dst = append(dst, f.regs[phi][0])
		src = append(src, f.regs[phi.Edges[pred]][0])
	}
	f.parallelCopy(dst, src)
}

func (f *lowerer) literal(x uint64) vreg {
	r := f.alloc(values.Uint64)
	f.emit(evm.WithBC(evm.OpConst, r.slot, f.c.word(x)))
	return r
}
func (f *lowerer) loadConstant(c *ssa.Const, r vreg) {
	if c.Value == nil {
		if r.typ.Repr() != values.Raw {
			f.emit(evm.ABC(evm.OpClearP, r.slot, 0, 0))
		}
		if r.typ.Repr() != values.Pointer {
			f.emit(evm.WithBC(evm.OpConst, r.slot, f.c.word(0)))
		}
		return
	}
	if f.shape(c.Type()).Kind == values.ComplexKind {
		f.complexConstant(c, r)
		return
	}
	var x uint64
	switch c.Value.Kind() {
	case constant.Complex:
		f.complexConstant(c, r)
		return
	case constant.Bool:
		if constant.BoolVal(c.Value) {
			x = 1
		}
	case constant.String:
		f.emit(evm.WithBC(evm.OpString, r.slot, f.c.string(constant.StringVal(c.Value))))
		return
	case constant.Int:
		if n, ok := constant.Int64Val(c.Value); ok {
			x = uint64(n)
		} else {
			x, _ = constant.Uint64Val(c.Value)
		}
		if n, _ := numericType(c.Type()); n.Kind == evm.NumericFloat {
			v, _ := constant.Float64Val(c.Value)
			if n.Bits == 32 {
				v32, _ := constant.Float32Val(c.Value)
				x = uint64(math.Float32bits(v32))
			} else {
				x = math.Float64bits(v)
			}
		}
	case constant.Float:
		v, _ := constant.Float64Val(c.Value)
		n, _ := numericType(c.Type())
		if n.Bits == 32 {
			v32, _ := constant.Float32Val(c.Value)
			x = uint64(math.Float32bits(v32))
		} else {
			x = math.Float64bits(v)
		}
	default:
		f.c.fail(f.pos, "constant %s is not implemented yet", c)
		return
	}
	f.emit(evm.WithBC(evm.OpConst, r.slot, f.c.word(x)))
}
func (f *lowerer) jump(op evm.Opcode, reg uint16, target int) {
	f.branches = append(f.branches, branchPatch{len(f.code.Code), target})
	f.emit(evm.WithBC(op, reg, 0))
}
func (f *lowerer) edge(from, to *ssa.BasicBlock) int {
	if len(to.Instrs) == 0 {
		return to.Index
	}
	if _, ok := to.Instrs[0].(*ssa.Phi); !ok {
		return to.Index
	}
	pred := slices.Index(to.Preds, from)
	for _, ins := range to.Instrs {
		phi, ok := ins.(*ssa.Phi)
		if !ok {
			break
		}
		if f.regs[phi][0].slot != f.regs[phi.Edges[pred]][0].slot {
			label := len(f.fn.Blocks) + len(f.edges)
			f.edges = append(f.edges, flowEdge{from, to, label})
			return label
		}
	}
	return to.Index
}
func (f *lowerer) instruction(i ssa.Instruction) {
	if len(f.c.env.recordMethods) > 0 && f.recordInstruction(i) {
		return
	}
	if f.deferInstruction(i) {
		return
	}
	if f.extendedInstruction(i) {
		return
	}
	switch i := i.(type) {
	case *ssa.Phi, *ssa.DebugRef:
	case *ssa.BinOp:
		f.binary(i)
	case *ssa.UnOp:
		f.unary(i)
	case *ssa.Convert:
		f.convert(f.regs[i][0], f.regs[i.X][0], i.X.Type(), i.Type())
	case *ssa.MakeInterface:
		f.makeInterface(i)
	case *ssa.TypeAssert:
		f.typeAssert(i)
	case *ssa.ChangeInterface:
		f.move(f.regs[i][0], f.regs[i.X][0])
	case *ssa.ChangeType:
		f.move(f.regs[i][0], f.regs[i.X][0])
	case *ssa.Extract: // Already names its component register.
	case *ssa.Jump:
		b := i.Block()
		to := b.Succs[0]
		pos := f.pos
		for _, bin := range f.onEntry[b] {
			f.pos = bin.Pos()
			f.binary(bin)
		}
		f.pos = pos
		f.copyPhis(b, to)
		if !(f.optimizing() && f.fuseLoopBackedge(b, to)) && to.Index != b.Index+1 {
			f.jump(evm.OpJump, 0, to.Index)
		}
	case *ssa.If:
		b := i.Block()
		yes, no := f.edge(b, b.Succs[0]), f.edge(b, b.Succs[1])
		if neg, ok := i.Cond.(*ssa.BinOp); f.optimizing() && ok && branchUsesInequality(neg) {
			yes, no = no, yes
		}
		next := b.Index + 1
		if next < len(f.fn.Blocks) && no == next {
			f.jumpIf(i, evm.OpJnz, yes)
		} else {
			f.jumpIf(i, evm.OpJz, no)
			if next == len(f.fn.Blocks) || yes != next {
				f.jump(evm.OpJump, 0, yes)
			}
		}
	case *ssa.Return:
		// Range-body defers belong to this source function, but x/tools can omit
		// RunDefers in its normal return path. Return operands are already loaded,
		// so drain then reuse the canonical result-cell reload block.
		if f.c.ownsDeferStack[f.fn] && i.Block() != f.fn.Recover {
			if f.fn.Recover == nil {
				f.c.fail(i.Pos(), "defer stack owner has no result reload block")
				return
			}
			f.emit(evm.ABC(evm.OpRunDefers, 0, 0, 0))
			f.jump(evm.OpJump, 0, f.fn.Recover.Index)
			return
		}
		var to, from []vreg
		for j, v := range i.Results {
			to = append(to, vreg{f.result.Slots[j], f.code.Signature.Results[j]})
			from = append(from, f.regs[v][0])
		}
		f.parallelCopy(to, from)
		f.emit(evm.ABC(evm.OpReturn, 0, 0, 0))
	case *ssa.MakeClosure:
		f.makeClosure(i)
	case *ssa.Call:
		f.emitCall(i)
	default:
		f.c.fail(i.Pos(), "operation %T is not implemented yet", i)
	}
}
func (f *lowerer) outMove(slot uint16, r vreg, read bool) {
	for _, op := range []evm.Opcode{evm.OpMoveR, evm.OpMoveP} {
		if op == evm.OpMoveR && r.typ.Repr() == values.Pointer || op == evm.OpMoveP && r.typ.Repr() == values.Raw {
			continue
		}
		a, b := slot, r.slot
		if read {
			a, b = b, a
		}
		f.outgoing = append(f.outgoing, outgoingPatch{len(f.code.Code), read})
		f.emit(evm.ABC(op, a, b, 0))
	}
}
