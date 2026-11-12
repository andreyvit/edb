package evmcompiler

import (
	"fmt"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"go/token"
	"golang.org/x/tools/go/ssa"
)

var deferStackType = values.NewType("defer-stack handle", values.Raw)

// x/tools uses an opaque pseudo-type for defer stacks. Track generated values
// by provenance; never ask go/types or typeutil to inspect that pseudo-type.
func (c *typedCompiler) markDeferValues(functions []*ssa.Function) {
	c.deferValues = make(map[ssa.Value]bool)
	c.ownsDeferStack = make(map[*ssa.Function]bool)
	for _, fn := range functions {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if call, ok := ins.(*ssa.Call); ok {
					if builtin, ok := call.Call.Value.(*ssa.Builtin); ok && builtin.Name() == "ssa:deferstack" {
						c.deferValues[call] = true
						c.ownsDeferStack[fn] = true
					}
				}
			}
		}
	}
	changed := true
	for changed {
		changed = false
		mark := func(v ssa.Value) {
			if !c.deferValues[v] {
				c.deferValues[v] = true
				changed = true
			}
		}
		for _, fn := range functions {
			for _, b := range fn.Blocks {
				for _, ins := range b.Instrs {
					switch i := ins.(type) {
					case *ssa.Store:
						if c.deferValues[i.Val] {
							mark(i.Addr)
						}
					case *ssa.UnOp:
						if i.Op == token.MUL && c.deferValues[i.X] {
							mark(i)
						}
					case *ssa.Phi:
						for _, e := range i.Edges {
							if c.deferValues[e] {
								mark(i)
							}
						}
					case *ssa.MakeClosure:
						target := i.Fn.(*ssa.Function)
						for n, v := range i.Bindings {
							if c.deferValues[v] {
								mark(target.FreeVars[n])
							}
						}
					}
				}
			}
		}
	}
	for _, fn := range functions {
		for _, b := range fn.Blocks {
			for _, ins := range b.Instrs {
				if p, ok := ins.(*ssa.Phi); ok && c.deferValues[p] {
					for _, e := range p.Edges {
						if !c.deferValues[e] {
							c.fail(p.Pos(), "mixed defer stack provenance")
						}
					}
				}
			}
		}
	}
}
func (f *lowerer) deferInstruction(i ssa.Instruction) bool {
	switch i := i.(type) {
	case *ssa.Alloc:
		if f.c.deferValues[i] {
			return true
		}
	case *ssa.Store:
		if f.c.deferValues[i.Addr] {
			if !f.c.deferValues[i.Val] {
				f.c.fail(i.Pos(), "invalid defer stack store")
			} else {
				f.move(f.regs[i.Addr][0], f.regs[i.Val][0])
			}
			return true
		}
	case *ssa.UnOp:
		if f.c.deferValues[i] {
			f.move(f.regs[i][0], f.regs[i.X][0])
			return true
		}
	case *ssa.RunDefers:
		f.emit(evm.ABC(evm.OpRunDefers, 0, 0, 0))
		return true
	case *ssa.Panic:
		f.emit(evm.ABC(evm.OpPanic, f.regs[i.X][0].slot, 0, 0))
		return true
	case *ssa.Defer:
		f.deferCall(i)
		return true
	}
	return false
}
func (f *lowerer) deferCall(i *ssa.Defer) {
	common := &i.Call
	d := evm.DeferOp{}
	var a uint16
	if i.DeferStack != nil {
		if !f.c.deferValues[i.DeferStack] {
			f.c.fail(i.Pos(), "invalid defer stack provenance")
			return
		}
		d.ExternalStack = true
		a = f.regs[i.DeferStack][0].slot
	}
	for _, arg := range common.Args {
		d.Args = append(d.Args, operand(f.regs[arg][0]))
	}
	if common.IsInvoke() {
		d.Receiver = f.regs[common.Value][0].slot
		d.Kind = evm.DeferInvoke
		var ok bool
		d.Invoke, _, _, ok = f.invokeDescriptor(common)
		if !ok {
			return
		}
	} else {
		if b, ok := common.Value.(*ssa.Builtin); ok {
			switch b.Name() {
			case "panic":
				d.Kind = evm.DeferPanic
			case "recover":
				d.Kind = evm.DeferRecover
			default:
				idx, ok := f.c.deferBuiltins[i]
				if !ok {
					f.c.fail(i.Pos(), "deferred builtin %s is not implemented yet", b.Name())
					return
				}
				d.Kind = evm.DeferDirect
				d.Target = idx
			}
		} else if fn, ok := common.Value.(*ssa.Function); ok {
			d.Kind = evm.DeferDirect
			var ok bool
			d.Target, _, ok = f.c.target(fn, i.Pos())
			if !ok {
				return
			}
		} else {
			d.Kind = evm.DeferIndirect
			d.Function = operand(f.regs[common.Value][0])
			var ok bool
			d.Signature, _, ok = f.indirectSignature(common.Signature())
			if !ok {
				return
			}
		}
	}
	idx := uint32(len(f.c.module.Defers))
	f.c.module.Defers = append(f.c.module.Defers, d)
	f.emit(evm.WithBC(evm.OpDefer, a, idx))
}
func (c *typedCompiler) prepareDeferBuiltins(functions []*ssa.Function) {
	c.deferBuiltins = make(map[*ssa.Defer]evm.FunctionRef)
	for _, fn := range functions {
		for _, block := range fn.Blocks {
			for _, ins := range block.Instrs {
				d, ok := ins.(*ssa.Defer)
				if !ok {
					continue
				}
				b, ok := d.Call.Value.(*ssa.Builtin)
				if !ok {
					continue
				}
				if b.Name() == "print" || b.Name() == "println" {
					ref, _ := c.debugPrintImport(d.Call.Args, b.Name() == "println", d.Pos())
					c.deferBuiltins[d] = ref
					continue
				}
				var kind evm.ExtendedKind
				switch b.Name() {
				case "clear":
					kind = evm.ExtendedClear
				case "delete":
					kind = evm.ExtendedDelete
				case "copy":
					kind = evm.ExtendedCopy
				default:
					continue
				}
				var sig evm.Signature
				var shape *values.Shape
				for i, arg := range d.Call.Args {
					s, err := c.env.shape(arg.Type())
					if err != nil {
						c.fail(d.Pos(), "%v", err)
						return
					}
					if i == 0 {
						shape = s
					}
					sig.Params = append(sig.Params, s.Type)
				}
				layout, err := evm.LayoutOf(sig.Params)
				if err != nil {
					c.fail(d.Pos(), "%v", err)
					return
				}
				op := evm.ExtendedOp{Kind: kind, Shape: shape}
				for i, t := range sig.Params {
					op.In = append(op.In, evm.Operand{Slot: layout.Slots[i], Repr: t.Repr()})
				}
				rcount := layout.RCount
				if kind == evm.ExtendedCopy {
					op.Out = []evm.Operand{{Slot: 0, Repr: values.Raw}}
					rcount = max(rcount, 1)
					src, err := c.env.shape(d.Call.Args[1].Type())
					if err != nil {
						c.fail(d.Pos(), "%v", err)
						return
					}
					if src.Kind == values.StringKind {
						op.Flags = 1
					}
				}
				opidx := uint32(len(c.module.ExtendedOps))
				c.module.ExtendedOps = append(c.module.ExtendedOps, op)
				idx := uint32(len(c.module.Functions))
				c.deferBuiltins[d] = evm.FunctionRef{Index: idx}
				c.module.Functions = append(c.module.Functions, evm.Function{Name: "$deferBuiltin" + b.Name() + "$" + fmt.Sprint(idx), Positions: []evm.SourcePosition{c.position(d.Pos()), c.position(d.Pos())}, Internal: true, Position: c.position(d.Pos()), Signature: sig, RCount: rcount, PCount: layout.PCount, Code: []evm.Instruction{evm.WithBC(evm.OpExtended, 0, opidx), evm.ABC(evm.OpReturn, 0, 0, 0)}})
			}
		}
	}
}
