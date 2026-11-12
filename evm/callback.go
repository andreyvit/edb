package evm

import (
	"unsafe"

	"github.com/andreyvit/edb/values"
)

func (vm *VM) loadCaptures(f frame, fn *linkedFunction, env unsafe.Pointer) {
	if len(fn.Captures) == 0 {
		return
	}
	captures := values.CellSlice(env, uint64(len(fn.Captures)))
	for i, c := range fn.Captures {
		vm.write(f, Operand{c.Slot, c.Type.Repr()}, captures[i])
	}
}

// RunCallback runs a borrowed func() on the native call's goroutine. Its
// registers are separate so Go can keep using the original Call window.
// Globals, captured cells, transaction scopes and all budgets stay shared.
func (c *Call) RunCallback(v values.Value) (err error) {
	if v.R == 0 {
		return ErrNilFunction
	}
	if v.R > uint64(len(c.program.functionRefs)) {
		return ErrFunction
	}
	ref := c.program.functionRefs[v.R-1]
	sig, ok := c.program.functionSignature(ref)
	if !ok || len(sig.Params) != 0 || len(sig.Results) != 0 {
		return ErrFunction
	}
	b := c.budget
	if err := b.check(b.remaining); err != nil {
		return err
	}
	parent := b.limits
	if depth := len(c.vm.stack) + 1; depth < parent.Depth {
		b.limits.Depth -= depth
	} else {
		return ErrDepth
	}
	b.limits.RSlots -= c.usedR
	b.limits.PSlots -= c.usedP
	defer func() {
		b.limits = parent
		c.Context = b.ctx
		if err != nil {
			b.err = err
		}
	}()
	if err := charge(uint64(unsafe.Sizeof(VM{})), &b.allocated, b.limits); err != nil {
		return err
	}
	vm := &VM{program: c.program, globals: c.vm.globals, Output: c.output, deferGeneration: 1}
	vm.call = Call{vm: vm, program: c.program, budget: b, Context: b.ctx, Position: c.Position, output: c.output}
	if ref.Native {
		if v.P != nil {
			return ErrFunction
		}
		g := &c.program.natives[ref.Index]
		vm.call.native = g
		if err := g.Call(&vm.call); err != nil {
			return err
		}
		return b.check(b.remaining)
	}
	if len(c.program.functions[ref.Index].Captures) != 0 && v.P == nil {
		return ErrFunction
	}
	return vm.execute(int(ref.Index), v.P, nil, nil, b)
}
