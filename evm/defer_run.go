package evm

import (
	"github.com/andreyvit/edb/values"
	"math"
	"unsafe"
)

type deferStack struct {
	top    uint32
	active bool
}
type deferredCall struct {
	previous     uint32
	kind         DeferKind
	target       FunctionRef
	env          unsafe.Pointer
	nilFunction  bool
	start, count int
	origin       frame
}

func (vm *VM) ownDeferStack(cur *frame, budget *executionBudget) (uint64, error) {
	if cur.deferStack != 0 {
		return cur.deferStack, nil
	}
	if uint64(len(vm.deferStacks)) >= math.MaxUint32 {
		return 0, ErrAllocation
	}
	if err := charge(2*uint64(unsafe.Sizeof(deferStack{})), &budget.allocated, budget.limits); err != nil {
		return 0, err
	}
	vm.deferStacks = append(vm.deferStacks, deferStack{active: true})
	cur.deferStack = uint64(vm.deferGeneration)<<32 | uint64(len(vm.deferStacks))
	return cur.deferStack, nil
}
func (vm *VM) deferStackIndex(handle uint64) (int, error) {
	idx := uint32(handle)
	if uint32(handle>>32) != vm.deferGeneration || idx == 0 || uint64(idx) > uint64(len(vm.deferStacks)) || !vm.deferStacks[idx-1].active {
		return 0, ErrDeferStack
	}
	return int(idx - 1), nil
}
func (vm *VM) closeDeferStack(cur frame) {
	if cur.deferStack != 0 {
		if i, err := vm.deferStackIndex(cur.deferStack); err == nil {
			vm.deferStacks[i].active = false
		}
	}
}
func (vm *VM) resolveFunction(v values.Value, signature uint32) (FunctionRef, unsafe.Pointer, error) {
	if v.R == 0 {
		return FunctionRef{}, nil, ErrNilFunction
	}
	if v.R > uint64(len(vm.program.functionRefs)) {
		return FunctionRef{}, nil, ErrFunction
	}
	ref := vm.program.functionRefs[v.R-1]
	sig, _ := vm.program.functionSignature(ref)
	if !sameSignature(sig, vm.program.signatures[signature].Signature) || ref.Native && v.P != nil {
		return ref, nil, ErrFunction
	}
	if !ref.Native && len(vm.program.functions[ref.Index].Captures) > 0 && v.P == nil {
		return ref, nil, ErrFunction
	}
	return ref, v.P, nil
}
func (vm *VM) resolveInvoke(v values.Value, desc *linkedInvoke) (FunctionRef, values.Value, error) {
	id := vm.program.typeShapes.Index(v)
	if id == 0 {
		return FunctionRef{}, values.Value{}, ErrNil
	}
	if id >= uint64(len(vm.program.types)) {
		return FunctionRef{}, values.Value{}, ErrFunction
	}
	d := &vm.program.types[id]
	method, ok := d.methods[desc.methodID]
	if !ok || !matchesMethod(method, desc.Method) {
		return FunctionRef{}, values.Value{}, ErrFunction
	}
	_, receiver, err := vm.program.typeShapes.Unpack(v)
	return method.Function, receiver, err
}
func (vm *VM) callLayouts(ref FunctionRef) (Signature, Layout, Layout) {
	if ref.Native {
		g := &vm.program.natives[ref.Index]
		return g.Signature, g.params, g.results
	}
	g := &vm.program.functions[ref.Index]
	return g.Signature, g.params, g.results
}
func (vm *VM) enterCode(cur *frame, ref FunctionRef, env unsafe.Pointer, recoverToken uint32, budget *executionBudget) error {
	if len(vm.stack)+1 >= budget.limits.Depth {
		return ErrDepth
	}
	g := &vm.program.functions[ref.Index]
	f := &vm.program.functions[cur.function]
	if len(g.Captures) > 0 && env == nil {
		return ErrFunction
	}
	next := frame{function: int(ref.Index), rb: cur.rb + f.RCount, pb: cur.pb + f.PCount, recoverToken: recoverToken}
	rEnd, pEnd := next.rb+g.rSize, next.pb+g.pSize
	if rEnd > min(len(vm.r), budget.limits.RSlots) || pEnd > min(len(vm.p), budget.limits.PSlots) {
		if err := vm.grow(rEnd, pEnd, budget); err != nil {
			return err
		}
	}
	if !g.definedRaw {
		clear(vm.r[next.rb+g.params.RCount : rEnd])
	}
	clear(vm.p[next.pb+g.params.PCount : pEnd])
	vm.loadCaptures(next, g, env)
	size := len(vm.stack)
	if size == cap(vm.stack) {
		var err error
		vm.stack, err = growBuffer(vm.stack, size+1, budget.limits.Depth, budget)
		if err != nil {
			return err
		}
	} else {
		vm.stack = vm.stack[:size+1]
	}
	vm.stack[size] = *cur
	*cur = next
	return nil
}
func (vm *VM) registerDefer(cur *frame, d *DeferOp, stackSlot uint16, budget *executionBudget) error {
	handle := uint64(0)
	var err error
	if d.ExternalStack {
		handle = vm.r[cur.rb+int(stackSlot)]
	} else {
		handle, err = vm.ownDeferStack(cur, budget)
		if err != nil {
			return err
		}
	}
	stackIndex, err := vm.deferStackIndex(handle)
	if err != nil {
		return err
	}
	record := deferredCall{kind: d.Kind, target: d.Target, start: len(vm.deferArgs), count: len(d.Args), origin: *cur}
	var receiver values.Value
	switch d.Kind {
	case DeferIndirect:
		v := vm.read(*cur, d.Function)
		if v.R == 0 {
			record.nilFunction = true
		} else {
			record.target, record.env, err = vm.resolveFunction(v, d.Signature)
			if err != nil {
				return err
			}
		}
	case DeferInvoke:
		op := &vm.program.invokes[d.Invoke]
		record.target, receiver, err = vm.resolveInvoke(vm.read(*cur, Operand{d.Receiver, values.Pair}), op)
		if err != nil {
			return err
		}
		record.count++
	}
	if uint64(len(vm.deferred)) >= math.MaxUint32 {
		return ErrAllocation
	}
	if err := charge(2*(uint64(record.count)*uint64(unsafe.Sizeof(values.Value{}))+uint64(unsafe.Sizeof(deferredCall{}))), &budget.allocated, budget.limits); err != nil {
		return err
	}
	if d.Kind == DeferInvoke {
		vm.deferArgs = append(vm.deferArgs, receiver)
	}
	for _, a := range d.Args {
		vm.deferArgs = append(vm.deferArgs, vm.read(*cur, a))
	}
	record.previous = vm.deferStacks[stackIndex].top
	vm.deferred = append(vm.deferred, record)
	vm.deferStacks[stackIndex].top = uint32(len(vm.deferred))
	return nil
}

// drain advances continuations until user bytecode can run again. It never
// recursively executes Go/VM code and never uses public Run for deferred calls.
func (vm *VM) drain(cur *frame, budget *executionBudget) error {
	for cur.mode != drainNone {
		if err := budget.contextErr(); err != nil {
			return vm.trap(*cur, err)
		}
		top := uint32(0)
		stackIndex := -1
		if cur.deferStack != 0 {
			var err error
			stackIndex, err = vm.deferStackIndex(cur.deferStack)
			if err != nil {
				return vm.trap(*cur, err)
			}
			top = vm.deferStacks[stackIndex].top
		}
		if top != 0 {
			// Copy before clearing the arena slot; child execution can grow the arenas.
			d := vm.deferred[top-1]
			vm.deferred[top-1] = deferredCall{}
			vm.deferStacks[stackIndex].top = d.previous
			args := vm.deferArgs[d.start : d.start+d.count]
			if d.kind == DeferRecover {
				clear(args)
				continue
			}
			if d.kind == DeferPanic || d.nilFunction {
				value := values.Value{}
				var cause error
				if d.nilFunction {
					cause = ErrNilFunction
				} else {
					value = args[0]
				}
				clear(args)
				if err := vm.beginPanic(cur, d.origin, value, cause, budget); err != nil {
					return err
				}
				continue
			}
			sig, params, results := vm.callLayouts(d.target)
			f := &vm.program.functions[cur.function]
			next := frame{rb: cur.rb + f.RCount, pb: cur.pb + f.PCount}
			if err := vm.grow(next.rb+max(params.RCount, results.RCount), next.pb+max(params.PCount, results.PCount), budget); err != nil {
				return vm.trap(d.origin, err)
			}
			for i, arg := range args {
				vm.write(next, Operand{params.Slots[i], sig.Params[i].Repr()}, arg)
			}
			clear(args)
			if d.target.Native {
				if err := vm.invokeNative(cur, d.target.Index, budget, vm.program.functions[d.origin.function].position(d.origin.pc-1), false); err != nil {
					if _, ok := err.(*standardPanic); ok {
						if err := vm.beginPanic(cur, d.origin, values.Value{}, err, budget); err != nil {
							return err
						}
						continue
					}
					return vm.trap(d.origin, err)
				}
				continue
			}
			token := uint32(0)
			if cur.mode == drainPanic && !vm.panics[cur.panicID-1].recovered && !vm.panics[cur.panicID-1].aborted {
				token = cur.panicID
			}
			if err := vm.enterCode(cur, d.target, d.env, token, budget); err != nil {
				return vm.trap(d.origin, err)
			}
			return nil
		}
		if cur.mode == drainNormal {
			cur.mode = drainNone
			return nil
		}
		panicID := cur.panicID
		p := &vm.panics[panicID-1]
		if p.recovered && !p.aborted {
			f := &vm.program.functions[cur.function]
			if f.RecoverPC == 0 {
				return vm.trap(*cur, ErrFunction)
			}
			cur.mode = drainNone
			cur.pc = int(f.RecoverPC) - 1
			cur.panicID = 0
			return nil
		}
		vm.closeDeferStack(*cur)
		f := &vm.program.functions[cur.function]
		clear(vm.p[cur.pb : cur.pb+f.pSize])
		if len(vm.stack) == 0 {
			return vm.trap(p.origin, p.cause)
		}
		parent := vm.stack[len(vm.stack)-1]
		vm.stack = vm.stack[:len(vm.stack)-1]
		if parent.mode == drainPanic && parent.panicID != 0 {
			vm.panics[parent.panicID-1].aborted = true
		}
		parent.mode = drainPanic
		parent.panicID = panicID
		*cur = parent
	}
	return nil
}
