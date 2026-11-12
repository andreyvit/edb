// Package evm implements a register VM for compiled Go programs. Its bytecode
// is trusted input, not a sandbox boundary or a persisted compatibility format.
package evm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

var (
	ErrSteps         = errors.New("instruction limit exceeded")
	ErrDepth         = errors.New("call depth limit exceeded")
	ErrRegisters     = errors.New("register limit exceeded")
	ErrDivideByZero  = errors.New("integer division by zero")
	ErrAllocations   = errors.New("allocation count limit exceeded")
	ErrAllocation    = errors.New("allocation budget exceeded")
	ErrNegativeShift = errors.New("negative shift amount")
	ErrInvalidState  = errors.New("VM session failed; call Reset before reuse")
	ErrNilFunction   = errors.New("call of nil function")
	ErrFunction      = errors.New("invalid function value")
	ErrReentry       = errors.New("VM is already running")
)

type Limits struct {
	Steps                 uint64 // one per dispatched bytecode instruction
	Allocations           uint64
	AllocationBytes       uint64
	OutputBytes           uint64
	Depth, RSlots, PSlots int
}
type drainMode uint8

const (
	drainNone drainMode = iota
	drainNormal
	drainPanic
)

type frame struct {
	function, pc, rb, pb  int
	deferStack            uint64
	panicID, recoverToken uint32
	mode                  drainMode
}

// VM owns reusable execution storage. It is neither concurrent nor reentrant.
// Zero limit fields select defaults; positive fields override them.
type VM struct {
	// Output receives fmt/log/debug output. Nil uses the normal Go logger.
	Output               func(string) error
	deferGeneration      uint32
	deferStacks          []deferStack
	deferred             []deferredCall
	deferArgs            []values.Value
	panics               []panicContext
	budget               executionBudget
	program              *Program
	Limits               Limits
	r                    []uint64
	p                    []unsafe.Pointer
	stack                []frame
	directR              [2]uint64
	directP              [2]unsafe.Pointer
	call                 Call
	running              bool
	globals              []unsafe.Pointer
	initialized, failed  bool
	allocated            allocationUsage
	recordLists          []recordListIterator
	recordListGeneration uint64
}

func New(p *Program) *VM {
	vm := &VM{program: p}
	if p != nil && p.recordListSlots != 0 {
		vm.recordLists = make([]recordListIterator, p.recordListSlots)
	}
	return vm
}

// Budget applies to one RunWithBudget call, including first-use initialization.
// Zero fields inherit VM defaults. Allocation limits also obey the remaining
// session allowance configured by VM.Limits.
type Budget struct {
	Steps, Allocations, AllocationBytes  uint64
	Duration                             time.Duration // soft wall-clock limit for the invocation
	HardDuration                         time.Duration
	HardAllocations, HardAllocationBytes uint64
	UnlimitedSteps                       bool
}

// Run writes results only on success. Value types are the entry signature's
// types, supplied externally; callers must honor them. Reusing a VM and output
// storage avoids allocations after growth. Native panics propagate to the host.
func (vm *VM) Run(ctx context.Context, entry string, args, results []values.Value) error {
	return vm.RunWithBudget(ctx, entry, args, results, Budget{})
}

// RunWithBudget bounds this invocation without changing defaults for later calls.
// Cancel the supplied context to interrupt an active invocation.
func (vm *VM) RunWithBudget(ctx context.Context, entry string, args, results []values.Value, call Budget) error {
	if vm.running {
		return ErrReentry
	}
	if vm.failed {
		return ErrInvalidState
	}
	idx, ok := vm.program.entries[entry]
	if !ok {
		return fmt.Errorf("unknown entry %q", entry)
	}
	f := &vm.program.functions[idx]
	if len(args) != len(f.Signature.Params) || len(results) != len(f.Signature.Results) {
		return fmt.Errorf("%s: argument/result count mismatch", entry)
	}
	limits := vm.Limits
	if limits.AllocationBytes == 0 {
		limits.AllocationBytes = 64 << 20
	}
	if limits.Allocations == 0 {
		limits.Allocations = 1_000_000
	}
	if call.HardDuration > 0 && call.Duration > call.HardDuration || call.HardAllocationBytes > 0 && call.AllocationBytes > call.HardAllocationBytes || call.HardAllocations > 0 && call.Allocations > call.HardAllocations {
		return fmt.Errorf("soft budget exceeds hard budget")
	}
	// Charge this run against both ceilings without a second check at each allocation.
	limits.AllocationBytes -= min(limits.AllocationBytes, vm.allocated.bytes)
	limits.Allocations -= min(limits.Allocations, vm.allocated.count)
	sessionBytes, sessionAllocs := limits.AllocationBytes, limits.Allocations
	if call.AllocationBytes != 0 {
		limits.AllocationBytes = min(limits.AllocationBytes, call.AllocationBytes)
	}
	if call.Allocations != 0 {
		limits.Allocations = min(limits.Allocations, call.Allocations)
	}
	if call.Steps != 0 {
		limits.Steps = call.Steps
	}
	if limits.OutputBytes == 0 {
		limits.OutputBytes = 1 << 20
	}
	if call.UnlimitedSteps {
		limits.Steps = math.MaxUint64
	}
	if limits.Steps == 0 {
		limits.Steps = 1_000_000
	}
	if limits.Depth == 0 {
		limits.Depth = 1024
	}
	if limits.RSlots == 0 {
		limits.RSlots = 1 << 20
	}
	if limits.PSlots == 0 {
		limits.PSlots = 1 << 20
	}
	if limits.Depth < 1 || limits.RSlots < 0 || limits.PSlots < 0 {
		return fmt.Errorf("invalid execution limits")
	}
	if vm.deferGeneration == math.MaxUint32 {
		return fmt.Errorf("defer generation exhausted")
	}
	vm.deferGeneration++
	vm.running = true
	defer func() {
		clear(vm.p)
		vm.call = Call{}
		clear(vm.directP[:])
		vm.stack = vm.stack[:0]
		clear(vm.deferred)
		vm.deferred = vm.deferred[:0]
		clear(vm.deferArgs)
		vm.deferArgs = vm.deferArgs[:0]
		clear(vm.panics)
		vm.panics = vm.panics[:0]
		vm.deferStacks = vm.deferStacks[:0]
		vm.running = false
	}()
	vm.failed = true
	started := time.Now()
	hardDuration := max(call.Duration, call.HardDuration)
	if hardDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, hardDuration)
		defer cancel()
	}
	vm.budget = executionBudget{limits: limits, remaining: limits.Steps, ctx: ctx, canCancel: ctx.Done() != nil}
	budget := &vm.budget
	budget.started, budget.duration, budget.hardDuration = started, call.Duration, hardDuration
	budget.hardBytes = min(max(limits.AllocationBytes, call.HardAllocationBytes), sessionBytes)
	budget.hardAllocs = min(max(limits.Allocations, call.HardAllocations), sessionAllocs)
	vm.call = Call{vm: vm, program: vm.program, budget: budget, Context: ctx, output: vm.Output}
	defer func() {
		vm.allocated.bytes += budget.allocated.bytes
		vm.allocated.count += budget.allocated.count
		for budget.scope != nil {
			budget.scope.release()
		}
		budget.ctx = nil
		budget.records = nil
	}()
	if err := budget.contextErr(); err != nil {
		return err
	}
	if !vm.initialized {
		if err := charge(uint64(len(vm.program.globals))*uint64(unsafe.Sizeof(unsafe.Pointer(nil))), &budget.allocated, limits); err != nil {
			return err
		}
		vm.globals = make([]unsafe.Pointer, len(vm.program.globals))
		for i, s := range vm.program.globals {
			if i&255 == 255 {
				if err := budget.contextErr(); err != nil {
					return err
				}
			}
			var ptr unsafe.Pointer
			var err error
			if s.NativeStorage() {
				err = charge(s.Native.Size, &budget.allocated, limits)
				if err == nil {
					ptr = s.Native.New()
				}
			} else {
				ptr, err = allocCells(s.Cells, &budget.allocated, limits)
			}
			if err != nil {
				return err
			}
			vm.globals[i] = ptr
		}
		if vm.program.init >= 0 {
			if err := vm.execute(vm.program.init, nil, nil, nil, budget); err != nil {
				return err
			}
		}
		vm.initialized = true
	}
	if err := vm.execute(idx, nil, args, results, budget); err != nil {
		return err
	}
	clear(vm.recordLists)
	vm.recordListGeneration = 0
	vm.failed = false
	return nil
}

// Reset discards session globals and initialization state, and releases retained
// pointers. A failed or interrupted session must be reset before it can run again.
func (vm *VM) Reset() error {
	if vm.running {
		return ErrReentry
	}
	clear(vm.p)
	clear(vm.r)
	clear(vm.globals)
	vm.globals = nil
	vm.stack = vm.stack[:0]
	vm.initialized = false
	vm.failed = false
	vm.allocated = allocationUsage{}
	return nil
}

type executionBudget struct {
	started                time.Time
	duration, hardDuration time.Duration
	hardBytes, hardAllocs  uint64
	scope                  *BudgetScope
	canCancel              bool
	records                *RecordScope
	err                    error
	limits                 Limits
	remaining              uint64
	ctx                    context.Context
	allocated              allocationUsage
	output                 uint64
}

// Host cancellation and every active soft deadline bound the invocation.
func (b *executionBudget) contextErr() error {
	if b.duration > 0 || b.scope != nil {
		now := time.Now()
		if b.duration > 0 && now.Sub(b.started) >= b.duration {
			return context.DeadlineExceeded
		}
		for s := b.scope; s != nil; s = s.parent {
			if s.duration > 0 && now.Sub(s.started) >= s.duration {
				return context.DeadlineExceeded
			}
		}
	}
	if b.canCancel {
		return b.ctx.Err()
	}
	return nil
}

func (vm *VM) execute(idx int, env unsafe.Pointer, args, results []values.Value, budget *executionBudget) error {
	limits := budget.limits
	f := &vm.program.functions[idx]
	if err := vm.grow(f.rSize, f.pSize, budget); err != nil {
		return err
	}
	if !f.definedRaw {
		clear(vm.r[:f.rSize])
	}
	clear(vm.p[:f.pSize])
	for i, arg := range args {
		slot := int(f.params.Slots[i])
		switch f.Signature.Params[i].Repr() {
		case values.Raw:
			vm.r[slot] = arg.R
		case values.Pointer:
			vm.p[slot] = arg.P
		case values.Pair:
			vm.r[slot], vm.p[slot] = arg.R, arg.P
		}
	}
	cur := frame{function: idx}
	vm.loadCaptures(cur, f, env)
	var failure error
	// External callbacks share the meter with their suspended caller.
	remaining := budget.remaining
	var r []uint64
	var p []unsafe.Pointer
resumeFrame:
	limits = budget.limits
	f = &vm.program.functions[cur.function]
	r, p = vm.r[cur.rb:], vm.p[cur.pb:]
dispatch:
	for {
		if remaining&255 == 0 {
			if remaining == 0 {
				return vm.trap(cur, ErrSteps)
			}
			budget.remaining = remaining
			if err := budget.check(remaining); err != nil {
				return vm.trap(cur, err)
			}
		}
		remaining--
		ins := f.Code[cur.pc]
		cur.pc++
		a, b, c := int(ins.A()), int(ins.B()), int(ins.C())
		switch ins.Op() {
		case OpDeferStack:
			handle, err := vm.ownDeferStack(&cur, budget)
			if err != nil {
				return vm.trap(cur, err)
			}
			r[a] = handle
		case OpDefer:
			if err := vm.registerDefer(&cur, &vm.program.defers[ins.BC()], ins.A(), budget); err != nil {
				failure = err
				goto fault
			}
		case OpRunDefers:
			cur.mode = drainNormal
			goto drain
		case OpPanic:
			if err := vm.beginPanic(&cur, cur, values.Value{R: r[a], P: p[a]}, nil, budget); err != nil {
				return err
			}
			goto drain
		case OpRecover:
			vm.write(cur, Operand{ins.A(), values.Pair}, vm.recover(cur))
		case OpAlloc:
			cells := vm.program.shapes[b].Cells
			ptr, err := allocCells(cells, &budget.allocated, limits)
			if err != nil {
				return vm.trap(cur, err)
			}
			p[a] = ptr
		case OpGlobal:
			p[a] = vm.globals[ins.BC()]
		case OpLoadIndexByte, OpLoadIndexWord, OpLoadIndexPtr:
			index := r[c]
			if index >= uint64(uint32(r[b])) {
				failure = ErrBounds
				goto fault
			}
			if p[b] == nil {
				failure = ErrNil
				goto fault
			}
			switch ins.Op() {
			case OpLoadIndexByte:
				r[a] = uint64(*(*byte)(unsafe.Add(p[b], index)))
			case OpLoadIndexWord:
				r[a] = *(*uint64)(unsafe.Add(p[b], index*8))
			default:
				p[a] = *(*unsafe.Pointer)(unsafe.Add(p[b], uintptr(index)*unsafe.Sizeof(p[b])))
			}
		case OpStoreIndexByte, OpStoreIndexWord, OpStoreIndexPtr:
			index := r[b]
			if index >= uint64(uint32(r[a])) {
				failure = ErrBounds
				goto fault
			}
			if p[a] == nil {
				failure = ErrNil
				goto fault
			}
			switch ins.Op() {
			case OpStoreIndexByte:
				*(*byte)(unsafe.Add(p[a], index)) = byte(r[c])
			case OpStoreIndexWord:
				*(*uint64)(unsafe.Add(p[a], index*8)) = r[c]
			default:
				*(*unsafe.Pointer)(unsafe.Add(p[a], uintptr(index)*unsafe.Sizeof(p[a]))) = p[c]
			}
		case OpLoadIndexR:
			index := r[c]
			if index >= uint64(uint32(r[b])) {
				failure = ErrBounds
				goto fault
			}
			if p[b] == nil {
				failure = ErrNil
				goto fault
			}
			r[a] = (*values.Value)(values.CellAt(p[b], index)).R
		case OpStoreIndexR:
			index := r[b]
			if index >= uint64(uint32(r[a])) {
				failure = ErrBounds
				goto fault
			}
			if p[a] == nil {
				failure = ErrNil
				goto fault
			}
			*(*values.Value)(values.CellAt(p[a], index)) = values.Value{R: r[c]}
		case OpLoad:
			if p[b] == nil {
				failure = ErrNil
				goto fault
			}
			shape := vm.program.shapes[c]
			v, err := snapshot(shape, shape.Stored(p[b]), &budget.allocated, limits)
			if err != nil {
				return vm.trap(cur, err)
			}
			vm.write(cur, Operand{uint16(a), shape.Type.Repr()}, v)
		case OpStore:
			if p[a] == nil {
				failure = ErrNil
				goto fault
			}
			shape := vm.program.shapes[c]
			shape.Store(p[a], vm.read(cur, Operand{uint16(b), shape.Type.Repr()}))
		case opReadChildPair:
			first, second := &vm.program.extendedOps[ins.BC()], &vm.program.extendedOps[f.Code[cur.pc].BC()]
			if failure = vm.recordOp(cur, first, second, budget); failure != nil {
				goto fault
			}
			cur.pc++
		case OpExtended:
			if ins.A() == floatLoopMarker {
				remaining = vm.floatBackedge(&cur, ins.BC(), remaining)
				break
			}
			op := &vm.program.extendedOps[ins.BC()]
			if op.Kind == ExtendedGetKey || op.Kind == ExtendedSetKey || op.Kind == ExtendedGetChildKey || op.Kind == ExtendedRecordList || op.Kind == ExtendedRecordListLen || op.Kind == ExtendedRecordListIndex {
				failure = vm.recordOp(cur, op, nil, budget)
			} else {
				failure = vm.extended(cur, op, budget)
			}
			if failure != nil {
				goto fault
			}
		case OpConvert:
			r[a] = vm.program.conversions[c].convert(r[b])
		case OpCheckShift:
			if int64(r[a]) < 0 {
				failure = ErrNegativeShift
				goto fault
			}
		case OpConcatStr, OpEqStr, OpLtStr:
			x := (values.Value{R: r[b], P: p[b]}).AsString()
			y := (values.Value{R: r[c], P: p[c]}).AsString()
			switch ins.Op() {
			case OpConcatStr:
				size := uint64(len(x)) + uint64(len(y))
				if err := charge(size, &budget.allocated, limits); err != nil {
					return vm.trap(cur, err)
				}
				v := values.String(x + y)
				r[a], p[a] = v.R, v.P
			case OpEqStr:
				r[a] = boolWord(x == y)
			case OpLtStr:
				r[a] = boolWord(x < y)
			}
		case OpAddF32, OpSubF32, OpMulF32, OpDivF32, OpEqF32, OpLtF32:
			x, y := math.Float32frombits(uint32(r[b])), math.Float32frombits(uint32(r[c]))
			switch ins.Op() {
			case OpAddF32:
				r[a] = uint64(math.Float32bits(x + y))
			case OpSubF32:
				r[a] = uint64(math.Float32bits(x - y))
			case OpMulF32:
				r[a] = uint64(math.Float32bits(x * y))
			case OpDivF32:
				r[a] = uint64(math.Float32bits(x / y))
			case OpEqF32:
				r[a] = boolWord(x == y)
			case OpLtF32:
				r[a] = boolWord(x < y)
			}
		case OpConst:
			r[a] = vm.program.words[ins.BC()]
		case OpString:
			v := vm.program.strings[ins.BC()]
			r[a], p[a] = v.R, v.P
		case OpMoveR:
			r[a] = r[b]
		case OpMoveP:
			p[a] = p[b]
		case OpMoveV:
			r[a], p[a] = r[b], p[b]
		case OpClearP:
			p[a] = nil
		case OpAddImmI64:
			r[a] = r[b] + uint64(int64(int16(c)))
		case OpAddI64:
			r[a] = r[b] + r[c]
		case OpSubI64:
			r[a] = r[b] - r[c]
		case OpMulI64:
			r[a] = r[b] * r[c]
		case OpDivI64, OpDivU64, OpRemI64, OpRemU64:
			x, y := r[b], r[c]
			if y == 0 {
				failure = ErrDivideByZero
				goto fault
			}
			switch ins.Op() {
			case OpDivI64:
				r[a] = uint64(int64(x) / int64(y))
			case OpDivU64:
				r[a] = x / y
			case OpRemI64:
				r[a] = uint64(int64(x) % int64(y))
			case OpRemU64:
				r[a] = x % y
			}
		case OpMulAddF64:
			a, b, c, d := ins.ABCD12()
			r[a] = math.Float64bits(float64(math.Float64frombits(r[b])*math.Float64frombits(r[c])) + math.Float64frombits(r[d]))
		case OpAddMulF64:
			a, b, c, d := ins.ABCD12()
			r[a] = math.Float64bits(math.Float64frombits(r[b]) + float64(math.Float64frombits(r[c])*math.Float64frombits(r[d])))
		case OpSubAddF64:
			a, b, c, d := ins.ABCD12()
			r[a] = math.Float64bits(float64(math.Float64frombits(r[b])-math.Float64frombits(r[c])) + math.Float64frombits(r[d]))
		case OpAddF64:
			r[a] = math.Float64bits(math.Float64frombits(r[b]) + math.Float64frombits(r[c]))
		case OpSubF64:
			r[a] = math.Float64bits(math.Float64frombits(r[b]) - math.Float64frombits(r[c]))
		case OpMulF64:
			r[a] = math.Float64bits(math.Float64frombits(r[b]) * math.Float64frombits(r[c]))
		case OpDivF64:
			r[a] = math.Float64bits(math.Float64frombits(r[b]) / math.Float64frombits(r[c]))
		case OpAnd:
			r[a] = r[b] & r[c]
		case OpOr:
			r[a] = r[b] | r[c]
		case OpXor:
			r[a] = r[b] ^ r[c]
		case OpShl:
			r[a] = r[b] << r[c]
		case OpShrI64:
			r[a] = uint64(int64(r[b]) >> r[c])
		case OpShrU64:
			r[a] = r[b] >> r[c]
		case OpEqR:
			r[a] = boolWord(r[b] == r[c])
		case OpEqP:
			r[a] = boolWord(p[b] == p[c])
		case OpLeI64:
			r[a] = boolWord(int64(r[b]) <= int64(r[c]))
		case OpLeU64:
			r[a] = boolWord(r[b] <= r[c])
		case OpLtI64:
			r[a] = boolWord(int64(r[b]) < int64(r[c]))
		case OpLtU64:
			r[a] = boolWord(r[b] < r[c])
		case OpEqF64:
			r[a] = boolWord(math.Float64frombits(r[b]) == math.Float64frombits(r[c]))
		case OpLtF64:
			r[a] = boolWord(math.Float64frombits(r[b]) < math.Float64frombits(r[c]))
		case OpJNil:
			if p[a] == nil {
				cur.pc += int(int32(ins.BC()))
			}
		case OpJNotNil:
			if p[a] != nil {
				cur.pc += int(int32(ins.BC()))
			}
		case opBranchLtI64:
			r[a] = boolWord(int64(r[b]) < int64(r[c]))
			goto comparisonBranch
		case opBranchLeI64:
			r[a] = boolWord(int64(r[b]) <= int64(r[c]))
			goto comparisonBranch
		case opBranchLtF64:
			r[a] = boolWord(math.Float64frombits(r[b]) < math.Float64frombits(r[c]))
			goto comparisonBranch
		case opBranchEqR:
			r[a] = boolWord(r[b] == r[c])
			goto comparisonBranch
		case OpLoopAddLtI64, OpLoopAddLeI64:
			a, b, c, _ := ins.ABCD12()
			r[a] += r[c]
			x, y := int64(r[a]), int64(r[b])
			if x < y || x == y && ins.Op() == OpLoopAddLeI64 {
				cur.pc += ins.SignedD12()
			}
		case OpLoopLeI64:
			r[a]++
			if int64(r[a]) <= int64(r[b]) {
				cur.pc += int(int16(c))
			}
		case OpLoopLtI64:
			r[a]++
			if int64(r[a]) < int64(r[b]) {
				cur.pc += int(int16(c))
			}
		case OpJump:
			cur.pc += int(int32(ins.BC()))
		case OpJz:
			if r[a] == 0 {
				cur.pc += int(int32(ins.BC()))
			}
		case OpJnz:
			if r[a] != 0 {
				cur.pc += int(int32(ins.BC()))
			}
		case OpFunction:
			r[a] = uint64(ins.BC()) + 1
			p[a] = nil
		case OpClosure:
			desc := vm.program.closures[ins.BC()]
			ptr, err := allocCells(uint64(len(desc.Captures)), &budget.allocated, limits)
			if err != nil {
				return vm.trap(cur, err)
			}
			cells := values.CellSlice(ptr, uint64(len(desc.Captures)))
			for i, r := range desc.Captures {
				cells[i] = vm.read(cur, r)
			}
			r[a], p[a] = uint64(desc.Function)+1, ptr
		case OpCheckNil:
			if p[a] == nil {
				failure = ErrNil
				goto fault
			}
		case opCallRawUnary:
			if err := budget.contextErr(); err != nil {
				return vm.trap(cur, err)
			}
			word := vm.program.natives[c].Direct.word(r[b])
			if err := budget.contextErr(); err != nil {
				return vm.trap(cur, err)
			}
			r[a] = word
		case OpCallHost, OpCallHostRR, OpCallHostRP:
			index := ins.BC()
			if ins.Op() != OpCallHost {
				index = uint32(c)
				r[f.RCount] = r[b]
			}
			budget.remaining = remaining
			failure = vm.invokeNative(&cur, index, budget, f.position(cur.pc-1), ins.Op() != OpCallHostRR)
			remaining = budget.remaining
			limits = budget.limits
			if failure != nil {
				goto nativeFault
			}
			switch ins.Op() {
			case OpCallHostRR:
				r[a] = r[f.RCount]
			case OpCallHostRP:
				p[a] = p[f.PCount]
			}
		case OpCallFunc, OpCallIndirect, OpCallVirt:
			// A direct scalar call can reuse both buffers. The ordinary entry path
			// handles growth, captures, recovery and limit errors.
			if ins.Op() == OpCallFunc && !f.RecoverTransparent {
				idx := int(ins.BC())
				g := &vm.program.functions[idx]
				rb := cur.rb + f.RCount
				end := rb + g.rSize
				depth := len(vm.stack)
				if g.pSize == 0 && len(g.Captures) == 0 && end <= min(len(vm.r), limits.RSlots) &&
					depth < cap(vm.stack) && depth+1 < limits.Depth {
					if !g.definedRaw {
						clear(vm.r[rb+g.params.RCount : end])
					}
					vm.stack = vm.stack[:depth+1]
					vm.stack[depth] = cur
					cur = frame{function: idx, rb: rb, pb: cur.pb + f.PCount}
					r = r[f.RCount:]
					p = p[f.PCount:]
					f = g
					goto dispatch
				}
			}
			ref := FunctionRef{Index: ins.BC()}
			var env unsafe.Pointer
			if ins.Op() == OpCallVirt {
				desc := &vm.program.invokes[ins.BC()]
				rb, pb := f.RCount, f.PCount
				var receiver values.Value
				var err error
				ref, receiver, err = vm.resolveInvoke(values.Value{R: r[rb], P: p[pb]}, desc)
				if err != nil {
					failure = err
					goto fault
				}
				r[rb], p[pb] = receiver.R, receiver.P
			}
			if ins.Op() == OpCallIndirect {
				var err error
				ref, env, err = vm.resolveFunction(values.Value{R: r[a], P: p[a]}, ins.BC())
				if err != nil {
					failure = err
					goto fault
				}
			}

			if ref.Native {
				if env != nil {
					return vm.trap(cur, ErrFunction)
				}
				budget.remaining = remaining
				failure = vm.invokeNative(&cur, ref.Index, budget, f.position(cur.pc-1), false)
				remaining = budget.remaining
				if failure != nil {
					goto nativeFault
				}
				break
			}
			token := uint32(0)
			if f.RecoverTransparent {
				token = cur.recoverToken
			}
			if err := vm.enterCode(&cur, ref, env, token, budget); err != nil {
				return vm.trap(cur, err)
			}
			goto resumeFrame

		case opTestDirect:
			if failure = vm.invokeDirect(&cur, ins, budget); failure != nil {
				goto nativeFault
			}
			ins = f.Code[cur.pc]
			cur.pc++
			var take bool
			switch ins.Op() {
			case OpJz:
				take = r[a] == 0
			case OpJnz:
				take = r[a] != 0
			case OpJNil:
				take = p[a] == nil
			case OpJNotNil:
				take = p[a] != nil
			}
			if take {
				cur.pc += int(int32(ins.BC()))
			}
		case opCallDirect:
			failure = vm.invokeDirect(&cur, ins, budget)
			if failure != nil {
				goto nativeFault
			}
		case OpReturn:
			if cur.deferStack != 0 {
				stackIndex, err := vm.deferStackIndex(cur.deferStack)
				if err != nil {
					return vm.trap(cur, err)
				}
				if vm.deferStacks[stackIndex].top != 0 {
					return vm.trap(cur, ErrFunction)
				}
				vm.deferStacks[stackIndex].active = false
			}
			if len(vm.stack) == 0 {
				if err := budget.check(remaining); err != nil {
					return vm.trap(cur, err)
				}
				if budget.canCancel {
					if err := deadlineErr(budget.ctx); err != nil {
						return vm.trap(cur, err)
					}
				}
				budget.remaining = remaining
				for i, t := range f.Signature.Results {
					slot := int(f.results.Slots[i])
					v := values.Value{}
					if t.Repr() != values.Pointer {
						v.R = vm.r[cur.rb+slot]
					}
					if t.Repr() != values.Raw {
						v.P = vm.p[cur.pb+slot]
					}
					results[i] = v
				}
				return nil
			}
			if f.pSize > f.results.PCount {
				clear(vm.p[cur.pb+f.results.PCount : cur.pb+f.pSize])
			}
			cur = vm.stack[len(vm.stack)-1]
			vm.stack = vm.stack[:len(vm.stack)-1]
			if cur.mode != drainNone {
				goto drain
			}
			goto resumeFrame
		}
		continue
	comparisonBranch:
		ins = f.Code[cur.pc]
		cur.pc++
		if (r[a] != 0) == (ins.Op() == OpJnz) {
			cur.pc += int(int32(ins.BC()))
		}
		continue
	nativeFault:
		if _, ok := failure.(*standardPanic); !ok {
			return vm.trap(cur, failure)
		}
	fault:
		if !isLanguageError(failure) {
			return vm.trap(cur, failure)
		}
		if err := vm.beginPanic(&cur, cur, values.Value{}, failure, budget); err != nil {
			return err
		}
	drain:
		budget.remaining = remaining
		if err := vm.drain(&cur, budget); err != nil {
			return err
		}
		remaining = budget.remaining
		goto resumeFrame
	}
}

func (vm *VM) invokeNative(cur *frame, index uint32, budget *executionBudget, position SourcePosition, mayReturnRecord bool) error {
	if err := budget.contextErr(); err != nil {
		return err
	}
	f := &vm.program.functions[cur.function]
	g := &vm.program.natives[index]
	rb, pb := cur.rb+f.RCount, cur.pb+f.PCount
	rn, pn := g.rSize, g.pSize
	if g.params.RCount < rn {
		clear(vm.r[rb+g.params.RCount : rb+rn])
	}
	if g.params.PCount < pn {
		clear(vm.p[pb+g.params.PCount : pb+pn])
	}
	vm.call.native, vm.call.Context, vm.call.Position = g, budget.ctx, position
	vm.call.output = vm.Output
	vm.call.R = vm.r[rb : rb+rn : rb+rn]
	vm.call.P = vm.p[pb : pb+pn : pb+pn]
	vm.call.usedR, vm.call.usedP = rb+rn, pb+pn
	err := g.Call(&vm.call)
	vm.call.R, vm.call.P = nil, nil
	if g.results.PCount < pn {
		clear(vm.p[pb+g.results.PCount : pb+pn])
	}
	if err != nil {
		return err
	}
	if budget.err != nil {
		return budget.err
	}
	return budget.contextErr()
}

func boolWord(v bool) uint64 {
	if v {
		return 1
	}
	return 0
}

// Grow reusable execution buffers only after reserving their full new backing
// array. Explicit capacities avoid depending on Go's append growth heuristic.
func growBuffer[T any](buf []T, length, limit int, budget *executionBudget) ([]T, error) {
	if length <= cap(buf) {
		return buf[:length], nil
	}
	capacity := length
	if cap(buf) <= limit/2 {
		capacity = max(capacity, 2*cap(buf))
	} else {
		capacity = limit
	}
	var zero T
	size := uint64(unsafe.Sizeof(zero))
	if uint64(capacity) > math.MaxInt/size {
		return buf, ErrAllocation
	}
	if err := charge(uint64(capacity)*size, &budget.allocated, budget.limits); err != nil {
		return buf, err
	}
	next := make([]T, length, capacity)
	copy(next, buf)
	return next, nil
}
func (vm *VM) grow(r, p int, budget *executionBudget) error {
	limits := budget.limits
	if r > limits.RSlots || p > limits.PSlots {
		return ErrRegisters
	}
	var err error
	if r > len(vm.r) {
		vm.r, err = growBuffer(vm.r, r, limits.RSlots, budget)
		if err != nil {
			return err
		}
	}
	if p > len(vm.p) {
		vm.p, err = growBuffer(vm.p, p, limits.PSlots, budget)
	}
	return err
}
func (vm *VM) trap(f frame, err error) error {
	fn := &vm.program.functions[f.function]
	pc := max(f.pc-1, 0)
	return &ExecutionError{Function: fn.Name, PC: pc, Position: fn.position(pc), Err: err}
}
