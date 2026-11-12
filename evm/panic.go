package evm

import (
	"errors"
	"fmt"
	"github.com/andreyvit/edb/values"
	"math"
	"unsafe"
)

var ErrTypeAssertion = errors.New("interface conversion failed")
var ErrDeferStack = errors.New("defer stack is no longer active")

// PanicError exposes the concrete value of an uncaught explicit Go panic. Type
// is external metadata; Value is the unboxed payload belonging to this Program.
type PanicError struct {
	Type        *values.Type
	Value       values.Value
	description string
}

func (e *PanicError) Error() string { return "panic: " + e.description }

type panicContext struct {
	value              values.Value
	cause              error
	origin             frame
	recovered, aborted bool
}

var runtimeErrorTextShape = &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}
var runtimeErrorObjectShape = &values.Shape{Type: values.NewType("runtime.errorString", values.Pointer), Kind: values.StructKind, Cells: 1, Fields: []values.Field{{Name: "message", Shape: runtimeErrorTextShape}}}
var runtimeErrorShape = &values.Shape{Type: values.NewType("*runtime.errorString", values.Pointer), Kind: values.PointerKind, Cells: 1, Elem: runtimeErrorObjectShape}
var panicNilShape = &values.Shape{Type: values.NewType("*runtime.PanicNilError", values.Pointer), Kind: values.PointerKind, Cells: 1, Elem: runtimeErrorObjectShape}

func (p *Program) linkPanicTypes() error {
	needed := false
	for _, g := range p.natives {
		needed = needed || g.panics
	}
	for i := range p.functions {
		f := &p.functions[i]
		if f.RecoverPC != 0 {
			needed = true
			break
		}
		for _, ins := range f.Code {
			if ins.Op() == OpPanic || ins.Op() == OpRecover || ins.Op() == OpDefer {
				needed = true
				break
			}
		}
	}
	if !needed {
		return nil
	}
	for _, g := range p.natives {
		if g.panics {
			if _, ok := p.typeIDs[values.StringType]; !ok {
				p.typeIDs[values.StringType] = uint32(len(p.types))
				p.types = append(p.types, linkedDynamicType{shape: runtimeErrorTextShape})
			}
			break
		}
	}
	for _, s := range []*values.Shape{runtimeErrorShape, panicNilShape} {
		d := linkedDynamicType{shape: s, methods: make(map[uint32]*DynamicMethod)}
		for _, name := range []string{"Error", "RuntimeError"} {
			sig := Signature{Params: []*values.Type{s.Type}}
			var call Native
			if name == "Error" {
				sig.Results = []*values.Type{values.StringType}
				call = func(c *Call) error { c.SetValue(0, *(*values.Value)(c.P[0])); return nil }
			} else {
				call = func(*Call) error { return nil }
			}
			args, err := ParameterLayout(sig.Params, true)
			if err != nil {
				return err
			}
			results, err := LayoutOf(sig.Results)
			if err != nil {
				return err
			}
			target := FunctionRef{Native: true, Index: uint32(len(p.natives))}
			p.natives = append(p.natives, linkNative(ExternalFunction{Name: "runtime." + name, Signature: sig, Call: call}, args, results))
			d.methods[p.methodID(name)] = &DynamicMethod{ID: name, Signature: Signature{Results: sig.Results}, Function: target}
		}
		id := uint64(len(p.types))
		p.types = append(p.types, d)
		p.typeIDs[s.Type] = uint32(id)
		if s == runtimeErrorShape {
			p.runtimeError = id
		} else {
			p.panicNil = id
		}
	}
	return nil
}
func isLanguageError(err error) bool {
	if _, ok := err.(*standardPanic); ok {
		return true
	}
	if err == ErrNil || err == ErrBounds || err == ErrNilMap || err == ErrDivideByZero || err == ErrNegativeShift || err == ErrNilFunction || err == ErrDeferStack {
		return true
	}
	if errors.Is(err, ErrTypeAssertion) {
		return true
	}
	_, ok := err.(*values.ComparisonError)
	return ok
}
func (vm *VM) errorValue(message string, nilPanic bool, budget *executionBudget) (values.Value, error) {
	if err := charge(uint64(len(message)), &budget.allocated, budget.limits); err != nil {
		return values.Value{}, err
	}
	p, err := allocCells(1, &budget.allocated, budget.limits)
	if err != nil {
		return values.Value{}, err
	}
	*(*values.Value)(p) = values.String(message)
	id := vm.program.runtimeError
	if nilPanic {
		id = vm.program.panicNil
	}
	if id == 0 {
		return values.Value{}, ErrFunction
	}
	return values.Value{R: id, P: p}, nil
}
func (vm *VM) beginPanic(cur *frame, origin frame, value values.Value, cause error, budget *executionBudget) error {
	if uint64(len(vm.panics)) >= math.MaxUint32 {
		return vm.trap(origin, ErrAllocation)
	}
	if vm.program.runtimeError == 0 && cause != nil {
		return vm.trap(origin, cause)
	}
	var err error
	if sp, ok := cause.(*standardPanic); ok {
		id, ok := vm.program.typeIDs[values.StringType]
		if !ok {
			return vm.trap(origin, ErrFunction)
		}
		value, err = vm.program.packInterface(uint64(id), values.String(sp.message), &budget.allocated, budget.limits)
		if err != nil {
			return vm.trap(origin, err)
		}
		cause = nil
	}
	if cause != nil {
		value, err = vm.errorValue(cause.Error(), false, budget)
	} else if value == (values.Value{}) {
		value, err = vm.errorValue("panic called with nil argument", true, budget)
	}
	if err != nil {
		return vm.trap(origin, err)
	}
	if cause == nil {
		shape, payload, err := vm.program.typeShapes.Unpack(value)
		if err != nil || shape == nil {
			return vm.trap(origin, ErrFunction)
		}
		text := shape.Type.Name()
		if vm.program.typeShapes.Index(value) == vm.program.panicNil {
			text = (*values.Value)(value.P).AsString()
		}
		switch shape.Kind {
		case values.StringKind:
			text = payload.AsString()
		case values.IntKind:
			text = fmt.Sprint(payload.AsInt())
		case values.UintKind:
			text = fmt.Sprint(payload.R)
		case values.BoolKind:
			text = fmt.Sprint(payload.R != 0)
		}
		cause = &PanicError{Type: shape.Type, Value: payload, description: text}
	}
	if err := charge(2*uint64(unsafe.Sizeof(panicContext{}))+uint64(unsafe.Sizeof(PanicError{}))+64, &budget.allocated, budget.limits); err != nil {
		return vm.trap(origin, err)
	}
	if cur.mode == drainPanic && cur.panicID != 0 {
		vm.panics[cur.panicID-1].aborted = true
	}
	vm.panics = append(vm.panics, panicContext{value: value, cause: cause, origin: origin})
	cur.panicID = uint32(len(vm.panics))
	cur.mode = drainPanic
	return nil
}
func (vm *VM) recover(cur frame) values.Value {
	if cur.recoverToken == 0 || uint64(cur.recoverToken) > uint64(len(vm.panics)) {
		return values.Value{}
	}
	p := &vm.panics[cur.recoverToken-1]
	if p.recovered || p.aborted {
		return values.Value{}
	}
	p.recovered = true
	return p.value
}
