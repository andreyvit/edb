package evm

import (
	"fmt"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// PointerInterface packs a trusted native pointer for an interface argument to Run.
// The type must be registered by the host and linked into this particular program.
func (p *Program) PointerInterface(t *values.Type, ptr unsafe.Pointer) (values.Value, error) {
	id, ok := p.typeIDs[t]
	if !ok || p.types[id].shape.Kind != values.PointerKind {
		return values.Value{}, fmt.Errorf("unlinked native pointer type")
	}
	if p.types[id].shape.Type.Repr() == values.Pair {
		return p.PackInterface(t, values.Value{R: values.NativeAddress, P: ptr})
	}
	return values.Value{R: uint64(id), P: ptr}, nil
}
func (c *Call) Argument(i int) values.Value     { return c.argument(i) }
func (c *Call) SetResult(i int, v values.Value) { c.result(i, v) }
func (c *Call) AllocCells(n uint64) (unsafe.Pointer, error) {
	p, err := allocCells(n, &c.budget.allocated, c.budget.limits)
	if err != nil {
		c.budget.err = err
	}
	return p, err
}
func (c *Call) Program() *Program { return c.program }
func (p *Program) InterfaceType(v values.Value) *values.Type {
	id := p.typeShapes.Index(v)
	if id == 0 || id >= uint64(len(p.types)) {
		return nil
	}
	return p.types[id].shape.Type
}

// Shape returns immutable linked storage metadata for a host adapter.
func (p *Program) Shape(t *values.Type) *values.Shape { return p.shapeByType[t] }

// UnpackInterface reads a program-local interface payload for a host adapter.
func (p *Program) UnpackInterface(v values.Value) (*values.Shape, values.Value, error) {
	return p.typeShapes.Unpack(v)
}

// PackInterface prepares a host input. The caller owns allocation limits and
// snapshots aggregate values before passing them here. Native results should
// use Call.SetInterface, which charges the active VM budget.
func (p *Program) PackInterface(t *values.Type, v values.Value) (values.Value, error) {
	if t == nil {
		return values.Value{}, nil
	}
	id, ok := p.typeIDs[t]
	if !ok {
		return values.Value{}, fmt.Errorf("unlinked interface type %s", t.Name())
	}
	if result, ok := p.typeShapes.Inline(uint64(id), v); ok {
		return result, nil
	}
	box := new(values.Value)
	*box = v
	return values.Value{R: uint64(id), P: unsafe.Pointer(box)}, nil
}

// ChargeAllocation accounts for native result payloads retained by the VM.
func (c *Call) ChargeAllocation(bytes uint64) error {
	err := c.reserve(bytes)
	if err != nil {
		c.budget.err = err
	}
	return err
}
