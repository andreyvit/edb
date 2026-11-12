package evm

import (
	"fmt"
	"github.com/andreyvit/edb/values"
)

// Interface reads a native argument by ABI slot, preserving nil versus typed nil.
// Returned values and type identities belong to this Program.
func (c *Call) Interface(slot int) (*values.Type, values.Value, error) {
	if c.program == nil {
		return nil, values.Value{}, fmt.Errorf("inactive native call")
	}
	g := c.native
	if !c.interfaceSlot(slot, g.Signature.Params, g.params) {
		return nil, values.Value{}, fmt.Errorf("argument slot is not an interface")
	}
	s, v, err := c.program.typeShapes.Unpack(c.Value(slot))
	if err != nil || s == nil {
		return nil, v, err
	}
	return s.Type, v, nil
}
func (c *Call) interfaceSlot(slot int, ts []*values.Type, l Layout) bool {
	for i, t := range ts {
		if int(l.Slots[i]) == slot {
			s := c.program.shapeByType[t]
			if s != nil && s.Kind == values.InterfaceKind {
				return true
			}
		}
	}
	return false
}

// SetInterface writes an interface result by ABI slot. Dynamic types must be
// declared in the wrapper's DynamicTypes (or used by source) before linking.
// Boxing and value copies are charged to this execution's allocation budget.
func (c *Call) SetInterface(slot int, t *values.Type, v values.Value) (err error) {
	defer func() {
		if c.budget != nil && (err == ErrAllocation || err == ErrAllocations) {
			c.budget.err = err
		}
	}()
	if c.program == nil {
		return fmt.Errorf("inactive native call")
	}
	g := c.native
	if !c.interfaceSlot(slot, g.Signature.Results, g.results) {
		return fmt.Errorf("result slot is not an interface")
	}
	if t == nil {
		c.SetValue(slot, values.Value{})
		return nil
	}
	id, ok := c.program.typeIDs[t]
	if !ok {
		return fmt.Errorf("dynamic type %s is not linked", t.Name())
	}
	d := &c.program.types[id]
	for i, rt := range g.Signature.Results {
		if int(g.results.Slots[i]) == slot {
			s := c.program.shapeByType[rt]
			if s != nil && s.Kind == values.InterfaceKind {
				if !c.program.implements(d, s) {
					return fmt.Errorf("dynamic type does not implement result interface")
				}
			}
		}
	}
	v, err = snapshot(d.shape, v, &c.budget.allocated, c.budget.limits)
	if err != nil {
		return err
	}
	v, err = c.program.packInterface(uint64(id), v, &c.budget.allocated, c.budget.limits)
	if err != nil {
		return err
	}
	c.SetValue(slot, v)
	return nil
}
