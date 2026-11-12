package evm

import (
	"fmt"
	"github.com/andreyvit/edb/values"
	"slices"
)

type DeferKind uint8

const (
	DeferDirect DeferKind = iota + 1
	DeferIndirect
	DeferInvoke
	DeferPanic
	DeferRecover
)

// DeferOp describes values to capture now, then invoke when its owner exits.
// ExternalStack selects the Raw stack handle in the instruction's A operand.
type DeferOp struct {
	Kind          DeferKind
	Target        FunctionRef
	Function      Operand
	Signature     uint32
	Invoke        uint32
	Receiver      uint16 // Pair register captured by DeferInvoke.
	Args          []Operand
	ExternalStack bool
}

func (p *Program) linkDefers(m *Module) error {
	for _, d := range m.Defers {
		d.Args = slices.Clone(d.Args)
		switch d.Kind {
		case DeferDirect:
			sig, ok := p.functionSignature(d.Target)
			if !ok || !d.Target.Native && len(p.functions[d.Target.Index].Captures) > 0 {
				return fmt.Errorf("invalid deferred target")
			}
			if !sameOperands(d.Args, sig.Params) {
				return fmt.Errorf("invalid deferred arguments")
			}
		case DeferIndirect:
			if uint64(d.Signature) >= uint64(len(p.signatures)) || d.Function.Repr != values.Pair || !sameOperands(d.Args, p.signatures[d.Signature].Params) {
				return fmt.Errorf("invalid deferred function signature")
			}
		case DeferInvoke:
			if uint64(d.Invoke) >= uint64(len(p.invokes)) {
				return fmt.Errorf("invalid deferred invoke")
			}
			if !sameOperands(d.Args, methodSignature(p.invokes[d.Invoke].Method.Signature).Params) {
				return fmt.Errorf("invalid deferred invoke arguments")
			}
		case DeferPanic:
			if len(d.Args) != 1 || d.Args[0].Repr != values.Pair {
				return fmt.Errorf("invalid deferred panic")
			}
		case DeferRecover:
			if len(d.Args) != 0 {
				return fmt.Errorf("invalid deferred recover")
			}
		default:
			return fmt.Errorf("invalid defer kind")
		}
		p.defers = append(p.defers, d)
	}
	return nil
}
func sameOperands(args []Operand, types []*values.Type) bool {
	if len(args) != len(types) {
		return false
	}
	for i, r := range args {
		if r.Repr != types[i].Repr() {
			return false
		}
	}
	return true
}
func (p *Program) deferSize(d *DeferOp) (int, int) {
	var a, b Layout
	switch d.Kind {
	case DeferDirect:
		if d.Target.Native {
			g := &p.natives[d.Target.Index]
			a, b = g.params, g.results
		} else {
			g := &p.functions[d.Target.Index]
			a, b = g.params, g.results
		}
	case DeferIndirect:
		g := &p.signatures[d.Signature]
		a, b = g.params, g.results
	case DeferInvoke:
		g := &p.invokes[d.Invoke]
		return g.rSize, g.pSize
	}
	return max(a.RCount, b.RCount), max(a.PCount, b.PCount)
}
func (p *Program) validDefer(f *linkedFunction, d *DeferOp) bool {
	valid := func(r Operand) bool {
		return r.Repr >= values.Raw && r.Repr <= values.Pair && (r.Repr == values.Pointer || rValid(r, f)) && (r.Repr == values.Raw || pValid(r, f))
	}
	for _, arg := range d.Args {
		if !valid(arg) {
			return false
		}
	}
	if d.Kind == DeferIndirect && !valid(d.Function) {
		return false
	}
	if d.Kind == DeferInvoke {
		return valid(Operand{d.Receiver, values.Pair})
	}
	return true
}
