package evm

import (
	"fmt"

	"github.com/andreyvit/edb/values"
)

// NamedFunction identifies a public, noncapturing guest function. It contains no
// VM state and may be retained by a host registration after the call returns.
type NamedFunction struct {
	Name      string
	Signature Signature
}

// NamedFunction resolves a function passed through an interface. Captures,
// methods and native functions cannot become independently callable entries.
func (p *Program) NamedFunction(v values.Value) (*NamedFunction, error) {
	shape, value, err := p.typeShapes.Unpack(v)
	if err != nil {
		return nil, err
	}
	if shape == nil || shape.Kind != values.FunctionKind || value.R == 0 || value.R > uint64(len(p.functionRefs)) || value.P != nil {
		return nil, fmt.Errorf("expected a named, noncapturing guest function")
	}
	ref := p.functionRefs[value.R-1]
	if ref.Native {
		return nil, fmt.Errorf("expected a guest function")
	}
	f := &p.functions[ref.Index]
	if f.Internal || f.Method || len(f.Captures) != 0 {
		return nil, fmt.Errorf("expected a public, noncapturing guest function")
	}
	return &NamedFunction{Name: f.Name, Signature: copySignature(f.Signature)}, nil
}
