package evm

import (
	"github.com/andreyvit/edb/values"
	"go/constant"
)

type ExternalConstant struct {
	// Untyped preserves exact Go constants until the consuming expression supplies a type.
	Untyped constant.Value
	Rune    bool
	Name    string
	Type    *values.Type
	Value   values.Value
}

// TypeDecl exposes a named type, or an unnamed support type when Name is empty.
// Shape is declarative storage; native wrappers use the same cells as VM code.
type TypeDecl struct {
	// ScalarBase distinguishes Go int from int64 when their storage shapes match.
	ScalarBase *values.Type
	Name       string
	Shape      *values.Shape
}

// ExternalMethod has a receiver-free source signature. Its native ABI prepends
// Receiver to the argument list, reserving incoming V0 via ParameterLayout.
type ExternalMethod struct {
	Ownership    *OwnershipEffects
	Direct       DirectBinding
	Name         string
	Receiver     *values.Type
	Signature    Signature
	Variadic     bool
	Call         Native
	DynamicTypes []*values.Type
}

// OwnsMethodReceiver reports whether t is a named concrete type declared by p,
// or an explicitly declared pointer to one.
func (p *Package) OwnsMethodReceiver(t *values.Type) bool {
	for _, r := range p.Records {
		if r != nil && r.Pointer == t {
			return true
		}
	}
	var base *values.Type
	for i := range p.Types {
		d := &p.Types[i]
		s := d.Shape
		if s == nil || s.Type != t {
			continue
		}
		if d.Name != "" {
			return s.Kind != values.InterfaceKind && s.Kind != values.PointerKind
		}
		if s.Kind == values.PointerKind && s.Elem != nil {
			base = s.Elem.Type
		}
	}
	if base != nil {
		for i := range p.Types {
			d := &p.Types[i]
			if d.Name != "" && d.Shape != nil && d.Shape.Type == base {
				return d.Shape.Kind != values.InterfaceKind && d.Shape.Kind != values.PointerKind
			}
		}
	}
	return false
}
