package evm

import (
	"fmt"
	"github.com/andreyvit/edb/values"
)

func (p *Program) packInterface(id uint64, v values.Value, allocated *allocationUsage, limits Limits) (values.Value, error) {
	if result, ok := p.typeShapes.Inline(id, v); ok {
		return result, nil
	}
	ptr, err := allocCells(1, allocated, limits)
	if err != nil {
		return values.Value{}, err
	}
	*(*values.Value)(ptr) = v
	return values.Value{R: id, P: ptr}, nil
}

// DynamicType defines a concrete interface payload. Indexes belong to a Program.
type DynamicType struct {
	Shape   *values.Shape
	Methods []DynamicMethod
}
type DynamicMethod struct {
	ID        string
	Signature Signature
	Variadic  bool
	Function  FunctionRef
}
type Invoke struct {
	InterfaceName string // Qualified static interface name for listings.
	Method        values.Method
}
type linkedDynamicType struct {
	shape   *values.Shape
	methods map[uint32]*DynamicMethod
}
type methodRequirement struct {
	id     uint32
	method values.Method
}

func (p *Program) methodID(name string) uint32 {
	if id, ok := p.methodIDs[name]; ok {
		return id
	}
	id := uint32(len(p.methodIDs))
	p.methodIDs[name] = id
	return id
}

type linkedInvoke struct {
	methodID uint32
	Invoke
	rSize, pSize int
}

func methodSignature(s *values.Shape) Signature {
	var sig Signature
	for _, t := range s.Params {
		sig.Params = append(sig.Params, t.Type)
	}
	for _, t := range s.Results {
		sig.Results = append(sig.Results, t.Type)
	}
	return sig
}
func (p *Program) linkInterfaces(m *Module, seen map[*values.Shape]*values.Shape) error {
	p.types = make([]linkedDynamicType, 1, max(1, len(m.Types)))
	for id, t := range m.Types {
		if id == 0 {
			if t.Shape != nil || len(t.Methods) != 0 {
				return fmt.Errorf("dynamic type zero is reserved for nil")
			}
			continue
		}
		s, err := snapshotShape(t.Shape, seen)
		if err != nil {
			return err
		}
		if s != nil && !validateStorageShape(s, make(map[*values.Shape]shapeValidationState)) {
			return fmt.Errorf("invalid dynamic type storage")
		}
		if s == nil || s.Kind == values.InterfaceKind {
			return fmt.Errorf("invalid dynamic concrete type")
		}
		d := linkedDynamicType{shape: s, methods: map[uint32]*DynamicMethod{}}
		if _, ok := p.typeIDs[s.Type]; ok {
			return fmt.Errorf("duplicate dynamic type")
		}
		p.typeIDs[s.Type] = uint32(len(p.types))
		for _, method := range t.Methods {
			if method.ID == "" {
				return fmt.Errorf("empty method identity")
			}
			if _, ok := d.methods[p.methodID(method.ID)]; ok {
				return fmt.Errorf("duplicate method identity")
			}
			sig, ok := p.functionSignature(method.Function)
			if !ok || len(sig.Params) == 0 || sig.Params[0] != s.Type || !sameSignature(Signature{sig.Params[1:], sig.Results}, method.Signature) {
				return fmt.Errorf("invalid dynamic method target %s", method.ID)
			}
			if method.Function.Native && m.Imports[method.Function.Index].Receiver == nil || !method.Function.Native && !p.functions[method.Function.Index].Method {
				return fmt.Errorf("dynamic target lacks receiver slot")
			}
			if !method.Function.Native && len(p.functions[method.Function.Index].Captures) > 0 {
				return fmt.Errorf("method target captures")
			}
			method.Signature = copySignature(method.Signature)
			d.methods[p.methodID(method.ID)] = &method
		}
		p.types = append(p.types, d)
	}
	for _, op := range m.Invokes {
		if op.Method.ID == "" || op.Method.Signature == nil || op.Method.Signature.Kind != values.FunctionKind {
			return fmt.Errorf("invalid invoke signature")
		}
		var err error
		op.Method.Signature, err = snapshotShape(op.Method.Signature, seen)
		if err != nil {
			return err
		}
		sig := methodSignature(op.Method.Signature)
		results, err := LayoutOf(sig.Results)
		if err != nil {
			return err
		}
		// Pair is the largest receiver, so this bounds every concrete method ABI.
		params, err := LayoutOf(append([]*values.Type{values.StringType}, sig.Params...))
		if err != nil {
			return err
		}
		p.invokes = append(p.invokes, linkedInvoke{p.methodID(op.Method.ID), op, max(params.RCount, results.RCount), max(params.PCount, results.PCount)})
	}
	return nil
}
func matchesMethod(method *DynamicMethod, m values.Method) bool {
	s := m.Signature
	if method.Variadic != s.Variadic || len(method.Signature.Params) != len(s.Params) || len(method.Signature.Results) != len(s.Results) {
		return false
	}
	for i, t := range s.Params {
		if method.Signature.Params[i] != t.Type {
			return false
		}
	}
	for i, t := range s.Results {
		if method.Signature.Results[i] != t.Type {
			return false
		}
	}
	return true
}
func (p *Program) implements(t *linkedDynamicType, s *values.Shape) bool {
	for _, m := range p.requirements[s] {
		method, ok := t.methods[m.id]
		if !ok || !matchesMethod(method, m.method) {
			return false
		}
	}
	return true
}
func (p *Program) assert(v values.Value, s *values.Shape, expected uint64) (values.Value, bool, error) {
	if s.Type.Repr() == values.Pointer {
		return values.Value{P: v.P}, expected != 0 && p.typeShapes.Index(v) == expected, nil
	}
	if s.Kind != values.InterfaceKind {
		actual, payload, err := p.typeShapes.Unpack(v)
		if s.Kind == values.TypedPairKind && s.PointerLike && payload.P == nil {
			payload = values.Value{}
		}
		return payload, actual != nil && actual.Type == s.Type && err == nil, err
	}
	id := p.typeShapes.Index(v)
	if id == 0 {
		return values.Value{}, false, nil
	}
	if id >= uint64(len(p.types)) {
		return values.Value{}, false, fmt.Errorf("invalid interface type index")
	}
	d := &p.types[id]
	return v, p.implements(d, s), nil
}
