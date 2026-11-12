package evm

import (
	"fmt"
	"reflect"
	"slices"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// BorrowSource declares a result whose storage is valid within a scope.
// ScopeParameter is zero for the enclosing scope, or the one-based argument
// index identifying it (including the receiver for methods). Domain names are
// binding-defined identities, e.g. "edb.transaction".
type BorrowSource struct {
	Result         int
	Domain         string
	ScopeParameter int
}
type ScopeEnd struct {
	Domain         string
	ScopeParameter int
}

// ScopedCallback is invoked synchronously inside a newly established scope;
// the host closes that scope after the callback returns or unwinds.
type ScopedCallback struct {
	Argument int
	Domain   string
}

// OwnershipEffects describes host behavior for compiler ownership analysis.
// A nil declaration is conservative: the call may retain arguments and end
// scopes. Ends lists specific invalidations; NoInvalidate forbids all of them.
// Borrowed results must support ownership copying. Unknown aliasing may cause
// acquisition-time copying, reported in Function.OwnershipSites.
type OwnershipEffects struct {
	NoRetain     bool
	NoInvalidate bool
	Borrowed     []BorrowSource
	Ends         []ScopeEnd
	Callbacks    []ScopedCallback
}

func (e *OwnershipEffects) Clone() *OwnershipEffects {
	if e == nil {
		return nil
	}
	c := *e
	c.Borrowed = slices.Clone(e.Borrowed)
	c.Ends = slices.Clone(e.Ends)
	c.Callbacks = slices.Clone(e.Callbacks)
	return &c
}
func (e *OwnershipEffects) Equal(other *OwnershipEffects) bool { return reflect.DeepEqual(e, other) }
func (e *OwnershipEffects) Validate(sig Signature) error {
	if e == nil {
		return nil
	}
	if e.NoInvalidate && len(e.Ends) != 0 {
		return fmt.Errorf("NoInvalidate conflicts with scope endings")
	}
	for _, s := range e.Borrowed {
		if s.Domain == "" || s.Result < 0 || s.Result >= len(sig.Results) || s.ScopeParameter < 0 || s.ScopeParameter > len(sig.Params) {
			return fmt.Errorf("invalid borrowed result effect")
		}
		if !sig.Results[s.Result].CanOwn() {
			return fmt.Errorf("borrowed result %d (%s) has no ownership operation", s.Result, sig.Results[s.Result].Name())
		}
	}
	for _, s := range e.Ends {
		if s.Domain == "" || s.ScopeParameter < 0 || s.ScopeParameter > len(sig.Params) {
			return fmt.Errorf("invalid scope ending effect")
		}
	}
	for _, s := range e.Callbacks {
		if s.Domain == "" || s.Argument < 0 || s.Argument >= len(sig.Params) {
			return fmt.Errorf("invalid scoped callback effect")
		}
	}
	return nil
}

type OwnershipSite struct {
	PC     int
	Reason string
}

func (b *executionBudget) ChargeAllocation(n uint64) error { return charge(n, &b.allocated, b.limits) }
func (b *executionBudget) ContextErr() error               { return b.contextErr() }

func (p *Program) ownValue(s *values.Shape, v values.Value, b *executionBudget) (values.Value, error) {
	if s.Kind == values.InterfaceKind {
		shape, value, err := p.typeShapes.Unpack(v)
		if err != nil || shape == nil {
			return v, err
		}
		// A join can carry borrowed data on one branch and an ordinary scalar
		// on another. Non-borrowable concrete types already own their storage.
		if !shape.Type.CanOwn() && shape.Kind != values.StringKind {
			return v, nil
		}
		owned, err := p.ownValue(shape, value, b)
		if err != nil {
			return values.Value{}, err
		}
		if owned == value {
			return v, nil
		}
		id := p.typeShapes.Index(v)
		if out, ok := p.typeShapes.Inline(id, owned); ok {
			return out, nil
		}
		if err := b.ChargeAllocation(16); err != nil {
			return values.Value{}, err
		}
		return values.Value{R: id, P: unsafe.Pointer(&owned)}, nil
	}
	// Explicit Go struct copies contain an already-owned cell in VM storage.
	// Keep the address intact: other aliases may subsequently assign the struct.
	if s.Kind == values.TypedPairKind && s.PointerLike && v.R == 0 {
		return v, nil
	}
	if !s.Type.CanOwn() {
		if s.Kind == values.StringKind {
			return values.StringType.Own(v, b)
		}
		return values.Value{}, fmt.Errorf("%s has no ownership operation", s.Type.Name())
	}
	return s.Type.Own(v, b)
}
