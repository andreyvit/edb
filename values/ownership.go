package values

import "strings"

// OwnershipBudget is supplied at ownership boundaries, never on ordinary reads.
// ChargeAllocation must succeed before allocating a clone's backing storage.
type OwnershipBudget interface {
	ChargeAllocation(uint64) error
	ContextErr() error
}

// OwnFunc discharges borrowed-storage dependencies while preserving the logical
// value. It may return already independent values unchanged. Mutable identity
// needs an explicit contract; a shallow copy is not generally sufficient.
type OwnFunc func(Value, OwnershipBudget) (Value, error)

func NewOwnedType(name string, repr Repr, own OwnFunc) *Type {
	if own == nil {
		panic("values: missing ownership operation")
	}
	t := NewType(name, repr)
	t.own = own
	return t
}
func (t *Type) CanOwn() bool { return t != nil && t.own != nil }
func (t *Type) Own(v Value, b OwnershipBudget) (Value, error) {
	if t.own == nil {
		panic("values: type has no ownership operation")
	}
	if err := b.ContextErr(); err != nil {
		return Value{}, err
	}
	return t.own(v, b)
}
func ownString(v Value, b OwnershipBudget) (Value, error) {
	if v.R == 0 {
		return Value{}, nil
	}
	if err := b.ChargeAllocation(v.R); err != nil {
		return Value{}, err
	}
	return String(strings.Clone(v.AsString())), nil
}
