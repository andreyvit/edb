package values

import (
	"fmt"
	"math"
	"unsafe"
)

type DynamicType struct {
	Shape      *Shape
	Comparable bool
}

// TypeTable owns the program-local identities and encoding of interface values.
// Entry zero is nil. The table and its descriptors are immutable after creation.
type TypeTable struct {
	types    []DynamicType
	markers  []byte
	stringID uint64
	sliceIDs [2]uint64
	pairIDs  map[*Type]uint64
	pairNils map[uint64]Value
}

const (
	interfaceTypeMask    = uint64(1<<16 - 1)
	interfaceDataMask    = uint64(1<<44 - 1)
	interfaceString      = uint64(4) << 60
	interfaceTypedString = uint64(1) << 60
	interfaceSlice32     = uint64(3) << 60
	interfaceSlice22     = uint64(5) << 60
	interfaceByteSlice   = uint64(2) << 62
	interfaceAnySlice    = uint64(3) << 62
)

func NewTypeTable(types []DynamicType) *TypeTable {
	ts := &TypeTable{types: types, markers: make([]byte, len(types)), stringID: math.MaxUint64, sliceIDs: [2]uint64{math.MaxUint64, math.MaxUint64}}
	for id := 1; id < len(types); id++ {
		s := types[id].Shape
		if s.Kind == TypedPairKind {
			if ts.pairIDs == nil {
				ts.pairIDs = make(map[*Type]uint64)
				ts.pairNils = make(map[uint64]Value)
			}
			ts.pairIDs[s.Type] = uint64(id)
			ts.pairNils[uint64(id)] = TypedPair(nilRepresentation(s), nil, 0)
		}
		if s.Type == StringType {
			ts.stringID = uint64(id)
		}
		if s.Kind == SliceKind {
			i := -1
			if s.Elem.Type == Uint8 {
				i = 0
			}
			if s.Elem.Type == AnyType {
				i = 1
			}
			// Each prefix identifies one exact type. Prefer the unnamed slice;
			// other defined types use an explicit ID or the boxed fallback.
			if i >= 0 && (ts.sliceIDs[i] == math.MaxUint64 || s.DisplayName == "[]uint8" || s.DisplayName == "[]any" || s.DisplayName == "[]interface {}") {
				ts.sliceIDs[i] = uint64(id)
			}
		}
	}
	return ts
}

// Index extracts the dynamic type ID; Unpack validates the payload. Scalar
// markers must be checked first: their R contains all 64 bits.
func (ts *TypeTable) Index(v Value) uint64 {
	if len(ts.markers) != 0 {
		offset := uintptr(v.P) - uintptr(unsafe.Pointer(&ts.markers[0]))
		if offset > 0 && offset < uintptr(len(ts.markers)) {
			return uint64(offset)
		}
	}
	if v.R>>63 != 0 {
		return ts.sliceIDs[(v.R>>62)&1]
	}
	switch v.R >> 60 {
	case 0:
		return v.R
	case interfaceString >> 60:
		return ts.stringID
	case interfaceTypedString >> 60, interfaceSlice32 >> 60, interfaceSlice22 >> 60:
		return (v.R >> 44) & interfaceTypeMask
	case typedPairTag >> 60:
		if s := v.Representation().Shape(); s != nil {
			if id := ts.pairIDs[s.Type]; id != 0 {
				return id
			}
		}
		return math.MaxUint64
	default:
		return math.MaxUint64
	}
}

// Inline packs a concrete payload without allocation. Callers box the payload
// when it does not fit. Aggregates must already be immutable snapshots.
func (ts *TypeTable) Inline(id uint64, v Value) (Value, bool) {
	s := ts.types[id].Shape
	switch s.Type.Repr() {
	case Raw:
		return Value{R: v.R, P: unsafe.Pointer(&ts.markers[id])}, true
	case Pointer:
		return Value{R: id, P: v.P}, true
	}
	if s.Kind == TypedPairKind {
		if v == (Value{}) {
			return ts.pairNils[id], true
		}
		r := v.Representation().Shape()
		return v, r != nil && r.Type == s.Type
	}
	if s.Kind == StringKind {
		if id == ts.stringID && v.R < 1<<60 {
			return Value{R: interfaceString | v.R, P: v.P}, true
		}
		if id <= interfaceTypeMask && v.R <= interfaceDataMask {
			return Value{R: interfaceTypedString | id<<44 | v.R, P: v.P}, true
		}
	}
	if s.Kind == SliceKind {
		n, capacity := uint64(v.SliceLen()), uint64(v.SliceCap())
		spare := capacity - n
		if spare < 1<<30 {
			if id == ts.sliceIDs[0] {
				return Value{R: interfaceByteSlice | spare<<32 | n, P: v.P}, true
			}
			if id == ts.sliceIDs[1] {
				return Value{R: interfaceAnySlice | spare<<32 | n, P: v.P}, true
			}
		}
		if id <= interfaceTypeMask {
			if spare < 1<<12 {
				return Value{R: interfaceSlice32 | id<<44 | spare<<32 | n, P: v.P}, true
			}
			if n < 1<<22 && spare < 1<<22 {
				return Value{R: interfaceSlice22 | id<<44 | spare<<22 | n, P: v.P}, true
			}
		}
	}
	return Value{}, false
}

func (ts *TypeTable) Unpack(v Value) (*Shape, Value, error) {
	id := ts.Index(v)
	if id == 0 {
		if v != (Value{}) {
			return nil, Value{}, fmt.Errorf("invalid nil interface")
		}
		return nil, Value{}, nil
	}
	if id >= uint64(len(ts.types)) {
		return nil, Value{}, fmt.Errorf("invalid interface type index")
	}
	s := ts.types[id].Shape
	if s.Type.Repr() == Pointer && v.R == id {
		return s, Value{P: v.P}, nil
	}
	if v.P == unsafe.Pointer(&ts.markers[id]) {
		if s.Type.Repr() != Raw {
			return nil, Value{}, fmt.Errorf("invalid scalar interface")
		}
		return s, Value{R: v.R}, nil
	}
	if v.R == id {
		if v.P == nil {
			return nil, Value{}, fmt.Errorf("missing interface payload")
		}
		return s, *(*Value)(v.P), nil
	}
	if v.R>>63 != 0 {
		n := uint32(v.R)
		spare := uint32(v.R>>32) & (1<<30 - 1)
		if uint64(n)+uint64(spare) > math.MaxUint32 {
			return nil, Value{}, fmt.Errorf("invalid interface slice capacity")
		}
		return s, SliceValue(v.P, n, n+spare), nil
	}
	switch v.R >> 60 {
	case typedPairTag >> 60:
		if s.Kind != TypedPairKind {
			return nil, Value{}, fmt.Errorf("invalid typed-pair interface")
		}
		return s, v, nil
	case interfaceString >> 60:
		return s, Value{R: v.R & (1<<60 - 1), P: v.P}, nil
	case interfaceTypedString >> 60:
		if s.Kind != StringKind {
			return nil, Value{}, fmt.Errorf("invalid string interface")
		}
		return s, Value{R: v.R & interfaceDataMask, P: v.P}, nil
	case interfaceSlice32 >> 60, interfaceSlice22 >> 60:
		if s.Kind != SliceKind {
			return nil, Value{}, fmt.Errorf("invalid slice interface")
		}
		n, spare := uint32(v.R), uint32(v.R>>32)&(1<<12-1)
		if v.R>>60 == interfaceSlice22>>60 {
			n, spare = uint32(v.R)&(1<<22-1), uint32(v.R>>22)&(1<<22-1)
		}
		if uint64(n)+uint64(spare) > math.MaxUint32 {
			return nil, Value{}, fmt.Errorf("invalid interface slice capacity")
		}
		return s, SliceValue(v.P, n, n+spare), nil
	}
	return nil, Value{}, fmt.Errorf("invalid interface encoding")
}

func (ts *TypeTable) Comparable(id uint64) bool { return id == 0 || ts.types[id].Comparable }
