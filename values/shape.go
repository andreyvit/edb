package values

import (
	"context"
	"math"
	"unsafe"
)

// Shape describes typed VM storage. Descriptors, including recursive graphs,
// are built before use and never mutated by execution. Link snapshots them.
// Scalar cells hold one Value. Structs/arrays flatten their members into cells;
// their register values point to immutable snapshots (nil means all zero).
type Shape struct {
	Native *NativeLayout // optional live Go representation, distinct from Cells
	// DisplayName is optional source-language spelling, prepared at compile time.
	DisplayName string
	Methods     []Method

	Params, Results []*Shape
	// PointerLike gives opaque typed pairs pointer nil semantics: P == nil is
	// the zero value regardless of representation metadata. Other pairs may
	// legitimately carry nonzero auxiliary data with a nil pointer.
	PointerLike bool
	Variadic    bool
	Type        *Type
	Kind        Kind
	Bits        uint8
	Elem, Key   *Shape
	Fields      []Field
	Len         uint64
	Cells       uint64
}

// Method is a receiver-free interface requirement. ID includes the defining
// package path for unexported methods.
type Method struct {
	ID        string
	Signature *Shape
}
type Field struct {
	Embedded bool
	Tag      string
	Name     string
	Shape    *Shape
	Offset   uint64
}
type Kind uint8

const (
	BoolKind Kind = iota + 1
	IntKind
	UintKind
	FloatKind
	StringKind
	PointerKind
	StructKind
	ArrayKind
	SliceKind
	MapKind
	ComplexKind
	FunctionKind
	InterfaceKind
	// TypedPairKind carries a registered physical layout and auxiliary word.
	TypedPairKind
	TimestampKind
)

func (s *Shape) Aggregate() bool {
	return s.Kind == StructKind || s.Kind == ArrayKind || s.Kind == ComplexKind && s.Bits == 128
}
func (s *Shape) Stored(p unsafe.Pointer) Value {
	if s.Aggregate() {
		return Value{P: p}
	}
	if p == nil {
		return Value{}
	}
	return *(*Value)(p)
}
func (s *Shape) Store(p unsafe.Pointer, v Value) {
	if s.Aggregate() {
		dst := CellSlice(p, s.Cells)
		if v.P == nil {
			clear(dst)
		} else {
			copy(dst, CellSlice(v.P, s.Cells))
		}
	} else {
		if s.Type.Repr() == Raw {
			v.P = nil
		}
		if s.Type.Repr() == Pointer {
			v.R = 0
		}
		*(*Value)(p) = v
	}
}
func CellAt(p unsafe.Pointer, n uint64) unsafe.Pointer {
	if p == nil {
		return nil
	}
	return unsafe.Add(p, uintptr(n)*unsafe.Sizeof(Value{}))
}
func CellSlice(p unsafe.Pointer, n uint64) []Value { return unsafe.Slice((*Value)(p), int(n)) }

func (s *Shape) Comparable() bool {
	switch s.Kind {
	case SliceKind, MapKind, FunctionKind:
		return false
	case StructKind:
		for i := range s.Fields {
			f := &s.Fields[i]
			if !f.Shape.Comparable() {
				return false
			}
		}
	case ArrayKind:
		return s.Elem.Comparable()
	}
	return true
}
func (s *Shape) Equal(a, b Value, ts *TypeTable) (bool, error) {
	return s.EqualContext(context.Background(), a, b, ts)
}

// EqualContext allows cancellation during traversal of compound values.
func (s *Shape) EqualContext(ctx context.Context, a, b Value, ts *TypeTable) (bool, error) {
	switch s.Kind {
	case InterfaceKind:
		if err := ctx.Err(); err != nil {
			return false, err
		}
		aID, bID := ts.Index(a), ts.Index(b)
		if aID != bID {
			return false, nil
		}
		if aID == 0 {
			return true, nil
		}
		shape, x, err := ts.Unpack(a)
		if err != nil {
			return false, err
		}
		_, y, err := ts.Unpack(b)
		if err != nil {
			return false, err
		}
		if !ts.Comparable(aID) {
			return false, &ComparisonError{Type: shape.Type.Name()}
		}
		return shape.EqualContext(ctx, x, y, ts)
	case TimestampKind:
		return a.AsTime().Equal(b.AsTime()), nil
	case StringKind:
		return a.AsString() == b.AsString(), nil
	case FloatKind:
		return floatEqual(s.Bits, a, b), nil
	case PointerKind:
		return a.P == b.P, nil
	case TypedPairKind:
		if s.PointerLike && a.P == nil && b.P == nil {
			return true, nil
		}
		return a == b, nil
	case StructKind:
		for i := range s.Fields {
			if i&255 == 255 {
				if err := ctx.Err(); err != nil {
					return false, err
				}
			}
			f := &s.Fields[i]
			if f.Name != "_" {
				eq, err := f.Shape.EqualContext(ctx, f.Shape.Stored(CellAt(a.P, f.Offset)), f.Shape.Stored(CellAt(b.P, f.Offset)), ts)
				if err != nil || !eq {
					return eq, err
				}
			}
		}
		return true, nil
	case ArrayKind:
		if s.Elem.Cells == 0 {
			return true, nil
		}
		for i := uint64(0); i < s.Len; i++ {
			if i&255 == 255 {
				if err := ctx.Err(); err != nil {
					return false, err
				}
			}
			eq, err := s.Elem.EqualContext(ctx, s.Elem.Stored(CellAt(a.P, i*s.Elem.Cells)), s.Elem.Stored(CellAt(b.P, i*s.Elem.Cells)), ts)
			if err != nil || !eq {
				return eq, err
			}
		}
		return true, nil
	case ComplexKind:
		if s.Bits == 64 {
			return math.Float32frombits(uint32(a.R)) == math.Float32frombits(uint32(b.R)) && math.Float32frombits(uint32(a.R>>32)) == math.Float32frombits(uint32(b.R>>32)), nil
		}
		x := floatEqual(s.Elem.Bits, s.Elem.Stored(a.P), s.Elem.Stored(b.P))
		y := floatEqual(s.Elem.Bits, s.Elem.Stored(CellAt(a.P, 1)), s.Elem.Stored(CellAt(b.P, 1)))
		return x && y, nil
	case SliceKind, MapKind, FunctionKind:
		return false, &ComparisonError{Type: s.Type.Name()}
	default:
		return a.R == b.R, nil
	}
}
func hashMix(h, x uint64) uint64 { return (h ^ x) * 1099511628211 }
func (s *Shape) Hash(v Value, ts *TypeTable) (uint64, error) {
	return s.HashContext(context.Background(), v, ts)
}

// HashContext allows cancellation during traversal of compound values.
func (s *Shape) HashContext(ctx context.Context, v Value, ts *TypeTable) (uint64, error) {
	h := uint64(14695981039346656037)
	switch s.Kind {
	case InterfaceKind:
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		id := ts.Index(v)
		if id == 0 {
			return 0, nil
		}
		shape, x, err := ts.Unpack(v)
		if err != nil {
			return 0, err
		}
		if !ts.Comparable(id) {
			return 0, &ComparisonError{Type: shape.Type.Name(), Hash: true}
		}
		n, err := shape.HashContext(ctx, x, ts)
		return hashMix(id, n), err
	case StringKind:
		text := v.AsString()
		for len(text) > 0 {
			n := min(len(text), 4096)
			for _, b := range []byte(text[:n]) {
				h = hashMix(h, uint64(b))
			}
			text = text[n:]
			if len(text) > 0 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
			}
		}
		return h, nil
	case TimestampKind:
		t := v.AsTime()
		return hashMix(uint64(t.Unix()), uint64(t.Nanosecond())), nil
	case FloatKind:
		return floatHash(s.Bits, v), nil
	case PointerKind:
		return uint64(uintptr(v.P)), nil
	case TypedPairKind:
		if s.PointerLike && v.P == nil {
			return 0, nil
		}
		return hashMix(v.R, uint64(uintptr(v.P))), nil
	case StructKind:
		for i := range s.Fields {
			if i&255 == 255 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
			}
			f := &s.Fields[i]
			if f.Name != "_" {
				n, err := f.Shape.HashContext(ctx, f.Shape.Stored(CellAt(v.P, f.Offset)), ts)
				if err != nil {
					return 0, err
				}
				h = hashMix(h, n)
			}
		}
		return h, nil
	case ArrayKind:
		if s.Elem.Cells == 0 {
			return h, nil
		}
		for i := uint64(0); i < s.Len; i++ {
			if i&255 == 255 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
			}
			n, err := s.Elem.HashContext(ctx, s.Elem.Stored(CellAt(v.P, i*s.Elem.Cells)), ts)
			if err != nil {
				return 0, err
			}
			h = hashMix(h, n)
		}
		return h, nil
	case ComplexKind:
		var a, b uint64
		if s.Bits == 64 {
			a = floatHash(s.Elem.Bits, Value{R: uint64(uint32(v.R))})
			b = floatHash(s.Elem.Bits, Value{R: v.R >> 32})
		} else {
			a = floatHash(s.Elem.Bits, s.Elem.Stored(v.P))
			b = floatHash(s.Elem.Bits, s.Elem.Stored(CellAt(v.P, 1)))
		}
		return hashMix(a, b), nil
	case SliceKind, MapKind, FunctionKind:
		return 0, &ComparisonError{Type: s.Type.Name(), Hash: true}
	default:
		return v.R, nil
	}
}
func SliceValue(p unsafe.Pointer, length, capacity uint32) Value {
	return Value{R: uint64(length) | uint64(capacity)<<32, P: p}
}
func (v Value) SliceLen() uint32 { return uint32(v.R) }
func (v Value) SliceCap() uint32 { return uint32(v.R >> 32) }

// Work bounds traversal of a value before hashing/comparison. It follows value
// fields only, never pointers; repeated string fields count each traversal.
func (s *Shape) Work(v Value, limit uint64, ts *TypeTable) (uint64, bool) {
	n := uint64(1)
	if limit == 0 {
		return 0, false
	}
	if s.Kind == StringKind {
		return max(1, v.R), v.R <= limit
	}
	add := func(child *Shape, v Value) bool {
		c, ok := child.Work(v, limit-n, ts)
		if !ok {
			return false
		}
		n += c
		return true
	}
	switch s.Kind {
	case InterfaceKind:
		if v != (Value{}) {
			child, x, err := ts.Unpack(v)
			if err != nil {
				return 0, false
			}
			if !add(child, x) {
				return 0, false
			}
		}
	case StructKind:
		for i := range s.Fields {
			f := &s.Fields[i]
			if f.Name != "_" && !add(f.Shape, f.Shape.Stored(CellAt(v.P, f.Offset))) {
				return 0, false
			}
		}
	case ArrayKind:
		if s.Elem.Cells == 0 {
			return n, true
		}
		if s.Len > limit-n {
			return 0, false
		}
		for i := uint64(0); i < s.Len; i++ {
			if !add(s.Elem, s.Elem.Stored(CellAt(v.P, i*s.Elem.Cells))) {
				return 0, false
			}
		}
	}
	return n, true
}

func floatEqual(bits uint8, a, b Value) bool {
	if bits == 32 {
		return math.Float32frombits(uint32(a.R)) == math.Float32frombits(uint32(b.R))
	}
	return a.AsFloat() == b.AsFloat()
}
func floatHash(bits uint8, v Value) uint64 {
	if bits == 32 {
		if math.Float32frombits(uint32(v.R)) == 0 {
			return 0
		}
	} else if v.AsFloat() == 0 {
		return 0
	}
	return v.R
}

// ComparisonError is a Go language panic condition, distinct from corrupt
// interface storage. EVM may recover it; invalid type indexes stay fatal.
type ComparisonError struct {
	Type string
	Hash bool
}

func (e *ComparisonError) Error() string {
	if e.Hash {
		return "hash of unhashable type " + e.Type
	}
	return "comparing uncomparable type " + e.Type
}
