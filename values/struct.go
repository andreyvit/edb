package values

import (
	"fmt"
	"slices"
	"unsafe"
)

// NewStructShape declares a nominal VM struct using the same Shape and Field
// metadata as compiled structs. It copies the fields and computes cell offsets;
// supplied offsets are ignored. Child shapes must be fully laid out already.
// Build and retain shapes during setup. Type identity is by pointer, not name.
func NewStructShape(name string, fields ...Field) *Shape {
	s := &Shape{Type: NewType(name, Pointer), Kind: StructKind, Fields: slices.Clone(fields)}
	seen := make(map[string]bool, len(fields))
	for i := range s.Fields {
		f := &s.Fields[i]
		if f.Name == "" || f.Shape == nil || f.Shape.Type == nil || seen[f.Name] {
			panic(fmt.Sprintf("values: invalid field %q in %s", f.Name, name))
		}
		seen[f.Name] = true
		if f.Shape.Cells > 1<<32-s.Cells {
			panic("values: struct exceeds storage limit")
		}
		f.Offset = s.Cells
		s.Cells += f.Shape.Cells
	}
	return s
}

// NewPointerShape declares a pointer to VM cell storage. Reuse the result when
// declaring signatures and TypeDecls. Live native Go pointers instead use the
// NativeSchema produced by bindings, which supplies their native layout.
func NewPointerShape(elem *Shape) *Shape {
	if elem == nil || elem.Type == nil {
		panic("values: nil pointer element shape")
	}
	return &Shape{Type: NewType("*"+elem.Type.Name(), Pointer), Kind: PointerKind, Elem: elem, Cells: 1}
}

// New allocates zeroed VM cell storage for this shape, not a native Go object.
// The extra cell gives even a trailing zero-sized field an address in the
// allocation, matching VM allocations. All writes use Go write barriers.
func (s *Shape) New() unsafe.Pointer {
	if s.Cells > 1<<32 {
		panic("values: shape exceeds storage limit")
	}
	data := make([]Value, s.Cells+1)
	return unsafe.Pointer(&data[0])
}

// GetField borrows a field of VM cell storage. Aggregate fields point into that
// storage; use the field shape's Store to copy them. p must be nonnil storage
// for this struct shape, never a pointer to a native Go struct.
func (s *Shape) GetField(p unsafe.Pointer, i int) Value {
	if p == nil {
		panic("values: nil struct pointer")
	}
	f := s.Fields[i]
	return f.Shape.Stored(CellAt(p, f.Offset))
}

// SetField writes a field of VM cell storage. Scalar pointers alias their
// targets; aggregate fields copy their cells, using the same Store as the VM.
func (s *Shape) SetField(p unsafe.Pointer, i int, v Value) {
	if p == nil {
		panic("values: nil struct pointer")
	}
	f := s.Fields[i]
	f.Shape.Store(CellAt(p, f.Offset), v)
}
