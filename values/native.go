package values

import "unsafe"

// NativeAddress marks a pointer into live Go memory. A pointer with R=0 refers
// to VM cells. The pointer's static type determines whether it carries this
// discriminator; opaque interface receivers continue to use their own ABI.
const NativeAddress uint64 = 1

// NativeLayout supplies operations on Go memory, independently of VM cells.
// Load returns decoded scalars/strings, native pointer targets, or a borrowed
// native aggregate address. VM aggregate conversion remains an explicit step.
type NativeLayout struct {
	Size   uint64
	New    func() unsafe.Pointer
	Load   func(unsafe.Pointer) Value
	Store  func(unsafe.Pointer, Value)
	Fields []NativeField
}
type NativeField struct {
	Offset uintptr
	Get    func(unsafe.Pointer) Value
	Set    func(unsafe.Pointer, Value)
}

// NativeStorage reports whether this shape supports live native allocation.
// VM collection and interface layouts remain distinct from Go storage.
func (s *Shape) NativeStorage() bool {
	if s.Native == nil {
		return false
	}
	switch s.Kind {
	case BoolKind, IntKind, UintKind, FloatKind, StringKind, StructKind, PointerKind:
		return true
	}
	return false
}
