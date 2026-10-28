//go:build !race

package kvodraft

import (
	"testing"
	"unsafe"
)

// Allocation budgets apply to ordinary optimized builds. Race instrumentation
// changes escape behavior; functional editor tests still run under -race.
func TestAllocsAndSizes(t *testing.T) {
	fields := make([]Field, 128)
	for i := range fields {
		fields[i] = Field{uint64(i + 1), Word(uint64(i + 1))}
	}
	raw := mustNumeric(t, fields, Indexed)
	v, _ := Open(raw)
	out := make([]byte, 0, 4096)
	allocations := testing.AllocsPerRun(100, func() {
		var scratch [8]Edit
		ed := NewEditor(v, scratch[:], nil)
		ed.Set(7, Word(99))
		b := ed.Append(out[:0], Indexed)
		x := Trusted(b).Get(7)
		n := x.Uint()
		if n != 99 {
			panic("value")
		}
	})
	if allocations != 0 {
		t.Fatalf("editor allocations=%g", allocations)
	}
	if n := testing.AllocsPerRun(100, func() {
		var scratch [8]Edit
		var arena [64]byte
		ed := NewEditor(v, scratch[:], arena[:])
		ed.Set(9, ByteString([]byte("replacement")))
		_ = ed.Append(out[:0], Indexed)
	}); n != 0 {
		t.Fatalf("byte editor allocations=%g", n)
	}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := Open(raw); err != nil {
			panic(err)
		}
		_ = v.Get(7)
	}); n != 0 {
		t.Fatalf("read allocations=%g", n)
	}
	t.Logf("View=%d Value=%d Edit=%d Editor=%d PathPart=%d", unsafe.Sizeof(v), unsafe.Sizeof(Value{}), unsafe.Sizeof(Edit{}), unsafe.Sizeof(Editor{}), unsafe.Sizeof(PathPart{}))
}
