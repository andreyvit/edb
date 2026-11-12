package values

import (
	"testing"
	"unsafe"
)

func TestInterfaceHeaderBounds(t *testing.T) {
	text := &Shape{Type: StringType, Kind: StringKind, Cells: 1}
	namedText := &Shape{Type: NewType("Text", Pair), Kind: StringKind, Cells: 1}
	array := &Shape{Type: NewType("[]any", Pair), Kind: SliceKind, Cells: 1, Elem: &Shape{Type: AnyType, Kind: InterfaceKind, Cells: 1}}
	bytes := &Shape{Type: NewType("[]byte", Pair), Kind: SliceKind, Cells: 1, Elem: &Shape{Type: Uint8, Kind: UintKind, Bits: 8, Cells: 1}}
	ts := NewTypeTable([]DynamicType{{}, {Shape: text, Comparable: true}, {Shape: namedText, Comparable: true}, {Shape: array}, {Shape: bytes}})
	// Only headers are inspected; no multi-gigabyte backing allocation is needed.
	for _, tc := range []struct {
		name    string
		id      uint64
		payload Value
		inline  bool
	}{
		{"string60", 1, Uint(1<<60 - 1), true},
		{"string fallback", 1, Uint(1 << 60), false},
		{"defined string44", 2, Uint(1<<44 - 1), true},
		{"defined string fallback", 2, Uint(1 << 44), false},
		{"any slice spare30", 3, SliceValue(nil, 1, 1<<30), true},
		{"any slice fallback", 3, SliceValue(nil, 1, 1<<30+1), false},
		{"byte slice length32", 4, SliceValue(nil, 1<<32-1, 1<<32-1), true},
		{"byte slice spare30", 4, SliceValue(nil, 1, 1<<30), true},
		{"byte slice fallback", 4, SliceValue(nil, 1, 1<<30+1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, inline := ts.Inline(tc.id, tc.payload)
			if inline != tc.inline {
				t.Fatalf("inline=%v, want %v", inline, tc.inline)
			}
			if !inline {
				v = Value{R: tc.id, P: unsafe.Pointer(&tc.payload)}
			}
			shape, got, err := ts.Unpack(v)
			if err != nil || got != tc.payload || shape != ts.types[tc.id].Shape || ts.Index(v) != tc.id {
				t.Fatalf("round trip: %v %v %v", shape, got, err)
			}
		})
	}
	if _, _, err := ts.Unpack(Value{R: interfaceAnySlice | 1<<32 | (1<<32 - 1)}); err == nil {
		t.Fatal("accepted overflowing slice capacity")
	}

	types := make([]DynamicType, 1<<16+1)
	for i := 1; i < len(types); i++ {
		types[i] = DynamicType{Shape: namedText, Comparable: true}
	}
	wide := NewTypeTable(types)
	for _, id := range []uint64{1<<16 - 1, 1 << 16} {
		payload := String("value")
		v, inline := wide.Inline(id, payload)
		if inline != (id < 1<<16) {
			t.Fatalf("type %d: inline=%v", id, inline)
		}
		if !inline {
			v = Value{R: id, P: unsafe.Pointer(&payload)}
		}
		_, got, err := wide.Unpack(v)
		if err != nil || got != payload || wide.Index(v) != id {
			t.Fatalf("wide type %d: %v %v", id, got, err)
		}
	}
}
