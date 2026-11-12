package values

import (
	"math"
	"runtime"
	"sync"
	"testing"
	"unsafe"
)

func TestTypedPairInterfaces(t *testing.T) {
	typ := NewType("record", Pair)
	shape := &Shape{Type: typ, Kind: TypedPairKind, Cells: 1}
	other := &Shape{Type: NewType("other", Pair), Kind: TypedPairKind, Cells: 1}
	number := &Shape{Type: Uint64, Kind: UintKind, Cells: 1}
	a := NewTypeTable([]DynamicType{{}, {Shape: shape, Comparable: true}, {Shape: number, Comparable: true}, {Shape: other}})
	b := NewTypeTable([]DynamicType{{}, {Shape: other}, {Shape: shape, Comparable: true}})
	// Register after the type tables: representation identity is shared while
	// logical interface IDs remain local to each program.
	for range 2 {
		id := RegisterRepresentation(shape)
		data := []byte("survives GC")
		for _, aux := range []uint32{0, uint32(len(data)), math.MaxUint32} {
			v := TypedPair(id, unsafe.Pointer(unsafe.SliceData(data)), aux)
			packed, ok := a.Inline(1, v)
			if !ok || packed != v || a.Index(v) != 1 || b.Index(v) != 2 || v.Auxiliary() != aux {
				t.Fatal("typed pair was boxed or lost its identity")
			}
			for _, table := range []*TypeTable{a, b} {
				gotShape, got, err := table.Unpack(packed)
				if err != nil || gotShape != shape || got != v {
					t.Fatal(gotShape, got, err)
				}
			}
			var cell Value
			shape.Store(unsafe.Pointer(&cell), v)
			if cell != v {
				t.Fatal("storage lost auxiliary metadata")
			}
			runtime.GC()
			if string(unsafe.Slice((*byte)(cell.P), len(data))) != "survives GC" {
				t.Fatal("pointer not retained")
			}
		}
	}
	// Scalar marker recognition precedes tags, even when the bits spell a valid
	// registered pair. No scalar bits or noncanonical floats may be rewritten.
	for _, bits := range []uint64{typedPairTag, typedPairTag | 1<<32 | 17, math.MaxUint64} {
		v, ok := a.Inline(2, Uint(bits))
		s, got, err := a.Unpack(v)
		if !ok || err != nil || s != number || got != Uint(bits) {
			t.Fatal(v, got, err)
		}
	}
	nilPair, ok := a.Inline(1, Value{})
	if !ok || nilPair.P != nil || a.Index(nilPair) != 1 {
		t.Fatal("typed nil lost its type")
	}
	for range 100 {
		table := NewTypeTable([]DynamicType{{}, {Shape: shape}})
		otherNil, _ := table.Inline(1, Value{})
		if otherNil != nilPair {
			t.Fatal("nil representation allocated per program")
		}
	}
	if _, _, err := a.Unpack(Value{R: typedPairTag | representationMask<<32}); err == nil {
		t.Fatal("unknown representation accepted")
	}
	wrong := TypedPair(RegisterRepresentation(other), nil, 0)
	if _, ok := a.Inline(1, wrong); ok {
		t.Fatal("wrong nominal type accepted")
	}
}

func TestRepresentationPublication(t *testing.T) {
	s := &Shape{Type: NewType("concurrent", Pair), Kind: TypedPairKind, Cells: 1}
	id := RegisterRepresentation(s)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				if id.Shape() != s || RegisterRepresentation(s).Shape() != s {
					t.Error("publication lost metadata")
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkTypedPairInterface(b *testing.B) {
	s := &Shape{Type: NewType("benchmark", Pair), Kind: TypedPairKind, Cells: 1}
	ts := NewTypeTable([]DynamicType{{}, {Shape: s, Comparable: true}})
	data := []byte("record")
	v := TypedPair(RegisterRepresentation(s), unsafe.Pointer(unsafe.SliceData(data)), uint32(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packed, ok := ts.Inline(1, v)
		_, got, err := ts.Unpack(packed)
		if !ok || err != nil || got != v {
			b.Fatal("round trip")
		}
	}
}

func TestTypedPairNilSemantics(t *testing.T) {
	shape := &Shape{Type: NewType("opaque", Pair), Kind: TypedPairKind, Cells: 1}
	id := RegisterRepresentation(shape)
	a, b := TypedPair(id, nil, 0), TypedPair(id, nil, 1)
	if eq, err := shape.Equal(a, b, nil); err != nil || eq {
		t.Fatal("nil data pointer erased an opaque auxiliary word", eq, err)
	}
	pointer := &Shape{Type: NewType("pointer facade", Pair), Kind: TypedPairKind, Cells: 1, PointerLike: true}
	a = TypedPair(RegisterRepresentation(pointer), nil, 0)
	if eq, err := pointer.Equal(a, Value{}, nil); err != nil || !eq {
		t.Fatal("typed nil differs from zero", eq, err)
	}
	ha, _ := pointer.Hash(a, nil)
	hb, _ := pointer.Hash(Value{}, nil)
	if ha != hb {
		t.Fatal("equal nil values have different hashes")
	}
}
