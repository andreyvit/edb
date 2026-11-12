package values

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// RepresentationID identifies immutable physical-layout metadata. Several
// representations may share a logical Type. IDs are process-local, never reused,
// and must not be persisted or allocated per value or transaction.
type RepresentationID uint32

const typedPairTag = uint64(6) << 60
const representationMask = uint64(1<<28 - 1)

var representationRegistry struct {
	sync.Mutex
	shapes atomic.Pointer[[]*Shape]
	nils   map[*Type]RepresentationID
}

// RegisterRepresentation publishes a physical layout for a compact typed pair.
// The supplied shape and its reachable metadata must remain immutable.
func RegisterRepresentation(shape *Shape) RepresentationID {
	if shape == nil || shape.Type == nil || shape.Type.Repr() != Pair || shape.Kind != TypedPairKind {
		panic("values: a representation requires a typed-pair shape")
	}
	representationRegistry.Lock()
	defer representationRegistry.Unlock()
	return registerRepresentationLocked(shape)
}

// Program-local type tables share one nil carrier per logical type. Never
// retain a program's full shape graph or allocate an immortal ID per compile.
func nilRepresentation(s *Shape) RepresentationID {
	t := s.Type
	representationRegistry.Lock()
	defer representationRegistry.Unlock()
	if id := representationRegistry.nils[t]; id != 0 {
		return id
	}
	if representationRegistry.nils == nil {
		representationRegistry.nils = make(map[*Type]RepresentationID)
	}
	id := registerRepresentationLocked(&Shape{Type: t, Kind: TypedPairKind, Cells: 1, PointerLike: s.PointerLike})
	representationRegistry.nils[t] = id
	return id
}

func registerRepresentationLocked(shape *Shape) RepresentationID {
	var old []*Shape
	if p := representationRegistry.shapes.Load(); p != nil {
		old = *p
	}
	n := max(1, len(old))
	if uint64(n) > representationMask {
		panic("values: representation ID overflow")
	}
	next := old
	if n >= cap(next) {
		next = make([]*Shape, n+1, max(16, 2*cap(old)))
		copy(next, old)
	} else {
		next = next[:n+1]
	}
	next[n] = shape
	representationRegistry.shapes.Store(&next)
	return RepresentationID(n)
}

func (id RepresentationID) Shape() *Shape {
	p := representationRegistry.shapes.Load()
	if p == nil || id == 0 || uint64(id) >= uint64(len(*p)) {
		return nil
	}
	return (*p)[id]
}

// TypedPair keeps a real GC-visible pointer and a 32-bit auxiliary word in one
// cell. The ID must come from RegisterRepresentation. Auxiliary data commonly
// holds the length of an immutable byte span; it is not a second pointer.
func TypedPair(id RepresentationID, p unsafe.Pointer, aux uint32) Value {
	if id == 0 || uint64(id) > representationMask {
		panic("values: invalid representation ID")
	}
	return Value{R: typedPairTag | uint64(id)<<32 | uint64(aux), P: p}
}

// Representation returns zero for an untagged value. Concrete scalar callers
// must not interpret arbitrary numeric bits as tags: their external type wins.
func (v Value) Representation() RepresentationID {
	if v.R>>60 != typedPairTag>>60 {
		return 0
	}
	return RepresentationID(v.R >> 32 & representationMask)
}

func (v Value) Auxiliary() uint32 { return uint32(v.R) }
