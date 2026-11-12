package kvo

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/andreyvit/edb/values"
)

// descriptorTable shares only registration mechanics. Readers still load one
// atomic slice and index it directly by the process-wide representation ID.
// Published entries never change; spare capacity is only written beyond the
// published length. IDs and their reachable metadata are never reclaimed.
type descriptorTable[T any] struct {
	sync.Mutex
	entries atomic.Pointer[[]T]
}

func (t *descriptorTable[T]) register(shape *values.Shape, descriptors []T, ids []values.RepresentationID) {
	if len(descriptors) != len(ids) {
		panic("record: descriptor registration length mismatch")
	}
	t.Lock()
	defer t.Unlock()
	var entries []T
	if p := t.entries.Load(); p != nil {
		entries = *p
	}
	for i, descriptor := range descriptors {
		id := values.RegisterRepresentation(shape)
		n := int(id) + 1
		if n > cap(entries) {
			next := make([]T, n, max(n, max(32, 2*cap(entries))))
			copy(next, entries)
			entries = next
		} else {
			entries = entries[:n]
		}
		entries[id] = descriptor
		ids[i] = id
	}
	t.entries.Store(&entries)
}

type bindingKey struct {
	typ   reflect.Type
	model *Model
}

// Models already own process-lifetime layouts. Repeated default binding calls
// share their plans too, instead of creating permanent descriptors on each use.
var bindings = struct {
	sync.Mutex
	full        map[bindingKey]*Binding
	projections map[bindingKey]*Projection
}{full: make(map[bindingKey]*Binding), projections: make(map[bindingKey]*Projection)}
