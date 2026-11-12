package kvo

import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// ListCell is a typed cell pointing directly at borrowed storage. Its shared
// representation describes the encoding and element access; no per-list box is
// allocated. Traversal state is separate: local in Go or reused by the VM.
type ListCell struct{ value values.Value }
type collectionRepresentation struct {
	typ      *values.Type
	elem     AnyType
	plan     *nativePlan
	kind     representationKind
	encoding row2.Encoding
	codec    any
	model    *Model
}

var collectionRepresentations descriptorTable[collectionRepresentation]

type collectionKey struct {
	plan     *nativePlan
	kind     representationKind
	encoding row2.Encoding
}
type collectionCacheEntry struct {
	key collectionKey
	id  values.RepresentationID
}

type collectionAccess struct {
	sync.Mutex
	ids    atomic.Pointer[map[collectionKey]*collectionCacheEntry]
	last   atomic.Pointer[collectionCacheEntry]
	typ    *values.Type
	codec  any
	entity *EntityType
	elem   AnyType
}

func newCollectionAccess(codec any, entity *EntityType, elem AnyType) *collectionAccess {
	return &collectionAccess{typ: values.NewType("record.List", values.Pair), codec: codec, entity: entity, elem: elem}
}
func (a *collectionAccess) wrap(l ListValue) ListCell {
	key := collectionKey{l.plan, l.kind, l.encoding}
	var entry *collectionCacheEntry
	if last := a.last.Load(); last != nil && last.key == key {
		entry = last
	} else {
		if ids := a.ids.Load(); ids != nil {
			entry = (*ids)[key]
		}
		if entry == nil {
			entry = a.register(key)
		}
		a.last.Store(entry)
	}
	id := entry.id
	n := uint64(l.data.R)
	if l.kind == nativeRepresentation {
		n = 0
	}
	if n > math.MaxUint32 {
		panic("record: list exceeds 4 GiB")
	}
	return ListCell{values.TypedPair(id, l.data.P, uint32(n))}
}
func (a *collectionAccess) register(key collectionKey) *collectionCacheEntry {
	a.Lock()
	defer a.Unlock()
	old := a.ids.Load()
	if old != nil {
		if id := (*old)[key]; id != nil {
			return id
		}
	}
	e := collectionRepresentation{typ: a.typ, elem: a.elem, plan: key.plan, kind: key.kind, encoding: key.encoding, codec: a.codec}
	if a.entity != nil {
		e.model = a.entity.Model()
	}
	var registered [1]values.RepresentationID
	collectionRepresentations.register(&values.Shape{Type: a.typ, Kind: values.TypedPairKind, Cells: 1, PointerLike: true}, []collectionRepresentation{e}, registered[:])
	id := registered[0]
	ids := make(map[collectionKey]*collectionCacheEntry)
	if old != nil {
		for k, v := range *old {
			ids[k] = v
		}
	}
	entry := &collectionCacheEntry{key, id}
	ids[key] = entry
	a.ids.Store(&ids)
	return entry
}
func (l ListCell) representation() *collectionRepresentation {
	if l.value == (values.Value{}) {
		return nil
	}
	return &(*collectionRepresentations.entries.Load())[l.value.Representation()]
}
func (l ListCell) expand(e *collectionRepresentation) ListValue {
	if e == nil {
		return ListValue{}
	}
	data := values.Bytes(unsafe.Slice((*byte)(l.value.P), l.value.Auxiliary()))
	if e.kind == nativeRepresentation {
		data = values.Value{P: l.value.P}
	}
	return ListValue{data: data, plan: e.plan, kind: e.kind, encoding: e.encoding}
}
func (l ListCell) Len() int { return l.expand(l.representation()).Len() }

// Value transports a borrowed collection in one shared cell without boxing.
func (l ListCell) Value() values.Value { return l.value }

// Iter builds local traversal state; the cell itself stays 16 bytes.
func (l ListCell) Iter() ListValueIterator   { return l.expand(l.representation()).Iter() }
func (l ListCell) At(index int) values.Value { return l.expand(l.representation()).At(index) }

// TryListCell checks that v belongs to the collection representation registry.
// Record cells and ordinary bytes/slices are not interchangeable with list cells.
func TryListCell(v values.Value) (ListCell, bool) { return tryListCell(v, nil) }
func tryListCell(v values.Value, elem AnyType) (ListCell, bool) {
	if v == (values.Value{}) {
		return ListCell{}, true
	}
	id := v.Representation()
	p := collectionRepresentations.entries.Load()
	if id == 0 || p == nil || uint64(id) >= uint64(len(*p)) || (*p)[id].typ == nil || elem != nil && (*p)[id].elem != elem {
		return ListCell{}, false
	}
	return ListCell{v}, true
}
func ListCellFromValue(v values.Value) ListCell {
	l, ok := TryListCell(v)
	if !ok {
		panic("record: incompatible list cell")
	}
	return l
}

// ListAccess is shared model metadata for reading a collection when its element
// type is supplied dynamically, as it is in EVM bytecode. It creates the same
// ListCell representation used by List[T] and RecordList.
type ListAccess struct {
	code   uint64
	access *collectionAccess
}

func (a *ListAccess) Get(r Record) ListCell { return a.access.wrap(r.ListValue(a.code)) }
func newListAccess(code uint64, typ *ListType) *ListAccess {
	entity, _ := typ.ItemType().(*EntityType)
	return &ListAccess{code, newCollectionAccess(nil, entity, typ.ItemType())}
}

// BindListAccess returns the model's cached accessor. Recompiling a script does
// not create immortal representation metadata per program or per invocation.
func BindListAccess(model *Model, code uint64) *ListAccess {
	a := layoutFor(model).field(code).listAccess
	if a == nil {
		panic("record: list property required")
	}
	return a
}

// TryCell also validates the declared element type before using a dynamically
// transported cell. It accepts cells made by typed Go property references.
func (a *ListAccess) TryCell(v values.Value) (ListCell, bool) { return tryListCell(v, a.access.elem) }

// View expands a cell into stack access metadata. Retain the compact cell when
// passing a collection around; keep the expanded view beside a hot iterator.
func (l ListCell) View() ListValue { return l.expand(l.representation()) }
