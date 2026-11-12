package fast1

import (
	"cmp"
	"github.com/andreyvit/edb/kvo"
	"iter"
	"math"
	"slices"
)

var (
	tombstone = &mutableObjectData{i: -1, kind: objectKindTombstone}
)

// MutableRecord is a builder of ImmutableRecordDatas. It represents a delta
// of a tree of objects in a way that's fast to write and reasonably fast
// to read.
//
// (E.g. we're using unsorted keys, so reading performance could be improved
// by maybe sorting keys before reading.)
type MutableRecord struct {
	rootType kvo.AnyType
	original ImmutableRecordData
	objects  []*mutableObjectData
	edits    uint64
	root     mutableObjectData
	small    [2]*mutableObjectData
}

// NewRecord sets up an empty mutable record with the given root model and
// an empty root map, and returns the root map.
func NewRecord(rootType kvo.AnyType) MutableMap {
	return UpdateRecord(EmptyRecord(rootType))
}

// UpdateRecord sets up a mutable variant of the given record and returns
// its root map.
func UpdateRecord(orig ImmutableRecord) MutableMap {
	data := orig.data
	if data == nil {
		data = emptyImmutableRecordData
	}
	n := data.ObjectCount()
	rec := &MutableRecord{rootType: orig.rootType, original: data}
	if n <= len(rec.small) {
		rec.objects = rec.small[:n]
	} else {
		rec.objects = make([]*mutableObjectData, n, roundUpToPowerOf2(n))
	}
	return rec.Root()
}

func (rec *MutableRecord) IsDirty() bool {
	return rec.edits > 0
}

func (rec *MutableRecord) markModified() {
	rec.edits++
}

func (rec *MutableRecord) Original() ImmutableRecord {
	return rec.original.Record(rec.rootType)
}

func (rec *MutableRecord) Root() MutableMap {
	if len(rec.objects) == 0 {
		return rec.addMap(rec.rootType)
	} else {
		return rec.updateMap(0, rec.rootType)
	}
}

func (rec *MutableRecord) newObject(i int, kind objectKind) *mutableObjectData {
	var o *mutableObjectData
	if i == 0 {
		o = &rec.root
	} else {
		o = new(mutableObjectData)
	}
	o.i, o.kind = i, kind
	return o
}

func (rec *MutableRecord) addMap(typ kvo.AnyType) MutableMap {
	i := len(rec.objects)
	o := rec.newObject(i, objectKindMap)
	rec.objects = append(rec.objects, o)
	// no markModified here because adding an object without reference does
	// not change visible data of the record, and setting a reference will
	// mark the record as modified
	return MutableMap{rec, o, typ}
}

func (rec *MutableRecord) updateMap(i int, typ kvo.AnyType) MutableMap {
	o, _ := rec.lookupObject(i)
	return MutableMap{rec, o, typ}
}

func (rec *MutableRecord) getObject(i int) (*mutableObjectData, ImmutableObjectData) {
	if existing := rec.objects[i]; existing != nil {
		if existing == tombstone {
			panic("unreachable: tombstone encountered")
		}
		return existing, existing.orig
	}
	orig, _, _ := rec.original.object(i)
	return nil, orig
}

func (rec *MutableRecord) lookupObject(i int) (*mutableObjectData, bool) {
	if existing := rec.objects[i]; existing != nil && existing != tombstone {
		return existing, false
	}
	orig, kind, size := rec.original.object(i)
	o := rec.newObject(i, kind)
	o.orig, o.size = orig, size
	rec.objects[i] = o
	return o, true
}

func (rec *MutableRecord) PackedRoot() ImmutableMap {
	return rec.PackedRecord().Root()
}

func (rec *MutableRecord) PackedRecord() ImmutableRecord {
	return rec.Pack().Record(rec.rootType)
}

// Pack produces an on-disk binary encoding of the updated record, merging the
// changes recorded in MutableRecord with the original record.
func (rec *MutableRecord) Pack() ImmutableRecordData {
	if rec == nil {
		return nil // for cases when a nil *MutableRecord ends up as AnyFast1Record
	}
	orig := rec.original
	if !rec.IsDirty() {
		return orig
	}
	origN := orig.ObjectCount()
	n := len(rec.objects)
	if uint64(n) > math.MaxUint32 || n > math.MaxInt/8-1 {
		panic("record object count out of bounds")
	}
	totalCount := 1 + n
	for i, o := range rec.objects {
		if o == nil {
			if i >= origN {
				panic("unreachable")
			}
			_, _, size := unpackKindOffsetSize(orig.objectOffsetSizeUnchecked(i))
			words := objectWordCount(size)
			if words > math.MaxInt/8-totalCount {
				panic("record size out of bounds")
			}
			totalCount += words
		} else if o == tombstone {
			// skip
		} else {
			if len(o.data) > math.MaxInt/8-totalCount || len(o.orig) > math.MaxInt/8-totalCount-len(o.data) {
				panic("record size out of bounds")
			}
			totalCount += len(o.data) + len(o.orig) // overestimate
		}
	}
	res := make(ImmutableRecordData, totalCount)
	res[0] = uint64(n)
	next := 1 + n
	for i, o := range rec.objects {
		if o == nil {
			kind, off, size := unpackKindOffsetSize(orig.objectOffsetSizeUnchecked(i))
			res[1+i] = packKindOffsetSize(kind, objectOffset(next), size)
			next += copy(res[next:], orig[int(off):int(off)+objectWordCount(size)])
		} else if o == tombstone {
			// skip
		} else {
			switch o.kind {
			case objectKindString, objectKindRaw:
				res[1+i] = packKindOffsetSize(o.kind, objectOffset(next), o.size)
				next += copy(res[next:], o.data)
			case objectKindMap:
				totalKeyCount := o.packMap(res[next : next+len(o.data)+len(o.orig)])

				if totalKeyCount > math.MaxUint32/16 {
					panic("map size out of bounds")
				}
				res[1+i] = packKindOffsetSize(o.kind, objectOffset(next), uint32(totalKeyCount)*16)
				next += 2 * totalKeyCount
			default:
				words := len(o.data)
				if words > math.MaxUint32/8 {
					panic("object size out of bounds")
				}
				res[1+i] = packKindOffsetSize(o.kind, objectOffset(next), uint32(words)*8)
				next += copy(res[next:], o.data)
			}
		}
	}
	return res[:next]
}

// Merge only the changed keys into the already sorted original. Reserve the
// maximum space for each half, then slide the values against the actual keys.
func (o *mutableObjectData) packMap(dst []uint64) int {
	type pair struct{ key, value uint64 }
	var small [8]pair
	edits := small[:0]
	if n := len(o.data) / 2; n > len(small) {
		edits = make([]pair, 0, n)
	}
	for i := 0; i < len(o.data); i += 2 {
		edits = append(edits, pair{o.data[i], o.data[i+1]})
	}
	slices.SortFunc(edits, func(a, b pair) int { return cmp.Compare(a.key, b.key) })

	keys := o.orig.MapKeys()
	values := dst[len(dst)/2:]
	n, i := 0, 0
	for i < len(keys) || len(edits) > 0 {
		var key, value uint64
		if len(edits) > 0 && (i == len(keys) || edits[0].key <= keys[i]) {
			key, value = edits[0].key, edits[0].value
			edits = edits[1:]
			if i < len(keys) && keys[i] == key {
				i++
			}
		} else {
			key, value = keys[i], o.orig[len(keys)+i]
			i++
		}
		if value != 0 {
			dst[n], values[n] = key, value
			n++
		}
	}
	copy(dst[n:], values[:n])
	return n
}

func objectOffset(next int) uint32 {
	if next > 0x0FFF_FFFF {
		panic("object offset out of bounds")
	}
	return uint32(next)
}

// MutableMap allows mutating a map within a MutableRecord.
type MutableMap struct {
	rec *MutableRecord
	obj *mutableObjectData
	typ kvo.AnyType
}

func (m MutableMap) IsEmpty() bool {
	return len(m.obj.data) == 0 && len(m.obj.orig) == 0
}

func (m MutableMap) IsMissing() bool        { return m.rec == nil }
func (m MutableMap) Record() *MutableRecord { return m.rec }
func (m MutableMap) Type() kvo.AnyType      { return m.typ }
func (m MutableMap) Packable() Packable     { return m.rec }
func (m MutableMap) Dump() string           { return Dump(m) }

func (m MutableMap) Get(key uint64) uint64 {
	ensureCanAccessKey(m.typ, key)
	return m.obj.MapGet(key)
}

func (m MutableMap) Set(key, value uint64) {
	ensureCanAccessKey(m.typ, key)
	if m.obj.MapSet(key, value) {
		m.rec.markModified()
	}
}

func (m MutableMap) GetAnyMap(key uint64) AnyFast1Map {
	edit, read := m.LookupMap(key)
	if !edit.IsMissing() {
		return edit
	}
	return read
}

// LookupMap returns the edited child when present, otherwise its immutable view.
// Reading a child never creates an edit.
func (m MutableMap) LookupMap(key uint64) (MutableMap, ImmutableMap) {
	var valueType kvo.AnyType
	if m.typ != nil {
		valueType = m.typ.MapValueType(key)
		if valueType == nil {
			reportCannotAccessKey(m.typ, key)
		}
	}

	value := m.obj.MapGet(key)
	if value == 0 {
		return MutableMap{}, ImmutableMap{valueType, m.rec.original, nil}
	} else {
		updated, orig := m.rec.getObject(objectIndex(value))
		if updated != nil {
			return MutableMap{m.rec, updated, valueType}, ImmutableMap{}
		} else {
			return MutableMap{}, ImmutableMap{valueType, m.rec.original, orig}
		}
	}
}

func (m MutableMap) KeyCount() int            { return m.obj.KeyCount() }
func (m MutableMap) KeySeq() iter.Seq[uint64] { return m.obj.KeySeq() }

func (m MutableMap) Keys() []uint64 {
	result := make([]uint64, 0, m.KeyCount())
	for key := range m.KeySeq() {
		result = append(result, key)
	}
	return result
}

func (m MutableMap) UpdateMap(key uint64) MutableMap {
	var valueType kvo.AnyType
	if m.typ != nil {
		valueType = m.typ.MapValueType(key)
		if valueType == nil {
			reportCannotAccessKey(m.typ, key)
		}
	}
	value := m.obj.MapGet(key)
	if value == 0 {
		child := m.rec.addMap(valueType)
		m.obj.MapSet(key, child.obj.Ref())
		return child
	} else {
		child, isNew := m.rec.lookupObject(objectIndex(value))
		if isNew {
			if m.obj.MapSet(key, child.Ref()) {
				m.rec.markModified()
			}
		}
		return MutableMap{m.rec, child, valueType}
	}
}

// mutableObjectData holds the actual mutable data for an object of any kind
// within a MutableRecord.
type mutableObjectData struct {
	small [4]uint64
	data  []uint64
	orig  ImmutableObjectData
	size  uint32 // only valid for strings and binary data
	i     int
	kind  objectKind
}

func (o *mutableObjectData) MapGet(key uint64) uint64 {
	n := len(o.data) / 2
	for i := range n {
		if o.data[i*2] == key {
			return o.data[i*2+1]
		}
	}
	return o.orig.MapGet(key)
}

func (o *mutableObjectData) MapSet(key uint64, value uint64) bool {
	n := len(o.data) / 2
	for i := range n {
		if o.data[i*2] == key {
			old := o.data[i*2+1]
			if old == value {
				return false
			} else {
				o.data[i*2+1] = value
				return true
			}
		}
	}
	if o.data == nil {
		o.data = o.small[:0]
	}
	o.data = append(o.data, key, value)
	return true
}

func (o *mutableObjectData) KeyCount() int {
	return len(o.data) / 2
}

func (o *mutableObjectData) KeySeq() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		n := len(o.data) / 2
		for i := range n {
			if !yield(o.data[i*2]) {
				break
			}
		}
	}
}

func (o *mutableObjectData) Ref() uint64 {
	return uint64(o.i)
}
