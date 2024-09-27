package kvo

import "sort"

var (
	tombstone = &mutableObjectData{nil, nil, 0, -1, objectKindTombstone}
)

// MutableRecord is a builder of ImmutableRecordDatas. It represents a delta
// of a tree of objects in a way that's fast to write and reasonably fast
// to read.
//
// (E.g. we're using unsorted keys, so reading performance could be improved
// by maybe sorting keys before reading.)
type MutableRecord struct {
	rootType AnyType
	original ImmutableRecordData
	objects  []*mutableObjectData
	edits    uint64
}

// NewRecord sets up an empty mutable record with the given root model and
// an empty root map, and returns the root map.
func NewRecord(rootType AnyType) MutableMap {
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
	rec := &MutableRecord{orig.rootType, data, make([]*mutableObjectData, n, roundUpToPowerOf2(n)), 0}
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

func (rec *MutableRecord) addMap(typ AnyType) MutableMap {
	i := len(rec.objects)
	o := &mutableObjectData{nil, nil, 0, i, objectKindMap}
	rec.objects = append(rec.objects, o)
	// no markModified here because adding an object without reference does
	// not change visible data of the record, and setting a reference will
	// mark the record as modified
	return MutableMap{rec, o, typ}
}

func (rec *MutableRecord) updateMap(i int, typ AnyType) MutableMap {
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
	o := &mutableObjectData{nil, orig, size, i, kind}
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
		return nil // for cases when a nil *MutableRecord ends up as AnyRecord
	}
	orig := rec.original
	if !rec.IsDirty() {
		return orig
	}
	origN := orig.ObjectCount()
	n := len(rec.objects)
	totalCount := 1 + n
	for i, o := range rec.objects {
		if o == nil {
			if i >= origN {
				panic("unreachable")
			}
			_, _, size := unpackKindOffsetSize(orig.objectOffsetSizeUnchecked(i))
			totalCount += int((size + 7) / 8)
		} else if o == tombstone {
			// skip
		} else {
			totalCount += len(o.data) + len(o.orig) // overestimate
		}
	}
	res := make(ImmutableRecordData, totalCount)
	res[0] = uint64(n)
	next := 1 + n
	keysFound := make(map[uint64]struct{})
	for i, o := range rec.objects {
		if o == nil {
			kind, off, size := unpackKindOffsetSize(orig.objectOffsetSizeUnchecked(i))
			res[1+i] = packKindOffsetSize(kind, uint32(next), size)
			next += copy(res[next:], orig[off:off+(size+7)/8])
		} else if o == tombstone {
			// skip
		} else {
			switch o.kind {
			case objectKindString, objectKindRaw:
				res[1+i] = packKindOffsetSize(o.kind, uint32(next), o.size)
				next += copy(res[next:], o.data)
			case objectKindMap:
				reserved := res[next : next+len(o.data)+len(o.orig)]

				clear(keysFound)
				var totalKeyCount int
				for i, n := 0, len(o.data); i < n; i += 2 {
					key, value := o.data[i], o.data[i+1]
					keysFound[key] = struct{}{}
					if value != 0 {
						totalKeyCount++
					}
				}
				for i, n := 0, len(o.orig)/2; i < n; i++ {
					key := o.orig[i]
					if _, ok := keysFound[key]; !ok {
						value := o.orig[i+n]
						if value != 0 {
							totalKeyCount++
						}
					}
				}
				var keyIdx int
				for i, n := 0, len(o.data); i < n; i += 2 {
					key, value := o.data[i], o.data[i+1]
					if value != 0 {
						reserved[keyIdx] = key
						reserved[keyIdx+totalKeyCount] = value
						keyIdx++
					}
				}
				for i, n := 0, len(o.orig)/2; i < n; i++ {
					key := o.orig[i]
					if _, ok := keysFound[key]; !ok {
						value := o.orig[i+n]
						if value != 0 {
							reserved[keyIdx] = key
							reserved[keyIdx+totalKeyCount] = value
							keyIdx++
						}
					}
				}
				if keyIdx != totalKeyCount {
					panic("unreachable")
				}

				sort.Sort(keysThenValues(reserved[:2*totalKeyCount]))

				res[1+i] = packKindOffsetSize(o.kind, uint32(next), uint32(totalKeyCount*16))
				next += 2 * totalKeyCount
			default:
				res[1+i] = packKindOffsetSize(o.kind, uint32(next), uint32(len(o.data)*8))
				next += copy(res[next:], o.data)
			}
		}
	}
	return res[:next]
}

// MutableMap allows mutating a map within a MutableRecord.
type MutableMap struct {
	rec *MutableRecord
	obj *mutableObjectData
	typ AnyType
}

func (m MutableMap) IsEmpty() bool {
	return len(m.obj.data) == 0 && len(m.obj.orig) == 0
}

func (m MutableMap) IsMissing() bool        { return m.rec == nil }
func (m MutableMap) Record() *MutableRecord { return m.rec }
func (m MutableMap) Type() AnyType          { return m.typ }
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

func (m MutableMap) GetAnyMap(key uint64) AnyMap {
	var valueType AnyType
	if m.typ != nil {
		valueType = m.typ.MapValueType(key)
		if valueType == nil {
			reportCannotAccessKey(m.typ, key)
		}
	}

	value := m.obj.MapGet(key)
	if value == 0 {
		return ImmutableMap{valueType, m.rec.original, nil}
	} else {
		updated, orig := m.rec.getObject(int(value))
		if updated != nil {
			return MutableMap{m.rec, updated, valueType}
		} else {
			return ImmutableMap{valueType, m.rec.original, orig}
		}
	}
}

func (m MutableMap) Keys() []uint64 {
	panic("not implemented")
}

func (m MutableMap) UpdateMap(key uint64) MutableMap {
	var valueType AnyType
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
		child, isNew := m.rec.lookupObject(int(value))
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
	data []uint64
	orig ImmutableObjectData
	size uint32 // only valid for strings and binary data
	i    int
	kind byte
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
	o.data = append(o.data, key, value)
	return true
}

func (o *mutableObjectData) Ref() uint64 {
	return uint64(o.i)
}
