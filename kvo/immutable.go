package kvo

import (
	"fmt"
	"sort"
	"unsafe"
)

// ImmutableRecord is a high-level interface to immutableRecordData, representing
// an immutable a tree of objects in a binary format suitable for storing
// on disk and optimized for random access.
type ImmutableRecord struct {
	rootType AnyType
	data     ImmutableRecordData
}

func EmptyRecord(rootType AnyType) ImmutableRecord {
	return ImmutableRecord{rootType, emptyImmutableRecordData}
}

func LoadRecord(data []byte, rootType AnyType) ImmutableRecord {
	return ImmutableRecord{rootType, LoadRecordData(data)}
}

func LoadRecordData(data []byte) ImmutableRecordData {
	if len(data) == 0 {
		return emptyImmutableRecordData
	} else {
		if len(data)&0x7 != 0 {
			panic(fmt.Sprintf("record data must be 8-byte aligned (len = %d)", len(data)))
		}
		return ImmutableRecordData(unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), len(data)/8))
	}
}

func (data ImmutableRecordData) Record(rootType AnyType) ImmutableRecord {
	if len(data) == 0 {
		data = emptyImmutableRecordData
	}
	return ImmutableRecord{rootType, data}
}

func (r ImmutableRecord) Data() ImmutableRecordData {
	return r.data
}

func (r ImmutableRecord) Pack() ImmutableRecordData {
	return r.data
}

func (r ImmutableRecord) Root() ImmutableMap {
	return ImmutableMap{r.rootType, r.data, r.data.RootObject()}
}
func (r ImmutableRecord) AnyRoot() AnyMap {
	return r.Root()
}

type ImmutableMap struct {
	typ AnyType
	rec ImmutableRecordData
	obj ImmutableObjectData // non-nil (even if empty) unless object missing
}

func (m ImmutableMap) IsMissing() bool                     { return m.obj == nil }
func (m ImmutableMap) RecordWithThisRoot() ImmutableRecord { return m.rec.Record(m.typ) }
func (m ImmutableMap) RecordData() ImmutableRecordData     { return m.rec }
func (m ImmutableMap) Packable() Packable                  { return m.rec }
func (m ImmutableMap) Type() AnyType                       { return m.typ }
func (m ImmutableMap) Dump() string                        { return Dump(m) }
func (m ImmutableMap) Get(key uint64) uint64               { return m.obj.MapGet(key) }
func (m ImmutableMap) Keys() []uint64                      { return m.obj.MapKeys() }

func (m ImmutableMap) KeyModel(key uint64) AnyType {
	if m.typ == nil {
		return nil
	} else {
		return m.typ.MapValueType(key)
	}
}

func (m ImmutableMap) GetMap(key uint64) ImmutableMap {
	valueType := m.KeyModel(key)
	raw := m.Get(key)
	if raw == 0 {
		return ImmutableMap{valueType, m.rec, nil}
	} else {
		return ImmutableMap{valueType, m.rec, m.rec.Object(int(raw))}
	}
}
func (m ImmutableMap) GetAnyMap(key uint64) AnyMap {
	return m.GetMap(key)
}

func (m ImmutableMap) Items() func(yield func(k, v uint64) bool) {
	return m.items
}
func (m ImmutableMap) items(yield func(k, v uint64) bool) {
	n := len(m.obj) / 2
	for i := 0; i < n; i++ {
		if !yield(m.obj[i], m.obj[i+n]) {
			break
		}
	}
}

// TODO
type List struct {
	itemModel *Model
	rec       ImmutableRecord
	obj       ImmutableObjectData
}

// ImmutableRecordData provides for random access to an immutable tree of
// objects stored in a binary format suitable for mmap'ing.
//
// The format is as follows:
//
//	 record -> flags:32 count:32 (objectKind:4 offset:28 size:32)... object...
//
//	 object -> map | arrayOrSet | stringOrBlob
//
//		map -> key1 ... keyN valueOrRef1 ... valueOrRefN  (N determined by map size; keys are sorted)
//		arrayOrSet -> valueOrRef1 ... valueOrRefN  (N is determined by array/set size; set elements are sorted)
//		stringOrBlob is just raw bytes  (this is why object directory stores sizes in bytes)
//
//	 valueOrRef -> uint:64 | subobject:64
//
//		subobject -> inlineStringOrData:64 | objectRef:64
//		  inlineStringOrData -> alwaysOne:1 zeros:4 byteCount:3 inlineBytes:[7]byte
//		  objectRef -> alwaysZero:1 zeroes:31 objectIndex:32
//
// Note that only maps are implements so far.
type ImmutableRecordData []uint64

var emptyImmutableRecordData = ImmutableRecordData{0}

func EmptyImmutableRecordData() ImmutableRecordData {
	return emptyImmutableRecordData
}

func (r ImmutableRecordData) Pack() ImmutableRecordData {
	return r
}

func (r ImmutableRecordData) HexString() string {
	return HexString(r)
}

func (r ImmutableRecordData) Bytes() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&r[0])), len(r)*8)
}

func (r ImmutableRecordData) Uints() []uint64 {
	return []uint64(r)
}

func (r ImmutableRecordData) bytes(i, n int) []byte {
	if i+(n+7)/8 > len(r) {
		panic("out of bounds")
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&r[i])), n)
}
func (r ImmutableRecordData) word(off int) uint64 {
	return r[off]
}
func (r ImmutableRecordData) ObjectCount() int {
	return int(r.word(0) & 0xFFFF_FFFF)
}
func (r ImmutableRecordData) objectOffsetSize(i int) uint64 {
	if n := r.ObjectCount(); i > n {
		panic(fmt.Sprintf("object index out of bounds (i = %d, len = %d)", i, n))
	}
	return r.objectOffsetSizeUnchecked(i)
}
func (r ImmutableRecordData) objectOffsetSizeUnchecked(i int) uint64 {
	return r.word(1 + i)
}
func (r ImmutableRecordData) Object(i int) ImmutableObjectData {
	o, _, _ := r.object(i)
	return o
}
func (r ImmutableRecordData) RootObject() ImmutableObjectData {
	if r.ObjectCount() == 0 {
		return nil
	}
	return r.Object(0)
}
func (r ImmutableRecordData) object(i int) (ImmutableObjectData, byte, uint32) {
	kind, off, size := unpackKindOffsetSize(r.objectOffsetSize(i))
	start := int(off)
	end := start + int((size+7)/8)
	return ImmutableObjectData(r[start:end]), kind, size
}

// ImmutableObjectData represents a single object within ImmutableRecordData.
// It can be a map, an array or a set. Could even be a string or a blob, but
// we'd lack the exact size info, and those are better represented as []byte.
type ImmutableObjectData []uint64

func (m ImmutableObjectData) MapKeys() []uint64 {
	return m[:len(m)/2]
}

func (m ImmutableObjectData) MapGet(key uint64) uint64 {
	n := len(m) / 2
	i := sort.Search(n, func(i int) bool { return m[i] >= key })
	if i < n && m[i] == key {
		return m[i+n]
	}
	return 0
}
