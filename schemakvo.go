package edb

import (
	"encoding/hex"
	"fmt"

	"github.com/andreyvit/edb/kvo"
)

type KVTable struct {
	name      string
	rootModel *kvo.Model
	keySample KVKey
	isRaw     bool

	indices       []*KVIndex
	indicesByName map[string]*KVIndex

	TaggableImpl
}

type KVKey interface {
	Clone() KVKey
	PutBytes(raw []byte) bool
	String() string
}

func DefineRawTable(scm *Schema, name string, build func(b *KVTableBuilder)) *KVTable {
	tbl := DefineKVTable(scm, name, nil, nil, build)
	tbl.isRaw = true
	return tbl
}

func DefineKVTable(scm *Schema, name string, rootModel *kvo.Model, keySample KVKey, build func(b *KVTableBuilder)) *KVTable {
	tbl := &KVTable{
		name:          name,
		rootModel:     rootModel,
		indicesByName: make(map[string]*KVIndex),
		keySample:     keySample,
	}

	b := KVTableBuilder{table: tbl}
	if build != nil {
		build(&b)
	}

	scm.addKVTable(tbl)
	return tbl
}

func (tbl *KVTable) Name() string {
	return tbl.name
}

func (tbl *KVTable) RootModel() *kvo.Model {
	return tbl.rootModel
}

func (tbl *KVTable) RootType() kvo.AnyType {
	if tbl.rootModel == nil {
		return kvo.Map(kvo.TUknownKey, kvo.TUnknownUint64)
	}
	return tbl.rootModel.Type()
}

func (tbl *KVTable) NewKey() KVKey {
	return tbl.keySample.Clone()
}

func (tbl *KVTable) KeyBytesToString(k []byte) string {
	key := tbl.NewKey()
	if key.PutBytes(k) {
		return key.String()
	} else {
		return "??" + hex.EncodeToString(k)
	}
}

func (tbl *KVTable) decodeValue(raw []byte) kvo.ImmutableMap {
	return kvo.LoadRecord(raw, tbl.rootModel.Type()).Root()
}

type KVTableBuilder struct {
	table *KVTable
}

func (b *KVTableBuilder) Tag(tag *Tag) {
	b.table.tags = append(b.table.tags, tag)
}

func (b *KVTableBuilder) DefineIndex(name string, keySample KVIndexKey, resolver KVIndexKeyToPrimaryKey, indexer KVIndexer) *KVIndex {
	idx := &KVIndex{
		name:                 name,
		idxBuck:              b.table.name + "_i_" + name,
		table:                b.table,
		keySample:            keySample,
		indexKeyToPrimaryKey: resolver,
		indexer:              indexer,
	}

	if b.table.indicesByName[name] != nil {
		panic(fmt.Sprintf("index %s already defined", name))
	}

	b.table.indices = append(b.table.indices, idx)
	return idx
}
