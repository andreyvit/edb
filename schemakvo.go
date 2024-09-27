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
	dataBuck  bucketName
	isRaw     bool

	indices       []*KVIndex
	indicesByName map[string]*KVIndex
}

type KVKey interface {
	Clone() KVKey
	PutBytes(raw []byte) bool
	String() string
}

func DefineRawTable(scm *Schema, name string) *KVTable {
	tbl := DefineKVTable(scm, name, nil, nil, nil)
	tbl.isRaw = true
	return tbl
}

func DefineKVTable(scm *Schema, name string, rootModel *kvo.Model, keySample KVKey, build func(b *KVTableBuilder)) *KVTable {
	tbl := &KVTable{
		name:          name,
		rootModel:     rootModel,
		dataBuck:      makeBucketName(name),
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

func (b *KVTableBuilder) DefineIndex(name string, keySample KVIndexKey, resolver KVIndexKeyToPrimaryKey, indexer KVIndexer) *KVIndex {
	idx := &KVIndex{
		name:                 name,
		idxBuck:              makeBucketName(b.table.name + "_i_" + name),
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
