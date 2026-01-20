package edb

import "github.com/andreyvit/edb/kvo"

type (
	KVIndexer              = func(b *KVIndexContentBuilder, pk []byte, v kvo.ImmutableRecord)
	KVIndexKeyToPrimaryKey = func(ik []byte) []byte
)

type KVIndex struct {
	name                 string
	idxBuck              string
	table                *KVTable
	keySample            KVIndexKey
	indexKeyToPrimaryKey KVIndexKeyToPrimaryKey
	indexer              KVIndexer
}

type KVIndexKey interface {
	Clone() KVIndexKey
	PutBytes(raw []byte) bool
	String() string
	FillPrimaryKey(pk KVKey)
}

func (idx *KVIndex) Table() *KVTable {
	return idx.table
}

func (idx *KVIndex) ShortName() string {
	return idx.name
}
func (idx *KVIndex) FullName() string {
	return idx.table.name + "." + idx.name
}

func (idx *KVIndex) enumEntries(k, v []byte, f func(ik []byte)) {
	b := KVIndexContentBuilder{f}
	idx.indexer(&b, k, kvo.LoadRecord(v, idx.table.RootType()))
}

func (idx *KVIndex) entries(k, v []byte) [][]byte {
	var result [][]byte
	idx.enumEntries(k, v, func(ik []byte) {
		result = append(result, ik)
	})
	return result
}

type KVIndexContentBuilder struct {
	f func(ik []byte)
}

func (b *KVIndexContentBuilder) Add(ik []byte) {
	b.f(ik)
}
