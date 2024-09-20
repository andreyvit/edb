package edb

import "github.com/andreyvit/edb/kvo"

type KVTable struct {
	name      string
	rootModel *kvo.Model
	dataBuck  bucketName
}

func DefineKVTable(scm *Schema, name string, rootModel *kvo.Model, build func(b *KVTableBuilder)) *KVTable {
	tbl := &KVTable{
		name:     name,
		dataBuck: makeBucketName(name),
	}
	scm.addKVTable(tbl)

	b := KVTableBuilder{tbl}
	if build != nil {
		build(&b)
	}

	return tbl
}

func (tbl *KVTable) Name() string {
	return tbl.name
}

func (tbl *KVTable) decodeValue(raw []byte) kvo.ImmutableMap {
	return kvo.LoadRecord(raw, tbl.rootModel).Root()
}

type KVTableBuilder struct {
	tbl *KVTable
}
