package edb

import (
	"fmt"
	"reflect"
	"strings"

	"go.etcd.io/bbolt"
)

type Index struct {
	table    *Table
	pos      int // index in table.indices, unstable across code changes
	name     string
	buck     bucketName
	recType  reflect.Type
	keyEnc   *flatEncoding
	isUnique bool
	filler   func(row any, ib *IndexBuilder)
}

func makeIndexBucketName(name string) bucketName {
	return makeBucketName("i_" + name)
}

func AddIndex[T any](name string) *Index {
	recType := reflect.TypeOf((*T)(nil)).Elem()

	idx := &Index{
		name:    name,
		buck:    makeIndexBucketName(name),
		recType: recType,
		keyEnc:  flatEncodingOf(recType),
	}
	return idx
}

func (idx *Index) requireTable() {
	if idx.table == nil {
		panic(fmt.Errorf("index %q was not added to a table", idx.name))
	}
}

func (idx *Index) ShortName() string {
	return idx.name
}
func (idx *Index) FullName() string {
	idx.requireTable()
	return idx.table.name + "." + idx.name
}

func (idx *Index) Unique() *Index {
	idx.isUnique = true
	return idx
}

func (idx *Index) bucketIn(tableRootB *bbolt.Bucket) *bbolt.Bucket {
	return nonNil(tableRootB.Bucket(idx.buck.Raw()))
}

func (idx *Index) keyTupleToString(indexKeyTup tuple) string {
	return strings.Join(idx.keyEnc.tupleToStrings(indexKeyTup), "|")
}

func (idx *Index) keyType() reflect.Type {
	return idx.keyEnc.typ
}

func (idx *Index) DecodeIndexKeyVal(tup tuple) reflect.Value {
	keyPtr := reflect.New(idx.recType)
	err := idx.keyEnc.decodeTup(tup, keyPtr)
	if err != nil {
		panic(fmt.Errorf("failed to decode %s key: %w", idx.FullName(), err))
	}
	return keyPtr.Elem()
}
