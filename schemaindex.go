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

	skipInitialFill bool
	debugScans      bool
}

func makeIndexBucketName(name string) bucketName {
	return makeBucketName("i_" + name)
}

type IndexOpt int

const (
	IndexOptSkipInitialFill IndexOpt = iota
	IndexOptDebugScans
)

func AddIndex[T any](name string, opts ...any) *Index {
	recType := reflect.TypeOf((*T)(nil)).Elem()

	idx := &Index{
		name:    name,
		buck:    makeIndexBucketName(name),
		recType: recType,
		keyEnc:  flatEncodingOf(recType),
	}

	for _, opt := range opts {
		switch opt := opt.(type) {
		case IndexOpt:
			switch opt {
			case IndexOptSkipInitialFill:
				idx.skipInitialFill = true
			case IndexOptDebugScans:
				idx.debugScans = true
			default:
				panic(fmt.Errorf("invalid option %T %v", opt, opt))
			}
		default:
			panic(fmt.Errorf("invalid option %T %v", opt, opt))
		}
	}

	return idx
}

func (idx *Index) requireTable() {
	if idx.table == nil {
		panic(fmt.Errorf("index %q was not added to a table", idx.name))
	}
}

func (idx *Index) Table() *Table {
	return idx.table
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
	keyVal := reflect.New(idx.recType).Elem()
	idx.DecodeIndexKeyValInto(keyVal, tup)
	return keyVal
}

func (idx *Index) DecodeIndexKeyValInto(keyVal reflect.Value, tup tuple) {
	err := idx.keyEnc.decodeTup(tup, keyVal)
	if err != nil {
		panic(fmt.Errorf("failed to decode %s key: %w", idx.FullName(), err))
	}
}

func (idx *Index) parseRawKeyFrom(buf []byte, s string) ([]byte, error) {
	return idx.keyEnc.stringsToRawKey(buf, strings.Split(s, idx.table.keyStringSep))
}

func (idx *Index) ParseNakedIndexKeyVal(s string) (reflect.Value, error) {
	buf := keyBytesPool.Get().([]byte)
	defer releaseKeyBytes(buf)
	raw, err := idx.parseRawKeyFrom(buf, s)
	if err != nil {
		return reflect.Value{}, err
	}
	keyVal := reflect.New(idx.recType).Elem()
	err = idx.keyEnc.decodeVal(raw, keyVal)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to decode %s key %q: %w", idx.FullName(), raw, err)
	}
	return keyVal, nil
}

func (idx *Index) ParseNakedIndexKey(s string) (any, error) {
	val, err := idx.ParseNakedIndexKeyVal(s)
	if err != nil {
		return nil, err
	}
	return val.Interface(), nil
}
