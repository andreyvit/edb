package edb

import (
	"reflect"
	"strings"
)

var indexBuilderPtrType = reflect.TypeOf((*IndexBuilder)(nil))
var anyType = reflect.TypeOf((*any)(nil)).Elem()

var dataBucket = makeBucketName("data")

type Schema struct {
	tables            []*Table
	tablesByLowerName map[string]*Table
	tablesByRowType   map[reflect.Type]*Table
	maps              []*KVMap
}

type SchemaOpts struct {
}

func NewSchema(opt SchemaOpts) *Schema {
	scm := &Schema{
		tablesByLowerName: make(map[string]*Table),
		tablesByRowType:   make(map[reflect.Type]*Table),
	}
	return scm
}

func (scm *Schema) Tables() []*Table {
	return append([]*Table(nil), scm.tables...)
}

func (scm *Schema) TableNamed(name string) *Table {
	return scm.tablesByLowerName[strings.ToLower(name)]
}

type bucketName []byte

func makeBucketName(name string) bucketName {
	return bucketName(name)
}

func (bn bucketName) String() string {
	return string(bn)
}

func (bn bucketName) Raw() []byte {
	return []byte(bn)
}

type KVMap struct {
	buck bucketName
}

func AddKVMap(scm *Schema, name string) *KVMap {
	mp := &KVMap{
		buck: makeBucketName(name),
	}
	scm.maps = append(scm.maps, mp)
	return mp
}

func AddSingletonKey[T any](mp *KVMap, key string) *SKey {
	sk := &SKey{
		mp:       mp,
		keyBytes: []byte(key),
		valueEnc: defaultValueEncoding,
	}
	return sk
}

type SKey struct {
	mp       *KVMap
	keyBytes []byte
	valueEnc encodingMethod
}

func (sk *SKey) String() string {
	return sk.mp.buck.String() + "." + string(sk.keyBytes)
}

func (sk *SKey) Raw() []byte {
	return sk.keyBytes
}
