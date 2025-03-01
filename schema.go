package edb

import (
	"fmt"
	"reflect"
	"strings"
)

type Tablish interface {
	TableName() string
	HasTag(tag *Tag) bool
}

var indexBuilderPtrType = reflect.TypeOf((*IndexBuilder)(nil))
var anyType = reflect.TypeOf((*any)(nil)).Elem()

var dataBucket = makeBucketName("data")

type Schema struct {
	Name                string
	tables              []*Table
	tablesByLowerName   map[string]*Table
	tablesByRowType     map[reflect.Type]*Table
	maps                []*KVMap
	kvtables            []*KVTable
	kvtablesByLowerName map[string]*KVTable
}

func (scm *Schema) init() {
	if scm.tablesByLowerName == nil {
		scm.tablesByLowerName = make(map[string]*Table)
		scm.tablesByRowType = make(map[reflect.Type]*Table)
		scm.kvtablesByLowerName = make(map[string]*KVTable)
	}
}

func (scm *Schema) Include(peer *Schema) {
	scm.init()
	for _, tbl := range peer.tables {
		if scm.tablesByLowerName[strings.ToLower(tbl.name)] != nil {
			panic(fmt.Errorf("table %s is defined in multiple schemas", tbl.name))
		}
		if scm.tablesByRowType[tbl.rowType] != nil {
			panic(fmt.Errorf("row type %v is used in multiple schemas", tbl.rowType))
		}
		scm.addTable(tbl)
	}
}

func (scm *Schema) addTable(tbl *Table) {
	lower := strings.ToLower(tbl.name)

	if scm.tablesByLowerName[lower] != nil {
		panic(fmt.Errorf("table %s is already defined", tbl.name))
	}
	if scm.kvtablesByLowerName[lower] != nil {
		panic(fmt.Errorf("KV table %s is already defined, and the namespace is shared", tbl.name))
	}
	if scm.tablesByRowType[tbl.rowType] != nil {
		panic(fmt.Errorf("row type %v is already used", tbl.rowType))
	}

	tbl.pos = len(scm.tables)
	scm.tables = append(scm.tables, tbl)
	scm.tablesByLowerName[lower] = tbl
	scm.tablesByRowType[tbl.rowType] = tbl
	scm.tablesByRowType[tbl.rowTypePtr] = tbl
}

func (scm *Schema) addKVTable(tbl *KVTable) {
	lower := strings.ToLower(tbl.name)

	if scm.tablesByLowerName[lower] != nil {
		panic(fmt.Errorf("non-KV table %s is already defined, and the namespace is shared", tbl.name))
	}
	if scm.kvtablesByLowerName[lower] != nil {
		panic(fmt.Errorf("KV table %s is already defined", tbl.name))
	}

	scm.kvtables = append(scm.kvtables, tbl)
	scm.kvtablesByLowerName[lower] = tbl
}

func (scm *Schema) Tables() []*Table {
	return append([]*Table(nil), scm.tables...)
}
func (scm *Schema) KVTables() []*KVTable {
	return append([]*KVTable(nil), scm.kvtables...)
}

func (scm *Schema) TableNamed(name string) *Table {
	return scm.tablesByLowerName[strings.ToLower(name)]
}

func (scm *Schema) KVTableNamed(name string) *KVTable {
	return scm.kvtablesByLowerName[strings.ToLower(name)]
}

func (scm *Schema) TableByRowType(rt reflect.Type) *Table {
	tbl := scm.tablesByRowType[rt]
	if tbl == nil {
		panic(fmt.Errorf("no table defined for row type %v", rt))
	}
	return tbl
}

func (scm *Schema) TableByRow(row any) *Table {
	rt := reflect.TypeOf(row)
	if rt.Kind() == reflect.Ptr && rt.Elem().Kind() == reflect.Struct {
		return scm.TableByRowType(rt)
	} else {
		panic(fmt.Errorf("expected pointer to a table row type, got %v", rt))
	}
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
