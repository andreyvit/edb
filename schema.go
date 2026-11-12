package edb

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
)

type Tablish interface {
	TableName() string
	HasTag(tag *Tag) bool
}

var indexBuilderPtrType = reflect.TypeOf((*IndexBuilder)(nil))
var anyType = reflect.TypeOf((*any)(nil)).Elem()

const dataBucketName = "data"

type Schema struct {
	Name                string
	tables              []*TableDef
	tablesByLowerName   map[string]*TableDef
	tablesByRowType     map[reflect.Type]*TableDef
	tablePositions      map[*TableDef]int
	maps                []*KVMap
	mapsByLowerName     map[string]*KVMap
	kvtables            []*KVTable
	kvtablesByLowerName map[string]*KVTable
}

func (scm *Schema) init() {
	if scm.tablesByLowerName == nil {
		scm.tablesByLowerName = make(map[string]*TableDef)
		scm.tablesByRowType = make(map[reflect.Type]*TableDef)
		scm.tablePositions = make(map[*TableDef]int)
		scm.mapsByLowerName = make(map[string]*KVMap)
		scm.kvtablesByLowerName = make(map[string]*KVTable)
	}
}

// Include composes all table families and singleton maps without modifying peer.
// A conflicting declaration panics and leaves the receiving schema unchanged.
func (scm *Schema) Include(peer *Schema) {
	scm.init()
	next := *scm
	next.tables = slices.Clone(scm.tables)
	next.kvtables = slices.Clone(scm.kvtables)
	next.maps = slices.Clone(scm.maps)
	next.tablesByLowerName = maps.Clone(scm.tablesByLowerName)
	next.tablesByRowType = maps.Clone(scm.tablesByRowType)
	next.tablePositions = maps.Clone(scm.tablePositions)
	next.kvtablesByLowerName = maps.Clone(scm.kvtablesByLowerName)
	next.mapsByLowerName = maps.Clone(scm.mapsByLowerName)
	for _, tbl := range peer.tables {
		next.addTable(tbl)
	}
	for _, tbl := range peer.kvtables {
		next.addKVTable(tbl)
	}
	for _, mp := range peer.maps {
		next.addMap(mp)
	}
	*scm = next
}

func (scm *Schema) checkName(name string) {
	lower := strings.ToLower(name)
	if scm.tablesByLowerName[lower] != nil {
		panic(fmt.Errorf("table %s is already defined", name))
	}
	if scm.kvtablesByLowerName[lower] != nil {
		panic(fmt.Errorf("KV table %s is already defined, and the namespace is shared", name))
	}
	if scm.mapsByLowerName[lower] != nil {
		panic(fmt.Errorf("singleton map %s is already defined, and the namespace is shared", name))
	}
}

func (scm *Schema) addTable(tbl *TableDef) {
	scm.init()
	scm.checkName(tbl.name)
	if scm.tablesByRowType[tbl.rowType] != nil {
		panic(fmt.Errorf("row type %v is already used", tbl.rowType))
	}
	scm.tablePositions[tbl] = len(scm.tables)
	scm.tables = append(scm.tables, tbl)
	scm.tablesByLowerName[strings.ToLower(tbl.name)] = tbl
	scm.tablesByRowType[tbl.rowType] = tbl
	scm.tablesByRowType[tbl.rowTypePtr] = tbl
}

func (scm *Schema) addKVTable(tbl *KVTable) {
	scm.init()
	scm.checkName(tbl.name)
	scm.kvtables = append(scm.kvtables, tbl)
	scm.kvtablesByLowerName[strings.ToLower(tbl.name)] = tbl
}

func (scm *Schema) addMap(mp *KVMap) {
	scm.init()
	scm.checkName(mp.name)
	scm.maps = append(scm.maps, mp)
	scm.mapsByLowerName[strings.ToLower(mp.name)] = mp
}

func (scm *Schema) Tables() []*TableDef {
	return slices.Clone(scm.tables)
}
func (scm *Schema) KVTables() []*KVTable {
	return slices.Clone(scm.kvtables)
}

func (scm *Schema) TableNamed(name string) *TableDef {
	return scm.tablesByLowerName[strings.ToLower(name)]
}

func (scm *Schema) KVTableNamed(name string) *KVTable {
	return scm.kvtablesByLowerName[strings.ToLower(name)]
}

func (scm *Schema) TableByRowType(rt reflect.Type) *TableDef {
	tbl := scm.tablesByRowType[rt]
	if tbl == nil {
		panic(fmt.Errorf("no table defined for row type %v", rt))
	}
	return tbl
}

func (scm *Schema) TableByRow(row any) *TableDef {
	rt := reflect.TypeOf(row)
	if rt.Kind() == reflect.Ptr && rt.Elem().Kind() == reflect.Struct {
		return scm.TableByRowType(rt)
	} else {
		panic(fmt.Errorf("expected pointer to a table row type, got %v", rt))
	}
}

type KVMap struct {
	name           string
	singletonTypes map[string]reflect.Type
}

func AddKVMap(scm *Schema, name string) *KVMap {
	mp := &KVMap{
		name: name,
	}
	scm.addMap(mp)
	return mp
}

// AddSingletonKey declares a singleton's value type. A key can be declared only
// once within a map, even if a second declaration uses the same value type.
func AddSingletonKey[T any](mp *KVMap, key string) *SKey[T] {
	if previous, exists := mp.singletonTypes[key]; exists {
		panic(fmt.Errorf("singleton %s.%s is already defined with value type %v", mp.name, key, previous))
	}
	if mp.singletonTypes == nil {
		mp.singletonTypes = make(map[string]reflect.Type)
	}
	mp.singletonTypes[key] = reflect.TypeFor[T]()
	sk := &SKey[T]{
		mp:       mp,
		keyBytes: []byte(key),
		valueEnc: defaultValueEncoding,
	}
	return sk
}

// SKey identifies a singleton storing a value of type T. SGet and SPut carry
// this type through to their destination and source arguments.
type SKey[T any] struct {
	_        [0]*T // Preserve type identity without storing a value.
	mp       *KVMap
	keyBytes []byte
	valueEnc encodingMethod
}

func (sk *SKey[T]) String() string {
	return sk.mp.name + "." + string(sk.keyBytes)
}

func (sk *SKey[T]) Raw() []byte {
	return sk.keyBytes
}

func (sk *SKey[T]) ValueType() reflect.Type {
	return reflect.TypeFor[T]()
}
