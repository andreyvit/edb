package edb

import (
	"fmt"
	"strings"

	"github.com/cespare/xxhash/v2"
)

type schemaNamedObject interface {
	Name() string
	NameHash() uint64
}

// Hash the same canonical name used by the shared schema namespace. The algorithm
// is xxHash64, seed zero, over UTF-8 bytes; it must stay stable for external IDs.
func schemaNameHash(name string) uint64 {
	return xxhash.Sum64String(strings.ToLower(name))
}

func (scm *Schema) registerNameHash(obj schemaNamedObject) {
	hash := obj.NameHash()
	if previous := scm.objectsByNameHash[hash]; previous != nil {
		panic(fmt.Errorf("name hash collision %016x: %q (%T) and %q (%T)", hash, previous.Name(), previous, obj.Name(), obj))
	}
	scm.objectsByNameHash[hash] = obj
}

// NameHash returns the cached hash of a registered table, KV table or singleton
// map. Lookup is case-insensitive, like TableNamed; an unknown name panics.
func (scm *Schema) NameHash(name string) uint64 {
	lower := strings.ToLower(name)
	if tbl := scm.tablesByLowerName[lower]; tbl != nil {
		return tbl.NameHash()
	}
	if tbl := scm.kvtablesByLowerName[lower]; tbl != nil {
		return tbl.NameHash()
	}
	if mp := scm.mapsByLowerName[lower]; mp != nil {
		return mp.NameHash()
	}
	panic(fmt.Errorf("no schema object named %q", name))
}

// TableByNameHash returns nil for an unknown hash or another object kind.
func (scm *Schema) TableByNameHash(hash uint64) *TableDef {
	tbl, _ := scm.objectsByNameHash[hash].(*TableDef)
	return tbl
}

// KVTableByNameHash returns nil for an unknown hash or another object kind.
func (scm *Schema) KVTableByNameHash(hash uint64) *KVTable {
	tbl, _ := scm.objectsByNameHash[hash].(*KVTable)
	return tbl
}

// KVMapByNameHash returns nil for an unknown hash or another object kind.
func (scm *Schema) KVMapByNameHash(hash uint64) *KVMap {
	mp, _ := scm.objectsByNameHash[hash].(*KVMap)
	return mp
}

// NameHash returns xxHash64 of the lowercase registered name, cached at declaration.
func (tbl *TableDef) NameHash() uint64 { return tbl.nameHash }

// NameHash returns xxHash64 of the lowercase registered name, cached at declaration.
func (tbl *KVTable) NameHash() uint64 { return tbl.nameHash }

func (mp *KVMap) Name() string { return mp.name }

// NameHash returns xxHash64 of the lowercase registered name, cached at declaration.
func (mp *KVMap) NameHash() uint64 { return mp.nameHash }
