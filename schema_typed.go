package edb

import (
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo"
)

// Table is a table's encoding-independent access API with a declared key
// type. DefineTable constructs it together with its shared definition.
type Table[Key any] struct{ typedTable[Key] }

// RowTable is the native Go row counterpart of Table.
type RowTable[Row, Key any] struct{ typedTable[Key] }

type tableDef = TableDef

type typedTable[Key any] struct{ *tableDef }

// Typed binds a dynamically obtained definition, validating its primary-key type
// and record model. Statically declared tables need no separate binding.
func (t *TableDef) Typed[Key any]() Table[Key] {
	checkTableKey[Key](t)
	if t.Model() == nil {
		panic(fmt.Errorf("%s: %w", t.name, ErrNoRecordBinding))
	}
	return Table[Key]{typedTable[Key]{t}}
}

// Rows validates both the native row type and the primary key type.
func (t *TableDef) Rows[Row, Key any]() RowTable[Row, Key] {
	checkTableKey[Key](t)
	if row := reflect.TypeFor[Row](); row != t.rowType {
		panic(fmt.Errorf("%s: row type is %v, got %v", t.name, t.rowType, row))
	}
	return RowTable[Row, Key]{typedTable[Key]{t}}
}

func checkTableKey[Key any](t *TableDef) {
	if key := reflect.TypeFor[Key](); key != t.keyType {
		panic(fmt.Errorf("%s: primary key type is %v, got %v", t.name, t.keyType, key))
	}
}

func (t typedTable[Key]) Table() *TableDef  { return t.tableDef }
func (t typedTable[Key]) Model() *kvo.Model { return t.tableDef.Model() }

func (t Table[Key]) Rows[Row any]() RowTable[Row, Key] {
	return t.tableDef.Rows[Row, Key]()
}
func (t RowTable[Row, Key]) Records() Table[Key] { return t.tableDef.Typed[Key]() }

// recordIndex carries both index-key and primary-key types. Lookup returns the
// first matching row for non-unique indexes, as the traditional Lookup does.
type recordIndex[IndexKey, Key any] struct{ typedIndex[IndexKey, Key] }

// rowIndex is the native Go row counterpart of recordIndex.
type rowIndex[Row, IndexKey, Key any] struct{ typedIndex[IndexKey, Key] }

type typedIndex[IndexKey, Key any] struct{ index *IndexDef }

func (t Table[Key]) bindIndex[IndexKey any](idx *IndexDef) recordIndex[IndexKey, Key] {
	return recordIndex[IndexKey, Key]{bindTypedIndex[IndexKey](t.typedTable, idx)}
}
func (t RowTable[Row, Key]) bindIndex[IndexKey any](idx *IndexDef) rowIndex[Row, IndexKey, Key] {
	return rowIndex[Row, IndexKey, Key]{bindTypedIndex[IndexKey](t.typedTable, idx)}
}

func bindTypedIndex[IndexKey, Key any](t typedTable[Key], idx *IndexDef) typedIndex[IndexKey, Key] {
	if idx.table != t.tableDef {
		panic(fmt.Errorf("%s: index %s belongs to another table", t.tableDef.name, idx.name))
	}
	if key := reflect.TypeFor[IndexKey](); key != idx.keyType() {
		panic(fmt.Errorf("%s: index key type is %v, got %v", idx.FullName(), idx.keyType(), key))
	}
	return typedIndex[IndexKey, Key]{idx}
}

func (i typedIndex[IndexKey, Key]) Index() *IndexDef { return i.index }
func (i typedIndex[IndexKey, Key]) Table() *TableDef { return i.index.table }

func (i recordIndex[IndexKey, Key]) Rows[Row any]() rowIndex[Row, IndexKey, Key] {
	return i.Table().Rows[Row, Key]().bindIndex[IndexKey](i.index)
}
func (i rowIndex[Row, IndexKey, Key]) Records() recordIndex[IndexKey, Key] {
	return i.Table().Typed[Key]().bindIndex[IndexKey](i.index)
}
