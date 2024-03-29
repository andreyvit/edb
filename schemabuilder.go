package edb

import (
	"fmt"
	"reflect"
)

type TableBuilder[Row, Key any] struct {
	tbl *Table
}

func DefineTable[Row, Key any](scm *Schema, name string, f func(b *TableBuilder[Row, Key])) *Table {
	scm.init()
	rowPtrType := reflect.TypeFor[*Row]()
	if rowPtrType.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("DefineTable(%s): Row must be a struct", name))
	}
	tbl := &Table{
		schema:          scm,
		name:            name,
		latestSchemaVer: 1,
		buck:            makeBucketName(name),
		rowTypePtr:      rowPtrType,
		rowType:         rowPtrType.Elem(),
		rowInfo:         reflectTypeWithoutCache(rowPtrType),
		valueEnc:        defaultValueEncoding,
		keyStringSep:    "|",
		indicesByName:   make(map[string]*Index),
		indexer:         nopIndexer,
	}
	tbl.keyType = tbl.rowInfo.keyField.Type
	tbl.keyEnc = flatEncodingOf(tbl.keyType)
	scm.addTable(tbl)
	tbl.zeroKey = tbl.keyEnc.encode(nil, reflect.Zero(tbl.keyType))

	b := TableBuilder[Row, Key]{
		tbl: tbl,
	}
	f(&b)
	return tbl
}

func (b *TableBuilder[Row, Key]) Indexer(f func(row *Row, ib *IndexBuilder)) {
	b.tbl.indexer = func(row any, ib *IndexBuilder) {
		f(row.(*Row), ib)
	}
}

func (b *TableBuilder[Row, Key]) Migrate(f func(tx *Tx, row *Row, oldVer uint64)) {
	b.tbl.migrator = func(tx *Tx, row any, oldVer uint64) {
		f(tx, row.(*Row), oldVer)
	}
}

func (b *TableBuilder[Row, Key]) AddIndex(idx *Index) {
	b.tbl.AddIndex(idx)
}

func (b *TableBuilder[Row, Key]) SetSchemaVersion(ver uint64) {
	b.tbl.latestSchemaVer = ver
}

func (b *TableBuilder[Row, Key]) SuppressContentWhenLogging() {
	b.tbl.suppressContent = true
}

func nopIndexer(row any, ib *IndexBuilder) {}
