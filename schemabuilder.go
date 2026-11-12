package edb

import (
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo"
)

type TableBuilder[Key any] struct {
	tbl          *TableDef
	nativeSchema kvo.NativeSchema
	migrators    []tableMigrator[Key]
}

// DefineTable registers a model-backed table. The model's first registered Go
// struct supplies native storage and its first field supplies the primary key.
// Key must exactly match that field's type. The schema version defaults to 1.
// SetRecordStorage selects Fast2 or Compact2 instead of MessagePack for new keys.
func DefineTable[Key any](scm *Schema, name string, model *kvo.Model, f func(b *TableBuilder[Key])) *Table[Key] {
	scm.init()
	if model == nil {
		panic(fmt.Sprintf("DefineTable(%s): model is required", name))
	}
	types := model.Type().(*kvo.EntityType).NativeTypes()
	if len(types) == 0 {
		panic(fmt.Sprintf("DefineTable(%s): model needs a registered native struct", name))
	}
	rowPtrType := reflect.PointerTo(types[0])
	tbl := &TableDef{
		schema:          scm,
		kvoModel:        model,
		pos:             len(scm.tables),
		name:            name,
		latestSchemaVer: 1,
		rowTypePtr:      rowPtrType,
		rowType:         rowPtrType.Elem(),
		rowInfo:         reflectTypeWithoutCache(rowPtrType),
		valueEnc:        defaultValueEncoding,
		keyStringSep:    "|",
		indicesByName:   make(map[string]*IndexDef),
	}
	tbl.keyType = tbl.rowInfo.keyField.Type
	if keyType := reflect.TypeFor[Key](); keyType != tbl.keyType {
		panic(fmt.Errorf("DefineTable(%s): Key %v does not match primary key field %v.%s of type %v", name, keyType, tbl.rowType, tbl.rowInfo.keyField.Name, tbl.keyType))
	}
	tbl.keyEnc = flatEncodingOf(tbl.keyType)
	scm.addTable(tbl)
	tbl.zeroKey = tbl.keyEnc.encode(nil, reflect.Zero(tbl.keyType))

	b := TableBuilder[Key]{
		tbl: tbl,
	}
	f(&b)
	for _, m := range b.migrators {
		if m.toVer > tbl.latestSchemaVer {
			panic(fmt.Sprintf("table %s: migrator version %d exceeds schema version %d", name, m.toVer, tbl.latestSchemaVer))
		}
	}
	if tbl.binding == nil {
		tbl.projection = kvo.Project(tbl.rowType, model, nil)
	}
	if b.nativeSchema != nil {
		if tbl.binding != nil {
			tbl.binding = tbl.binding.WithNativeSchema(b.nativeSchema)
		} else {
			tbl.projection = tbl.projection.WithNativeSchema(b.nativeSchema)
		}
	}
	tbl.validateRecordPrimaryKey()

	return &Table[Key]{typedTable[Key]{tbl}}
}

// SetIndexer builds secondary indexes from logical properties on any row format.
// The primary key is supplied separately; records need no stored key property.
// The record and its bytes are borrowed, read-only, and valid only during f.
// Use a presence-significant property when absent and zero index differently.
func (b *TableBuilder[Key]) SetIndexer(f func(Key, kvo.Record, *IndexBuilder)) {
	t := b.tbl
	if t.recordIndexer != nil || f == nil {
		panic("choose one non-nil table indexer")
	}
	t.recordIndexer = func(r kvo.Record, ib *IndexBuilder) {
		f(decodeTypedKey[Key](t, ib.key), r, ib)
	}
}

type tableMigrator[Key any] struct {
	toVer uint64
	apply func(*Tx, Key, *kvo.Editor)
}

// AddMigrator runs f when the stored version is below toVer. Eligible callbacks
// run in registration order, each comparing against the original stored version.
// toVer must be positive and no greater than the final table schema version.
// Native editors update structs in place; encoded editors preserve untouched
// fields. Reads do not persist migrations; saves and reindexing do.
// The callback must not change the primary key or retain the editor.
func (b *TableBuilder[Key]) AddMigrator(toVer uint64, f func(tx *Tx, key Key, edit *kvo.Editor)) {
	if toVer == 0 || f == nil {
		panic("AddMigrator requires a positive version and non-nil callback")
	}
	b.migrators = append(b.migrators, tableMigrator[Key]{toVer, f})
	if b.tbl.migrator == nil {
		b.tbl.migrator = func(tx *Tx, raw []byte, edit *kvo.Editor, oldVer uint64) {
			var key Key
			decoded := false
			for _, m := range b.migrators {
				if oldVer < m.toVer {
					if !decoded {
						key = decodeTypedKey[Key](b.tbl, raw)
						decoded = true
					}
					m.apply(tx, key, edit)
				}
			}
		}
	}
}

func (b *TableBuilder[Key]) Tag(tag *Tag) {
	b.tbl.tags = append(b.tbl.tags, tag)
}

func (b *TableBuilder[Key]) SetSchemaVersion(ver uint64) {
	b.tbl.latestSchemaVer = ver
}

func (b *TableBuilder[Key]) SuppressContentWhenLogging() {
	b.tbl.suppressContent = true
}

// SetNativeSchema shares native field operations and value types with the host VM.
func (b *TableBuilder[Key]) SetNativeSchema(schema kvo.NativeSchema) {
	if schema == nil || b.nativeSchema != nil {
		panic("choose one non-nil native schema")
	}
	b.nativeSchema = schema
}
