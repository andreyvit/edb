package edb

// AnyTable accepts a typed table or its shared definition. Database operations
// resolve the definition once; cursors and caches retain the concrete pointer.
type AnyTable interface {
	TableDef() *TableDef
}

func (t *TableDef) TableDef() *TableDef { return t }

// AnyIndex accepts a typed index or its shared definition.
type AnyIndex interface{ IndexDef() *IndexDef }

func (i *IndexDef) IndexDef() *IndexDef { return i }

type indexDef = IndexDef

// Index declares the index-key type. Table methods supply the primary-key type
// when looking up records or scanning index entries.
type Index[IndexKey any] struct{ *indexDef }

func (i *Index[IndexKey]) IndexDef() *IndexDef {
	if i == nil {
		return nil
	}
	return i.indexDef
}
