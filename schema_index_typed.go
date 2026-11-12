package edb

import "github.com/andreyvit/edb/kvo"

func (t Table[Key]) IndexScan[IndexKey any](txh Txish, idx *Index[IndexKey]) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, FullScan())}
}
func (t Table[Key]) ExactIndexScan[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, ExactScan(key))}
}
func (t Table[Key]) RangeIndexScan[IndexKey any](txh Txish, idx *Index[IndexKey], lower, upper IndexKey) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, RangeScan(lower, upper, true, true))}
}
func (t Table[Key]) IndexScanFrom[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, LowerBoundScan(key, true))}
}
func (t Table[Key]) IndexScanAfter[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, LowerBoundScan(key, false))}
}
func (t Table[Key]) IndexScanThrough[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, UpperBoundScan(key, true))}
}
func (t Table[Key]) IndexScanBefore[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, UpperBoundScan(key, false))}
}
func (t Table[Key]) PrefixIndexScan[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey, components int) RecordScan[Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RecordScan[Key]{i.scan(txh, ExactScan(key).Prefix(components))}
}
func (t Table[Key]) Lookup[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) (Key, kvo.Record) {
	return t.bindIndex[IndexKey](idx.IndexDef()).Lookup(txh, key)
}
func (t Table[Key]) TryLookup[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) (Key, kvo.Record, ValueMeta, error) {
	return t.bindIndex[IndexKey](idx.IndexDef()).TryLookup(txh, key)
}
func (t Table[Key]) LookupKey[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) (Key, bool) {
	return t.bindIndex[IndexKey](idx.IndexDef()).LookupKey(txh, key)
}
func (t RowTable[Row, Key]) IndexScan[IndexKey any](txh Txish, idx *Index[IndexKey]) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, FullScan())}
}
func (t RowTable[Row, Key]) ExactIndexScan[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, ExactScan(key))}
}
func (t RowTable[Row, Key]) RangeIndexScan[IndexKey any](txh Txish, idx *Index[IndexKey], lower, upper IndexKey) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, RangeScan(lower, upper, true, true))}
}
func (t RowTable[Row, Key]) IndexScanFrom[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, LowerBoundScan(key, true))}
}
func (t RowTable[Row, Key]) IndexScanAfter[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, LowerBoundScan(key, false))}
}
func (t RowTable[Row, Key]) IndexScanThrough[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, UpperBoundScan(key, true))}
}
func (t RowTable[Row, Key]) IndexScanBefore[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, UpperBoundScan(key, false))}
}
func (t RowTable[Row, Key]) PrefixIndexScan[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey, components int) RowScan[Row, Key] {
	i := bindTypedIndex[IndexKey](t.typedTable, idx.IndexDef())
	return RowScan[Row, Key]{i.scan(txh, ExactScan(key).Prefix(components))}
}
func (t RowTable[Row, Key]) Lookup[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) (Key, *Row) {
	return t.bindIndex[IndexKey](idx.IndexDef()).Lookup(txh, key)
}
func (t RowTable[Row, Key]) TryLookup[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) (Key, *Row, ValueMeta, error) {
	return t.bindIndex[IndexKey](idx.IndexDef()).TryLookup(txh, key)
}
func (t RowTable[Row, Key]) LookupKey[IndexKey any](txh Txish, idx *Index[IndexKey], key IndexKey) (Key, bool) {
	return t.bindIndex[IndexKey](idx.IndexDef()).LookupKey(txh, key)
}
