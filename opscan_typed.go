package edb

import (
	"github.com/andreyvit/edb/kvo"
	"iter"
)

// RecordScan is a reusable scan description. Each iteration opens a fresh
// cursor in the same transaction. Records borrow their transaction's storage;
// retain them beyond it only after Record.Own. Keys are decoded values.
type RecordScan[Key any] struct{ typedScan[Key] }

// RowScan uses the same scan rules but materializes owned native Go rows.
type RowScan[Row, Key any] struct{ typedScan[Key] }

type typedScan[Key any] struct {
	tx    *Tx
	table *TableDef
	index *IndexDef
	opt   ScanOptions
}

func (t typedTable[Key]) scan(txh Txish, opt ScanOptions) typedScan[Key] {
	return typedScan[Key]{tx: txh.DBTx(), table: t.tableDef, opt: opt}
}
func (i typedIndex[IndexKey, Key]) scan(txh Txish, opt ScanOptions) typedScan[Key] {
	return typedScan[Key]{tx: txh.DBTx(), table: i.index.table, index: i.index, opt: opt}
}

func (t Table[Key]) Scan(txh Txish) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, FullScan())}
}
func (t RowTable[Row, Key]) Scan(txh Txish) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, FullScan())}
}
func (i recordIndex[IndexKey, Key]) Scan(txh Txish) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, FullScan())}
}
func (i rowIndex[Row, IndexKey, Key]) Scan(txh Txish) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, FullScan())}
}

func (t Table[Key]) Exact(txh Txish, key Key) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, ExactScan(key))}
}
func (t RowTable[Row, Key]) Exact(txh Txish, key Key) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, ExactScan(key))}
}
func (i recordIndex[IndexKey, Key]) Exact(txh Txish, key IndexKey) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, ExactScan(key))}
}
func (i rowIndex[Row, IndexKey, Key]) Exact(txh Txish, key IndexKey) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, ExactScan(key))}
}

// Range includes both endpoints. From/After and Through/Before provide
// inclusive/exclusive one-sided bounds; Reversed changes traversal order.
func (t Table[Key]) Range(txh Txish, lower, upper Key) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, RangeScan(lower, upper, true, true))}
}
func (t RowTable[Row, Key]) Range(txh Txish, lower, upper Key) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, RangeScan(lower, upper, true, true))}
}
func (i recordIndex[IndexKey, Key]) Range(txh Txish, lower, upper IndexKey) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, RangeScan(lower, upper, true, true))}
}
func (i rowIndex[Row, IndexKey, Key]) Range(txh Txish, lower, upper IndexKey) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, RangeScan(lower, upper, true, true))}
}

func (t Table[Key]) From(txh Txish, key Key) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, LowerBoundScan(key, true))}
}
func (t RowTable[Row, Key]) From(txh Txish, key Key) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, LowerBoundScan(key, true))}
}
func (i recordIndex[IndexKey, Key]) From(txh Txish, key IndexKey) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, LowerBoundScan(key, true))}
}
func (i rowIndex[Row, IndexKey, Key]) From(txh Txish, key IndexKey) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, LowerBoundScan(key, true))}
}
func (t Table[Key]) After(txh Txish, key Key) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, LowerBoundScan(key, false))}
}
func (t RowTable[Row, Key]) After(txh Txish, key Key) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, LowerBoundScan(key, false))}
}
func (i recordIndex[IndexKey, Key]) After(txh Txish, key IndexKey) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, LowerBoundScan(key, false))}
}
func (i rowIndex[Row, IndexKey, Key]) After(txh Txish, key IndexKey) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, LowerBoundScan(key, false))}
}
func (t Table[Key]) Through(txh Txish, key Key) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, UpperBoundScan(key, true))}
}
func (t RowTable[Row, Key]) Through(txh Txish, key Key) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, UpperBoundScan(key, true))}
}
func (i recordIndex[IndexKey, Key]) Through(txh Txish, key IndexKey) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, UpperBoundScan(key, true))}
}
func (i rowIndex[Row, IndexKey, Key]) Through(txh Txish, key IndexKey) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, UpperBoundScan(key, true))}
}
func (t Table[Key]) Before(txh Txish, key Key) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, UpperBoundScan(key, false))}
}
func (t RowTable[Row, Key]) Before(txh Txish, key Key) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, UpperBoundScan(key, false))}
}
func (i recordIndex[IndexKey, Key]) Before(txh Txish, key IndexKey) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, UpperBoundScan(key, false))}
}
func (i rowIndex[Row, IndexKey, Key]) Before(txh Txish, key IndexKey) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, UpperBoundScan(key, false))}
}

// Prefix matches the first components of a composite key, using the declared
// key type. It is a tuple prefix, not a string prefix.
func (t Table[Key]) Prefix(txh Txish, key Key, components int) RecordScan[Key] {
	return RecordScan[Key]{t.scan(txh, ExactScan(key).Prefix(components))}
}
func (t RowTable[Row, Key]) Prefix(txh Txish, key Key, components int) RowScan[Row, Key] {
	return RowScan[Row, Key]{t.scan(txh, ExactScan(key).Prefix(components))}
}
func (i recordIndex[IndexKey, Key]) Prefix(txh Txish, key IndexKey, components int) RecordScan[Key] {
	return RecordScan[Key]{i.scan(txh, ExactScan(key).Prefix(components))}
}
func (i rowIndex[Row, IndexKey, Key]) Prefix(txh Txish, key IndexKey, components int) RowScan[Row, Key] {
	return RowScan[Row, Key]{i.scan(txh, ExactScan(key).Prefix(components))}
}

func (s RecordScan[Key]) Reversed() RecordScan[Key] {
	s.opt.Reverse = true
	return s
}
func (s RowScan[Row, Key]) Reversed() RowScan[Row, Key] {
	s.opt.Reverse = true
	return s
}

// Raw opens a cursor for low-level operations. It does not consume an earlier
// iteration; all scans and iterators must stay inside their transaction.
func (s typedScan[Key]) Raw() RawCursor {
	if s.index != nil {
		return s.tx.newIndexCursor(s.index, s.opt)
	}
	return s.tx.newTableCursor(s.table, s.opt)
}

// Keys decodes primary keys without loading or decoding row payloads.
func (s typedScan[Key]) Keys() iter.Seq[Key] {
	return func(yield func(Key) bool) {
		for c := s.Raw(); c.Next(); {
			if !yield(decodeTypedKey[Key](s.table, c.RawKey())) {
				return
			}
		}
	}
}

func (s RecordScan[Key]) Records() iter.Seq[kvo.Record] {
	return func(yield func(kvo.Record) bool) {
		for c := s.Raw(); c.Next(); {
			if !yield(s.record(c)) {
				return
			}
		}
	}
}
func (s RecordScan[Key]) Entries() iter.Seq2[Key, kvo.Record] {
	return func(yield func(Key, kvo.Record) bool) {
		for c := s.Raw(); c.Next(); {
			key := decodeTypedKey[Key](s.table, c.RawKey())
			if !yield(key, s.record(c)) {
				return
			}
		}
	}
}
func (s RowScan[Row, Key]) Rows() iter.Seq[*Row] {
	return func(yield func(*Row) bool) {
		Cursor[Row]{s.Raw()}.Rows()(yield)
	}
}
func (s RowScan[Row, Key]) Entries() iter.Seq2[Key, *Row] {
	return func(yield func(Key, *Row) bool) {
		for c := (Cursor[Row]{s.Raw()}); c.Next(); {
			key := decodeTypedKey[Key](s.table, c.RawKey())
			if !yield(key, c.Row()) {
				return
			}
		}
	}
}

func (s typedScan[Key]) record(c RawCursor) kvo.Record {
	r, _, err := s.tx.record(s.table, c.RawKey(), c.RawRow())
	if err != nil {
		panic(err)
	}
	return r
}
