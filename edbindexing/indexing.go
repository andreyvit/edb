// Package edbindexing supplies typed index callbacks for common property reads.
// Construct callbacks during schema registration. They borrow record data and
// yield keys without materializing native structs or collections.
package edbindexing

import (
	"time"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

// ByField yields the field value, including its zero value.
func ByField[T any, P kvo.Property[T]](p P) func(*edb.Tx, values.Value, kvo.Record, func(T)) {
	return func(_ *edb.Tx, _ values.Value, row kvo.Record, yield func(T)) {
		yield(row.Get(p))
	}
}

// ByFieldIfNonZero omits values equal to Go's zero value for T.
// For timestamps, use ByTimeFieldIfNonZero to apply time.Time.IsZero.
func ByFieldIfNonZero[T comparable, P kvo.Property[T]](p P) func(*edb.Tx, values.Value, kvo.Record, func(T)) {
	return func(_ *edb.Tx, _ values.Value, row kvo.Record, yield func(T)) {
		var zero T
		if v := row.Get(p); v != zero {
			yield(v)
		}
	}
}

// ByTimeFieldIfNonZero omits zero instants regardless of their time zone.
func ByTimeFieldIfNonZero[P kvo.Property[time.Time]](p P) func(*edb.Tx, values.Value, kvo.Record, func(time.Time)) {
	return func(_ *edb.Tx, _ values.Value, row kvo.Record, yield func(time.Time)) {
		if v := row.Get(p); !v.IsZero() {
			yield(v)
		}
	}
}

// ByScalarList yields every element in order, including zero and duplicate
// elements. EDB deduplicates index entries; this callback does not filter them.
func ByScalarList[T any, P kvo.Property[kvo.List[T]]](p P) func(*edb.Tx, values.Value, kvo.Record, func(T)) {
	return func(_ *edb.Tx, _ values.Value, row kvo.Record, yield func(T)) {
		// Range-over-func closures escape in this generic callback; keep the cursor on the stack.
		cursor := row.Get(p).Cursor()
		for v, ok := cursor.Next(); ok; v, ok = cursor.Next() {
			yield(v)
		}
	}
}
