package edb

import "reflect"

func tableOf[T any](tx *Tx) *Table {
	return tx.Schema().TableByRowType(reflect.TypeOf((*T)(nil)))
}
