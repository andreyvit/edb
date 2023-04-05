package edb

import "reflect"

func tableOf[T any](tx *Tx) *Table {
	return tx.tableByRowType(reflect.TypeOf((*T)(nil)))
}
