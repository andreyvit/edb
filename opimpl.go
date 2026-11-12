package edb

import "reflect"

func tableOf[T any](tx *Tx) *TableDef {
	return tx.Schema().TableByRowType(reflect.TypeOf((*T)(nil)))
}
