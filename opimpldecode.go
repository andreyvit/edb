package edb

import (
	"fmt"
	"log"
	"reflect"
)

type ValueMeta struct {
	SchemaVer uint64
	ModCount  uint64
}

func (vm ValueMeta) Exists() bool {
	return vm.ModCount != 0
}
func (vm ValueMeta) IsMissing() bool {
	return vm.ModCount == 0
}
func (vm ValueMeta) IsModified(oldMeta ValueMeta) bool {
	return vm.ModCount > oldMeta.ModCount
}

func anyToRow[Row any](v any) *Row {
	if v == nil {
		return nil
	}
	return v.(*Row)
}

func valToRow[Row any](val reflect.Value) *Row {
	if val.IsValid() {
		return val.Interface().(*Row)
	} else {
		return nil
	}
}

func valToAny(val reflect.Value) any {
	if val.IsValid() {
		return val.Interface()
	} else {
		return nil
	}
}

func keyRawToVal(raw []byte, tbl *Table) reflect.Value {
	if raw == nil {
		return reflect.Value{}
	} else {
		return tbl.DecodeKeyVal(raw)
	}
}

func decodeTableRow(tbl *Table, keyRaw, valueRaw []byte, migrationTx *Tx, isMemento bool) (rowVal reflect.Value, rowMeta ValueMeta, err error) {
	var vle value
	decodeTableValue(&vle, tbl, keyRaw, valueRaw, isMemento)
	rowVal, _, rowMeta, err = decodeTableRowFromValue(&vle, tbl, keyRaw, migrationTx)
	return
}

func decodeTableRowFromValue(vle *value, tbl *Table, keyRaw []byte, migrationTx *Tx) (rowVal, keyVal reflect.Value, rowMeta ValueMeta, err error) {
	rowVal = tbl.newRow(vle.SchemaVer)
	keyVal = tbl.RowKeyVal(rowVal)
	tbl.DecodeKeyValInto(keyVal, keyRaw)

	keyVal = tbl.DecodeKeyVal(keyRaw)

	err = vle.decodeRowInto(rowVal)
	if err != nil {
		err = tableErrf(tbl, nil, keyRaw, err, "data")
		return
	}
	tbl.rowInfo.keyValue(rowVal).Set(keyVal)

	rowMeta = vle.ValueMeta()

	if rowMeta.SchemaVer < tbl.latestSchemaVer && tbl.migrator != nil {
		tbl.migrator(migrationTx, rowVal.Interface(), rowMeta.SchemaVer)
	}
	return
}

func decodeTableValue(vle *value, tbl *Table, keyRaw, valueRaw []byte, isMemento bool) {
	err := vle.decode(valueRaw, isMemento)
	if err != nil {
		err := tableErrf(tbl, nil, keyRaw, err, "")
		log.Printf("** ERROR: %v", err)
		panic(err)
	}
}

func (vle *value) decodeRowInto(rowVal reflect.Value) error {
	return vle.Flags.encoding().DecodeValue(vle.Data, rowVal)
}

func decodeIndexTableKey(indexKeyRaw []byte, indexKeyTup tuple, indexVal []byte, idx *Index) ([]byte, tuple) {
	if indexKeyTup == nil {
		indexKeyTup = decodeIndexKey(indexKeyRaw, idx)
	}
	if idx.isUnique {
		return decodeUniqueIndexTableKey(indexKeyRaw, indexVal, idx), indexKeyTup
	} else {
		return extractUniqueIndexKey(indexKeyTup)
	}
}

func decodeUniqueIndexTableKey(indexKeyRaw, indexVal []byte, idx *Index) []byte {
	indexValTup, err := decodeTuple(indexVal)
	if err != nil {
		panic(fmt.Errorf("%s: invalid index value tuple for key %x, value is %x: %w", idx.FullName(), indexKeyRaw, indexVal, err))
	}
	if len(indexValTup) != 1 {
		panic(fmt.Errorf("%s: invalid index value tuple for key %x: got %d els, wanted %d, value is 0x%x", idx.FullName(), indexKeyRaw, len(indexValTup), 1, indexVal))
	}
	return indexValTup[0]
}

func decodeNonUniqueIndexTableKey(indexKeyRaw []byte, idx *Index) (tuple, []byte) {
	indexKeyTup := decodeIndexKey(indexKeyRaw, idx)
	return indexKeyTup, indexKeyTup[len(indexKeyTup)-1]
}

func decodeIndexKey(indexKeyRaw []byte, idx *Index) tuple {
	indexKeyTup, err := decodeTuple(indexKeyRaw)
	if err != nil {
		panic(fmt.Errorf("%s: invalid index key tuple %x: %w", idx.FullName(), indexKeyRaw, err))
	}
	return indexKeyTup
}

func extractUniqueIndexKey(indexKeyTup tuple) ([]byte, tuple) {
	n := len(indexKeyTup)
	return indexKeyTup[n-1], indexKeyTup[:n-1]
}

func decodeIndexRow(idx *Index, indexKeyRaw, indexValRaw []byte) (indexKey tuple, keyRaw []byte) {
	indexKeyTup := must(decodeTuple(indexKeyRaw))
	indexValTup := must(decodeTuple(indexValRaw))

	if idx.isUnique {
		if len(indexValTup) != 1 {
			panic(fmt.Errorf("%s: invalid index value tuple for key %x: got %d els, wanted %d, value is 0x%x", idx.FullName(), indexKeyRaw, len(indexValTup), 1, indexValRaw))
		}
		return indexKeyTup, indexValTup[0]
	} else {
		if len(indexValTup) != 0 {
			panic(fmt.Errorf("%s: invalid index value tuple for key %x: got %d els, wanted %d, value is 0x%x", idx.FullName(), indexKeyRaw, len(indexValTup), 0, indexValRaw))
		}

		n := len(indexKeyTup)
		return indexKeyTup[:n-1], indexKeyTup[n-1]
	}
}
