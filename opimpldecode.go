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

func keyRawToVal(raw []byte, tbl *TableDef) reflect.Value {
	if raw == nil {
		return reflect.Value{}
	} else {
		return tbl.DecodeKeyVal(raw)
	}
}

func decodeTableRow(tbl *TableDef, keyRaw, valueRaw []byte, migrationTx *Tx, isMemento bool) (rowVal reflect.Value, rowMeta ValueMeta, err error) {
	var vle value
	decodeTableValue(&vle, tbl, keyRaw, valueRaw, isMemento)
	return decodeTableRowFromValue(&vle, tbl, keyRaw, migrationTx)
}

func decodeTableRowFromValue(vle *value, tbl *TableDef, keyRaw []byte, tx *Tx) (reflect.Value, ValueMeta, error) {
	row := tbl.newRow(vle.SchemaVer)
	meta, err := decodeRowValueInto(row, vle, tbl, keyRaw, tx)
	return row, meta, err
}

func decodeRowValueInto(row reflect.Value, vle *value, tbl *TableDef, keyRaw []byte, tx *Tx) (ValueMeta, error) {
	field := tbl.RowKeyVal(row)
	if vle.Flags == vfKVO {
		// KVO excludes the primary key and clears the destination. Supply
		// the authoritative key once, after decoding the other fields.
		if err := tbl.decodeRowInto(vle, row); err != nil {
			return ValueMeta{}, tableErrf(tbl, nil, keyRaw, err, "data")
		}
		if err := tbl.TryDecodeKeyValInto(field, keyRaw); err != nil {
			return ValueMeta{}, err
		}
	} else {
		// Custom MessagePack decoders may inspect or overwrite their key.
		if err := tbl.TryDecodeKeyValInto(field, keyRaw); err != nil {
			return ValueMeta{}, err
		}
		key := tbl.DecodeKeyVal(keyRaw)
		if err := tbl.decodeRowInto(vle, row); err != nil {
			return ValueMeta{}, tableErrf(tbl, nil, keyRaw, err, "data")
		}
		field.Set(key)
	}
	meta := vle.ValueMeta()
	if meta.SchemaVer < tbl.latestSchemaVer && tbl.migrator != nil {
		tbl.migrateRecord(tx, keyRaw, tbl.nativeRecord(row), meta.SchemaVer)
	}
	return meta, nil
}

func decodeTableValue(vle *value, tbl *TableDef, keyRaw, valueRaw []byte, isMemento bool) {
	err := vle.decode(valueRaw, isMemento)
	if err != nil {
		err := tableErrf(tbl, nil, keyRaw, err, "")
		log.Printf("** ERROR: %v", err)
		panic(err)
	}
}

func (tbl *TableDef) decodeRowInto(vle *value, rowVal reflect.Value) error {
	if vle.Flags == vfKVO {
		if tbl.binding == nil {
			return ErrNoRecordBinding
		}
		return tbl.binding.TryDecode(vle.Data, rowVal.Elem())
	}
	return MsgPack.DecodeValue(vle.Data, rowVal)
}

func decodeIndexTableKey(indexKeyRaw []byte, indexKeyTup tuple, indexVal []byte, idx *IndexDef) ([]byte, tuple) {
	if indexKeyTup == nil {
		indexKeyTup = decodeIndexKey(indexKeyRaw, idx)
	}
	if idx.isUnique {
		return decodeUniqueIndexTableKey(indexKeyRaw, indexVal, idx), indexKeyTup
	} else {
		return extractUniqueIndexKey(indexKeyTup)
	}
}

func decodeUniqueIndexTableKey(indexKeyRaw, indexVal []byte, idx *IndexDef) []byte {
	var scratch [1][]byte
	indexValTup, err := decodeTupleInto(scratch[:0], indexVal)
	if err != nil {
		panic(fmt.Errorf("%s: invalid index value tuple for key %x, value is %x: %w", idx.FullName(), indexKeyRaw, indexVal, err))
	}
	if len(indexValTup) != 1 {
		panic(fmt.Errorf("%s: invalid index value tuple for key %x: got %d els, wanted %d, value is 0x%x", idx.FullName(), indexKeyRaw, len(indexValTup), 1, indexVal))
	}
	return indexValTup[0]
}

func decodeNonUniqueIndexTableKey(indexKeyRaw []byte, idx *IndexDef) (tuple, []byte) {
	indexKeyTup := decodeIndexKey(indexKeyRaw, idx)
	return indexKeyTup, indexKeyTup[len(indexKeyTup)-1]
}

func decodeIndexKey(indexKeyRaw []byte, idx *IndexDef) tuple {
	return decodeIndexKeyInto(nil, indexKeyRaw, idx)
}
func decodeIndexKeyInto(scratch tuple, indexKeyRaw []byte, idx *IndexDef) tuple {
	indexKeyTup, err := decodeTupleInto(scratch, indexKeyRaw)
	if err != nil {
		panic(fmt.Errorf("%s: invalid index key tuple %x: %w", idx.FullName(), indexKeyRaw, err))
	}
	return indexKeyTup
}

func extractUniqueIndexKey(indexKeyTup tuple) ([]byte, tuple) {
	n := len(indexKeyTup)
	return indexKeyTup[n-1], indexKeyTup[:n-1]
}

func decodeIndexRow(idx *IndexDef, indexKeyRaw, indexValRaw []byte) (indexKey tuple, keyRaw []byte) {
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
