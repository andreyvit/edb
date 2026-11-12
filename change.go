package edb

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo"
)

type (
	Change struct {
		table       *TableDef
		op          Op
		rawKey      []byte
		keyVal      reflect.Value
		newRecord   kvo.Record
		oldRecord   kvo.Record
		newGoStruct reflect.Value
		oldGoStruct reflect.Value
	}

	ChangeFlags uint64

	Op int
)

const (
	OpNone   Op = 0
	OpPut    Op = 1
	OpDelete Op = 2
)

const (
	ChangeFlagNotify ChangeFlags = 1 << iota
	ChangeFlagIncludeKey
	ChangeFlagIncludeNewRecord
	ChangeFlagIncludeOldRecord
	ChangeFlagIncludeNewGoStruct
	ChangeFlagIncludeOldGoStruct
)

func (chg *Change) Table() *TableDef {
	return chg.table
}
func (chg *Change) Op() Op {
	return chg.op
}
func (chg *Change) RawKey() []byte {
	return chg.rawKey
}
func (chg *Change) HasKey() bool {
	return chg.keyVal.IsValid()
}
func (chg *Change) KeyVal() reflect.Value {
	return chg.keyVal
}
func (chg *Change) Key() any {
	return chg.keyVal.Interface()
}

// NewRecord returns the requested post-write record; it is missing for deletes.
// Records borrow transaction data. Call Own to retain one beyond the transaction.
func (chg *Change) NewRecord() kvo.Record { return chg.newRecord }

// OldRecord returns the requested pre-write record; it is missing for inserts
// or when decoding the old value fails. Like NewRecord, it borrows transaction data.
func (chg *Change) OldRecord() kvo.Record { return chg.oldRecord }
func (chg *Change) HasNewRecord() bool    { return !chg.newRecord.Missing() }
func (chg *Change) HasOldRecord() bool    { return !chg.oldRecord.Missing() }

// NewGoStruct returns an independently decoded *T, or nil when absent or not
// requested. It contains all persisted fields, including fields outside a KVO
// projection, and the database primary key. Mutating it does not save changes.
func (chg *Change) NewGoStruct() any { return valToAny(chg.newGoStruct) }

// OldGoStruct returns an independently decoded pre-write *T, or nil when absent,
// not requested, or unreadable. Old-value decode errors are logged when possible
// and do not prevent repairing or deleting a damaged value.
func (chg *Change) OldGoStruct() any              { return valToAny(chg.oldGoStruct) }
func (chg *Change) HasNewGoStruct() bool          { return chg.newGoStruct.IsValid() }
func (chg *Change) HasOldGoStruct() bool          { return chg.oldGoStruct.IsValid() }
func (chg *Change) NewGoStructVal() reflect.Value { return chg.newGoStruct }
func (chg *Change) OldGoStructVal() reflect.Value { return chg.oldGoStruct }

// notifyChange runs after the data and index mutation, before commit. Records
// share the ordinary read path; Go structs are decoded only when requested.
func (tx *Tx) notifyChange(tbl *TableDef, op Op, keyRaw []byte, old, next *value) {
	opts := tx.changeOptions[tbl]
	if tx.changeHandler == nil || !opts.Contains(ChangeFlagNotify) {
		return
	}
	chg := Change{table: tbl, op: op, rawKey: bytes.Clone(keyRaw)}
	if opts.Contains(ChangeFlagIncludeKey) {
		chg.keyVal = tbl.DecodeKeyVal(chg.rawKey)
	}
	if next != nil {
		if opts.Contains(ChangeFlagIncludeNewRecord) {
			r, _, _, err := tx.recordFromValue(tbl, chg.rawKey, next)
			if err != nil {
				panic(err)
			}
			chg.newRecord = r
		}
		if opts.Contains(ChangeFlagIncludeNewGoStruct) {
			v, _, err := decodeTableRowFromValue(next, tbl, chg.rawKey, tx)
			if err != nil {
				panic(err)
			}
			chg.newGoStruct = v
		}
	}
	if old != nil {
		if opts.Contains(ChangeFlagIncludeOldRecord) {
			r, _, _, err := tx.recordFromValue(tbl, chg.rawKey, old)
			if err == nil {
				chg.oldRecord = r
			} else {
				tx.logChangeDecodeError(tbl, op, keyRaw, err)
			}
		}
		if opts.Contains(ChangeFlagIncludeOldGoStruct) {
			v, _, err := decodeTableRowFromValue(old, tbl, chg.rawKey, tx)
			if err == nil {
				chg.oldGoStruct = v
			} else {
				tx.logChangeDecodeError(tbl, op, keyRaw, err)
			}
		}
	}
	tx.changeHandler(tx, &chg)
}

func (tx *Tx) logChangeDecodeError(tbl *TableDef, op Op, keyRaw []byte, err error) {
	if tx.db.logf != nil {
		tx.db.logf("db: %s %s/%x: cannot decode old change value: %v", op, tbl.name, keyRaw, err)
	}
}

func (v ChangeFlags) Contains(f ChangeFlags) bool {
	return (v & f) == f
}
func (v ChangeFlags) ContainsAny(f ChangeFlags) bool {
	return (v & f) != 0
}

func (v Op) String() string {
	switch v {
	case OpNone:
		return "none"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("invalid op %d", int(v))
	}
}
