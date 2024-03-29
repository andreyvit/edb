package edb

import (
	"fmt"
	"reflect"
)

type (
	Change struct {
		table     *Table
		op        Op
		rawKey    []byte
		keyVal    reflect.Value
		rowVal    reflect.Value
		oldRowVal reflect.Value
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
	ChangeFlagIncludeRow
	ChangeFlagIncludeOldRow
)

func (chg *Change) Table() *Table {
	return chg.table
}
func (chg *Change) Op() Op {
	return chg.op
}
func (chg *Change) RawKey() []byte {
	return chg.rawKey
}
func (chg *Change) HasKey() bool {
	return !chg.keyVal.IsZero()
}
func (chg *Change) KeyVal() reflect.Value {
	return chg.keyVal
}
func (chg *Change) Key() any {
	return chg.keyVal.Interface()
}
func (chg *Change) HasRow() bool {
	return !chg.rowVal.IsZero()
}
func (chg *Change) RowVal() reflect.Value {
	return chg.rowVal
}
func (chg *Change) Row() any {
	return chg.rowVal.Interface()
}
func (chg *Change) HasOldRow() bool {
	return !chg.oldRowVal.IsZero()
}
func (chg *Change) OldRowVal() reflect.Value {
	return chg.oldRowVal
}
func (chg *Change) OldRow() any {
	return chg.oldRowVal.Interface()
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
