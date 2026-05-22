package edb

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

type corruptValueRow struct {
	ID    ID     `msgpack:"-"`
	Code  string `msgpack:"c"`
	Count int    `msgpack:"n"`
}

type corruptValueWrongPayload struct {
	ID    ID     `msgpack:"-"`
	Code  string `msgpack:"c"`
	Count string `msgpack:"n"`
}

var (
	corruptValueSchema  = &Schema{}
	corruptValuesByCode = AddIndex[string]("code")
	corruptValuesTable  = AddTable(corruptValueSchema, "corrupt_values", 1, func(row *corruptValueRow, ib *IndexBuilder) {
		ib.Add(corruptValuesByCode, row.Code)
	}, nil, []*Index{corruptValuesByCode})
)

func TestCorruptValue_DecodeRowPayloadReturnsErrors(t *testing.T) {
	db := setupCorruptValueDB(t)
	db.Write(func(tx *Tx) {
		keyRaw := corruptValuesTable.EncodeKey(ID(1))
		indexRaw := existingValueIndexBytes(t, tx, corruptValuesTable, keyRaw)
		rawValue := encodedCorruptValue(&corruptValueWrongPayload{
			ID:    1,
			Code:  "one",
			Count: "not an int",
		}, indexRaw)
		putRawTableValue(t, tx, corruptValuesTable, keyRaw, rawValue)
	})

	db.Read(func(tx *Tx) {
		gotMeta := tx.GetMeta(corruptValuesTable, ID(1))
		if !gotMeta.Exists() {
			t.Fatalf("GetMeta should still decode metadata for a value with bad row payload")
		}

		row, _, err := tx.TryGet(corruptValuesTable, ID(1))
		if row != nil {
			t.Fatalf("TryGet returned row %v for bad row payload", row)
		}
		assertTableDataError(t, err, "failed to decode msgpack")

		c := TableScan[corruptValueRow](tx, ExactScan(ID(1))).Raw()
		if !c.Next() {
			t.Fatalf("table cursor did not find corrupted row")
		}
		_, _, err = c.TryRowVal()
		assertTableDataError(t, err, "failed to decode msgpack")
		_, _, err = c.TryRow()
		assertTableDataError(t, err, "failed to decode msgpack")

		ic := IndexScan[corruptValueRow](tx, corruptValuesByCode, ExactScan("one")).Raw()
		if !ic.Next() {
			t.Fatalf("index cursor did not find corrupted row")
		}
		_, _, err = ic.TryRowVal()
		assertTableDataError(t, err, "failed to decode msgpack")
		_, _, err = ic.TryRow()
		assertTableDataError(t, err, "failed to decode msgpack")

		memento := encodedCorruptValue(&corruptValueWrongPayload{
			ID:    1,
			Code:  "one",
			Count: "not an int",
		}, nil)
		_, _, err = tx.DecodeMementoVal(corruptValuesTable, corruptValuesTable.EncodeKey(ID(1)), memento)
		assertTableDataError(t, err, "failed to decode msgpack")
	})
}

func TestCorruptValue_MalformedRecordPanicsAtDecodeBoundary(t *testing.T) {
	db := setupCorruptValueDB(t)
	db.Write(func(tx *Tx) {
		keyRaw := corruptValuesTable.EncodeKey(ID(1))
		rawValue := existingRawTableValue(t, tx, corruptValuesTable, keyRaw)
		putRawTableValue(t, tx, corruptValuesTable, keyRaw, rawValue[:len(rawValue)-1])
	})

	db.Read(func(tx *Tx) {
		assertTableDataPanic(t, func() {
			_, _, _ = tx.TryGet(corruptValuesTable, ID(1))
		}, "invalid value")

		assertTableDataPanic(t, func() {
			_ = tx.GetMeta(corruptValuesTable, ID(1))
		}, "invalid value")

		assertTableDataPanic(t, func() {
			c := TableScan[corruptValueRow](tx, ExactScan(ID(1))).Raw()
			if !c.Next() {
				t.Fatalf("table cursor did not find malformed row")
			}
			_, _, _ = c.TryRowVal()
		}, "invalid value")

		assertTableDataPanic(t, func() {
			ic := IndexScan[corruptValueRow](tx, corruptValuesByCode, ExactScan("one")).Raw()
			if !ic.Next() {
				t.Fatalf("index cursor did not find malformed row")
			}
			_, _, _ = ic.TryRowVal()
		}, "invalid value")

		assertTableDataPanic(t, func() {
			c := TableScan[corruptValueRow](tx, ExactScan(ID(1))).Raw()
			if !c.Next() {
				t.Fatalf("table cursor did not find malformed row")
			}
			_ = c.ValueMemento()
		}, "invalid value")
	})
}

func setupCorruptValueDB(t testing.TB) *DB {
	t.Helper()
	db := setup(t, corruptValueSchema)
	db.Write(func(tx *Tx) {
		Put(tx, &corruptValueRow{
			ID:    1,
			Code:  "one",
			Count: 1,
		})
	})
	return db
}

func encodedCorruptValue(row any, indexRaw []byte) []byte {
	buf := make([]byte, 0, maxValueHeaderSize+64+len(indexRaw))
	raw := reserveValueHeader(buf)
	raw = MsgPack.EncodeValue(raw, reflect.ValueOf(row))
	indexOff := len(raw)
	raw = append(raw, indexRaw...)
	return putValueHeader(raw, vfDefault, corruptValuesTable.latestSchemaVer, 1, indexOff)
}

func existingRawTableValue(t testing.TB, tx *Tx, tbl *Table, keyRaw []byte) []byte {
	t.Helper()
	raw := tbl.dataBucketIn(tx).Get(keyRaw)
	if raw == nil {
		t.Fatalf("missing raw table value for key %x", keyRaw)
	}
	return append([]byte(nil), raw...)
}

func existingValueIndexBytes(t testing.TB, tx *Tx, tbl *Table, keyRaw []byte) []byte {
	t.Helper()
	raw := existingRawTableValue(t, tx, tbl, keyRaw)
	var vle value
	if err := vle.decode(raw, false); err != nil {
		t.Fatalf("failed to decode existing value: %v", err)
	}
	return append([]byte(nil), vle.Index...)
}

func putRawTableValue(t testing.TB, tx *Tx, tbl *Table, keyRaw, valueRaw []byte) {
	t.Helper()
	if err := tbl.dataBucketIn(tx).Put(keyRaw, valueRaw); err != nil {
		t.Fatalf("failed to put raw table value: %v", err)
	}
}

func assertTableDataError(t testing.TB, err error, contains string) {
	t.Helper()
	if err == nil {
		t.Fatalf("got nil error")
	}
	var tableErr *TableError
	if !errors.As(err, &tableErr) {
		t.Fatalf("got %T %v, wanted TableError", err, err)
	}
	var dataErr *DataError
	if !errors.As(err, &dataErr) {
		t.Fatalf("got %T %v, wanted nested DataError", err, err)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Fatalf("got error %q, wanted substring %q", err.Error(), contains)
	}
}

func assertTableDataPanic(t testing.TB, fn func(), contains string) {
	t.Helper()
	defer func() {
		got := recover()
		if got == nil {
			t.Fatalf("expected panic")
		}
		err, ok := got.(error)
		if !ok {
			t.Fatalf("got panic %T %v, wanted error", got, got)
		}
		assertTableDataError(t, err, contains)
	}()
	fn()
}
