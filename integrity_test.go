package edb

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func AssertIntegrity(t testing.TB, tx *Tx) {
	t.Helper()
	problems := checkIntegrity(tx)
	if len(problems) > 0 {
		t.Fatalf("integrity check failed:\n%s", strings.Join(problems, "\n"))
	}
}

func TestAssertIntegrityAcceptsCurrentState(t *testing.T) {
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx,
			&User{ID: 1, Name: "foo", Email: "foo@example.com"},
			&User{ID: 2, Name: "bar", Email: "bar@example.com"},
			&Widget{Key: AB{A: 1, B: 2}, Name: "w", Email: "w@example.com"},
		)
		tx.KVPutRaw(wumpets, x("10 12"), buildKV(0x42, 0x8877).Bytes())
	})

	db.Read(func(tx *Tx) {
		AssertIntegrity(t, tx)
	})
}

func TestAssertIntegrityDetectsTableIndexDamage(t *testing.T) {
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
		_ = tx.UnsafeDeleteByKeyRawSkippingIndex(usersTable, usersTable.EncodeKey(ID(1)))
	})

	db.Read(func(tx *Tx) {
		assertIntegrityProblem(t, tx, "dangling index entry")
	})
}

func TestAssertIntegrityDetectsKVIndexDamage(t *testing.T) {
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		tx.KVPutRaw(wumpets, x("10 12"), buildKV(0x42, 0x8877).Bytes())
		ensure(tx.stx.Bucket(wumpetsByB.idxBuck, "").Delete(x("88 77 10 12")))
	})

	db.Read(func(tx *Tx) {
		assertIntegrityProblem(t, tx, "missing KV index entry")
	})
}

func assertIntegrityProblem(t testing.TB, tx *Tx, contains string) {
	t.Helper()
	problems := checkIntegrity(tx)
	if len(problems) == 0 {
		t.Fatalf("integrity check passed, wanted problem containing %q", contains)
	}
	for _, p := range problems {
		if strings.Contains(p, contains) {
			return
		}
	}
	t.Fatalf("integrity problems %q do not contain %q", problems, contains)
}

type integrityChecker struct {
	tx       *Tx
	problems []string
}

func checkIntegrity(tx *Tx) []string {
	c := integrityChecker{tx: tx}
	for _, tbl := range tx.db.schema.tables {
		c.checkTable(tbl)
	}
	for _, tbl := range tx.db.schema.kvtables {
		c.checkKVTable(tbl)
	}
	return c.problems
}

func (c *integrityChecker) problem(format string, args ...any) {
	c.problems = append(c.problems, fmt.Sprintf(format, args...))
}

func (c *integrityChecker) checkTable(tbl *Table) {
	dataBuck := c.tx.stx.Bucket(tbl.name, dataBucketName)
	if dataBuck == nil {
		c.problem("%s: missing data bucket", tbl.name)
		return
	}

	expected := make(map[*Index]map[string][]byte, len(tbl.indices))
	for _, idx := range tbl.indices {
		expected[idx] = make(map[string][]byte)
	}

	cursor := dataBuck.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		c.checkTableRow(tbl, k, v, expected)
	}

	for _, idx := range tbl.indices {
		c.checkTableIndex(tbl, idx, dataBuck, expected[idx])
	}
}

func (c *integrityChecker) checkTableRow(tbl *Table, keyRaw, valueRaw []byte, expected map[*Index]map[string][]byte) {
	var vle value
	if err := vle.decode(valueRaw, false); err != nil {
		c.problem("%s/%x: invalid row value: %v", tbl.name, keyRaw, err)
		return
	}

	rowVal, _, _, err := decodeTableRowFromValue(&vle, tbl, keyRaw, c.tx)
	if err != nil {
		c.problem("%s/%x: cannot decode row: %v", tbl.name, keyRaw, err)
		return
	}

	if err := c.checkStoredIndexList(tbl, keyRaw, vle.Index); err != nil {
		c.problem("%s/%x: invalid stored index list: %v", tbl.name, keyRaw, err)
	}

	ts := c.tx.db.tableState(tbl)
	ib := makeIndexBuilder(ts, keyRaw)
	tbl.indexer(rowVal.Interface(), &ib)
	ib.finalize()
	defer ib.release(c.tx)

	wantedIndexData := appendIndexKeys(nil, ib.rows)
	if !bytes.Equal(vle.Index, wantedIndexData) {
		c.problem("%s/%x: stored index list mismatch: got %x, wanted %x", tbl.name, keyRaw, vle.Index, wantedIndexData)
	}

	for _, row := range ib.rows {
		c.checkExpectedTableIndexRow(tbl, keyRaw, row, expected[row.Index])
	}
}

func (c *integrityChecker) checkStoredIndexList(tbl *Table, keyRaw, raw []byte) error {
	ts := c.tx.db.tableState(tbl)
	rows, err := decodeStoredIndexRows(raw)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if ts.indexByOrdinal(row.ord) == nil {
			return fmt.Errorf("unknown index ordinal %d for key %x", row.ord, keyRaw)
		}
	}
	return nil
}

func (c *integrityChecker) checkExpectedTableIndexRow(tbl *Table, tableKeyRaw []byte, row IndexRow, expected map[string][]byte) {
	idxBuck := c.tx.stx.Bucket(tbl.name, row.Index.buck)
	if idxBuck == nil {
		c.problem("%s: missing index bucket", row.Index.FullName())
		return
	}
	indexKey := string(row.KeyRaw)
	if prev, ok := expected[indexKey]; ok && !bytes.Equal(prev, row.ValueRaw) {
		c.problem("%s/%x: duplicate expected index key with values %x and %x", row.Index.FullName(), row.KeyRaw, prev, row.ValueRaw)
	}
	expected[indexKey] = slices.Clone(row.ValueRaw)

	actual := idxBuck.Get(row.KeyRaw)
	if actual == nil {
		c.problem("%s/%x: missing index entry for table key %x", row.Index.FullName(), row.KeyRaw, tableKeyRaw)
	} else if !bytes.Equal(actual, row.ValueRaw) {
		c.problem("%s/%x: index value mismatch: got %x, wanted %x", row.Index.FullName(), row.KeyRaw, actual, row.ValueRaw)
	}
}

func (c *integrityChecker) checkTableIndex(tbl *Table, idx *Index, dataBuck storageBucket, expected map[string][]byte) {
	idxBuck := c.tx.stx.Bucket(tbl.name, idx.buck)
	if idxBuck == nil {
		c.problem("%s: missing index bucket", idx.FullName())
		return
	}

	cursor := idxBuck.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		tableKeyRaw, err := decodeIndexTableKeyForIntegrity(idx, k, v)
		if err != nil {
			c.problem("%s/%x: invalid index entry: %v", idx.FullName(), k, err)
			continue
		}
		if dataBuck.Get(tableKeyRaw) == nil {
			c.problem("%s/%x: dangling index entry points to missing table key %x", idx.FullName(), k, tableKeyRaw)
		}

		expectedValue, ok := expected[string(k)]
		if !ok {
			c.problem("%s/%x: unexpected index entry", idx.FullName(), k)
		} else if !bytes.Equal(v, expectedValue) {
			c.problem("%s/%x: unexpected index value: got %x, wanted %x", idx.FullName(), k, v, expectedValue)
		}
	}
}

func (c *integrityChecker) checkKVTable(tbl *KVTable) {
	dataBuck := c.tx.stx.Bucket(tbl.name, "")
	if dataBuck == nil {
		c.problem("%s: missing KV data bucket", tbl.name)
		return
	}

	expected := make(map[*KVIndex]map[string]struct{}, len(tbl.indices))
	for _, idx := range tbl.indices {
		expected[idx] = make(map[string]struct{})
	}

	cursor := dataBuck.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		for _, idx := range tbl.indices {
			c.checkExpectedKVIndexRows(idx, k, v, expected[idx])
		}
	}

	for _, idx := range tbl.indices {
		c.checkKVIndex(idx, dataBuck, expected[idx])
	}
}

func (c *integrityChecker) checkExpectedKVIndexRows(idx *KVIndex, keyRaw, valueRaw []byte, expected map[string]struct{}) {
	idxBuck := c.tx.stx.Bucket(idx.idxBuck, "")
	if idxBuck == nil {
		c.problem("%s: missing KV index bucket", idx.FullName())
		return
	}

	entries, err := kvIndexEntriesForIntegrity(idx, keyRaw, valueRaw)
	if err != nil {
		c.problem("%s/%x: cannot compute KV index entries: %v", idx.table.name, keyRaw, err)
		return
	}
	for _, entry := range entries {
		if _, ok := expected[string(entry)]; ok {
			c.problem("%s/%x: duplicate expected KV index key", idx.FullName(), entry)
		}
		expected[string(entry)] = struct{}{}

		if actual := idxBuck.Get(entry); actual == nil {
			c.problem("%s/%x: missing KV index entry for table key %x", idx.FullName(), entry, keyRaw)
		} else if len(actual) != 0 {
			c.problem("%s/%x: KV index value is %x, wanted empty", idx.FullName(), entry, actual)
		}
	}
}

func (c *integrityChecker) checkKVIndex(idx *KVIndex, dataBuck storageBucket, expected map[string]struct{}) {
	idxBuck := c.tx.stx.Bucket(idx.idxBuck, "")
	if idxBuck == nil {
		c.problem("%s: missing KV index bucket", idx.FullName())
		return
	}

	cursor := idxBuck.Cursor()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		tableKeyRaw, err := kvIndexPrimaryKeyForIntegrity(idx, k)
		if err != nil {
			c.problem("%s/%x: cannot decode KV primary key: %v", idx.FullName(), k, err)
			continue
		}
		if tableKeyRaw == nil || dataBuck.Get(tableKeyRaw) == nil {
			c.problem("%s/%x: dangling KV index entry points to missing table key %x", idx.FullName(), k, tableKeyRaw)
		}
		if _, ok := expected[string(k)]; !ok {
			c.problem("%s/%x: unexpected KV index entry", idx.FullName(), k)
		}
		if len(v) != 0 {
			c.problem("%s/%x: KV index value is %x, wanted empty", idx.FullName(), k, v)
		}
	}
}

type storedIndexRow struct {
	ord uint64
}

func decodeStoredIndexRows(raw []byte) (rows []storedIndexRow, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
	}()
	decodeIndexKeys(raw, func(ord uint64, _ []byte) {
		rows = append(rows, storedIndexRow{ord: ord})
	})
	return rows, nil
}

func decodeIndexTableKeyForIntegrity(idx *Index, indexKeyRaw, indexValRaw []byte) (keyRaw []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
	}()
	_, keyRaw = decodeIndexRow(idx, indexKeyRaw, indexValRaw)
	return keyRaw, nil
}

func kvIndexEntriesForIntegrity(idx *KVIndex, keyRaw, valueRaw []byte) (entries [][]byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
	}()
	for _, entry := range idx.entries(keyRaw, valueRaw) {
		entries = append(entries, slices.Clone(entry))
	}
	return entries, nil
}

func kvIndexPrimaryKeyForIntegrity(idx *KVIndex, indexKeyRaw []byte) (keyRaw []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%v", p)
		}
	}()
	return idx.indexKeyToPrimaryKey(indexKeyRaw), nil
}
