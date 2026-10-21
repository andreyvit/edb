package edb

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"
)

type reuseAliasRow struct {
	Key     []byte `msgpack:"-"`
	Name    string
	Payload []byte
}

type reuseAliasFixture struct {
	db        *DB
	table     *Table
	index     *Index
	singleton *SKey
}

type reuseAliasSnapshot struct {
	rows    []*reuseAliasRow
	keys    [][]byte
	rawKey  []byte
	memento []byte
	meta    ValueMeta
}

func TestReuseAliasCommittedData(t *testing.T) {
	f := reuseAliasOpen(t)
	want := reuseAliasValue("committed", 0x37)
	singleton := bytes.Repeat([]byte{0x00, 0x82, 0xff}, 513)
	tx := f.db.BeginUpdate()
	defer tx.Close()
	Put(tx, want)
	SPut(tx, f.singleton, &singleton)
	written := reuseAliasRead(t, tx, f.index, want)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	reuseAliasClosePoison(t, tx)
	reuseAliasCheck(t, written, want)

	reader := f.db.BeginRead()
	defer reader.Close()
	stored := reuseAliasRead(t, reader, f.index, want)
	var gotSingleton []byte
	if !SGet(reader, f.singleton, &gotSingleton) {
		t.Fatal("missing singleton")
	}
	if count := Count(FullTableScan[reuseAliasRow](reader).Raw()); count != 1 {
		t.Fatalf("row count = %d, want 1", count)
	}
	reuseAliasClosePoison(t, reader)
	reuseAliasCheck(t, stored, want)
	if !reflect.DeepEqual(gotSingleton, singleton) {
		t.Fatalf("singleton = %x, want %x", gotSingleton, singleton)
	}
	reuseAliasCheckMemento(t, f, stored, want)
}

func TestReuseAliasChangeLifetime(t *testing.T) {
	f := reuseAliasOpen(t)
	old := reuseAliasValue("before", 0x19)
	want := reuseAliasValue("after", 0x63)
	tx := f.db.BeginUpdate()
	defer tx.Close()
	var changes []*Change
	var keyScratch [][]byte
	indexer := f.table.indexer
	f.table.indexer = func(row any, ib *IndexBuilder) {
		keyScratch = append(keyScratch, ib.key[:cap(ib.key)])
		indexer(row, ib)
	}
	tx.OnChange(map[*Table]ChangeFlags{
		f.table: ChangeFlagNotify | ChangeFlagIncludeKey | ChangeFlagIncludeMutableRow | ChangeFlagIncludeOldRow,
	}, func(_ *Tx, change *Change) {
		changes = append(changes, change)
	})
	Put(tx, old)
	input := reuseAliasValue("after", 0x63)
	Put(tx, input)
	escaped := reuseAliasRead(t, tx, f.index, want)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	reuseAliasClosePoison(t, tx, keyScratch...)
	clear(input.Key)
	clear(input.Payload)
	input.Name = "changed input"

	if len(changes) != 2 {
		t.Fatalf("change count = %d, want 2", len(changes))
	}
	for i, change := range changes {
		if change.Table() != f.table || change.Op() != OpPut || !change.HasKey() || !change.HasRow() || change.HasOldRow() != (i == 1) {
			t.Fatalf("change %d has incorrect shape: %+v", i, change)
		}
		if !bytes.Equal(change.RawKey(), f.table.EncodeKey(want.Key)) {
			t.Fatalf("change %d raw key = %x, want %x", i, change.RawKey(), f.table.EncodeKey(want.Key))
		}
		if !reflect.DeepEqual(change.Key(), want.Key) {
			t.Fatalf("change %d key = %v, want %x", i, change.Key(), want.Key)
		}
	}
	if !reflect.DeepEqual(changes[0].Row(), old) || !reflect.DeepEqual(changes[1].Row(), want) || !reflect.DeepEqual(changes[1].OldRow(), old) {
		t.Fatalf("change rows = %#v / %#v / %#v, want %#v / %#v / %#v", changes[0].Row(), changes[1].Row(), changes[1].OldRow(), old, want, old)
	}
	reuseAliasCheck(t, escaped, want)
	reuseAliasCheckMemento(t, f, escaped, want)
	reader := f.db.BeginRead()
	defer reader.Close()
	if Lookup[reuseAliasRow](reader, f.index, old.Name) != nil {
		t.Fatal("old unique index entry remains")
	}
	reuseAliasCheck(t, reuseAliasRead(t, reader, f.index, want), want)
}

func TestReuseAliasDeleteChangeLifetime(t *testing.T) {
	f := reuseAliasOpen(t)
	want := reuseAliasValue("deleted", 0x42)
	tx := f.db.BeginUpdate()
	defer tx.Close()
	Put(tx, want)
	escaped := reuseAliasRead(t, tx, f.index, want)
	var changes []*Change
	tx.OnChange(map[*Table]ChangeFlags{
		f.table: ChangeFlagNotify | ChangeFlagIncludeKey | ChangeFlagIncludeRow,
	}, func(_ *Tx, change *Change) {
		changes = append(changes, change)
	})
	keyRaw := f.table.EncodeKey(want.Key)
	if !tx.DeleteByKeyRaw(f.table, keyRaw) {
		t.Fatal("delete returned false")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	reuseAliasClosePoison(t, tx, keyRaw)

	if len(changes) != 1 {
		t.Fatalf("change count = %d, want 1", len(changes))
	}
	change := changes[0]
	if change.Table() != f.table || change.Op() != OpDelete || !change.HasKey() || !change.HasRow() || change.HasOldRow() {
		t.Fatalf("delete change has incorrect shape: %+v", change)
	}
	if expected := f.table.EncodeKey(want.Key); !bytes.Equal(change.RawKey(), expected) {
		t.Fatalf("delete raw key = %x, want %x", change.RawKey(), expected)
	}
	if !reflect.DeepEqual(change.Key(), want.Key) {
		t.Fatalf("delete key = %v, want %x", change.Key(), want.Key)
	}
	if !reflect.DeepEqual(change.Row(), want) {
		t.Fatalf("delete row = %#v, want %#v", change.Row(), want)
	}
	reuseAliasCheck(t, escaped, want)
	f.db.Read(func(tx *Tx) {
		if Get[reuseAliasRow](tx, want.Key) != nil || Lookup[reuseAliasRow](tx, f.index, want.Name) != nil {
			t.Fatal("deleted row or unique index entry remains")
		}
	})
}

func reuseAliasOpen(t *testing.T) reuseAliasFixture {
	t.Helper()
	schema := &Schema{}
	index := AddIndex[string]("name").Unique()
	table := AddTable(schema, "reuse_alias", 1, func(row *reuseAliasRow, ib *IndexBuilder) {
		ib.Add(index, row.Name)
	}, nil, []*Index{index})
	singleton := AddSingletonKey[[]byte](AddKVMap(schema, "metadata"), "payload")
	path := filepath.Join(t.TempDir(), "alias.db")
	if testing.Short() {
		path = InMemory
	}
	db, err := Open(path, schema, Options{IsTesting: true, ReuseTxBuffers: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return reuseAliasFixture{db: db, table: table, index: index, singleton: singleton}
}

func reuseAliasValue(name string, fill byte) *reuseAliasRow {
	return &reuseAliasRow{Key: []byte{0x11, 0x00, 0xff, 0x7f}, Name: name, Payload: bytes.Repeat([]byte{fill, 0x00, 0xff}, 513)}
}

func reuseAliasRead(t *testing.T, tx *Tx, index *Index, want *reuseAliasRow) reuseAliasSnapshot {
	t.Helper()
	result := reuseAliasSnapshot{rows: []*reuseAliasRow{
		Get[reuseAliasRow](tx, want.Key), Lookup[reuseAliasRow](tx, index, want.Name),
	}}
	key, found := LookupKey[[]byte](tx, index, want.Name)
	if !found {
		t.Fatal("unique index key not found")
	}
	result.keys = append(result.keys, key)
	for i, cursor := range []Cursor[reuseAliasRow]{
		ExactTableScan[reuseAliasRow](tx, want.Key), ExactIndexScan[reuseAliasRow](tx, index, want.Name),
	} {
		if !cursor.Next() {
			t.Fatalf("cursor %d returned no rows", i)
		}
		result.rows = append(result.rows, cursor.Row())
		result.keys = append(result.keys, cursor.Key().([]byte))
		if i == 0 {
			result.rawKey = bytes.Clone(cursor.RawKey())
			result.memento = bytes.Clone(cursor.ValueMemento())
			result.meta = cursor.Meta()
		}
		if cursor.Next() {
			t.Fatalf("cursor %d returned extra rows", i)
		}
	}
	return result
}

func reuseAliasClosePoison(t *testing.T, tx *Tx, extra ...[]byte) {
	t.Helper()
	arrays := append([][]byte(nil), extra...)
	for i, buffers := range [][]*byteBuffer{tx.valueBufs, tx.indexValueBufs, tx.indexKeyBufs} {
		if (tx.IsWritable() || i == 2) && len(buffers) == 0 {
			t.Fatalf("buffer group %d is empty", i)
		}
		for _, buffer := range buffers {
			if buffer == nil || cap(buffer.data) == 0 {
				t.Fatalf("buffer group %d contains an empty buffer", i)
			}
			arrays = append(arrays, buffer.data[:cap(buffer.data)])
		}
	}
	tx.Close()
	if tx.valueBufs != nil || tx.indexValueBufs != nil || tx.indexKeyBufs != nil || tx.valueBufsOwner != nil || tx.indexValueBufsOwner != nil || tx.indexKeyBufsOwner != nil {
		t.Fatal("Close retained buffer tracking or owners")
	}
	for _, array := range arrays {
		for i := range array {
			array[i] = 0xa5
		}
	}
}

func reuseAliasCheck(t *testing.T, snapshot reuseAliasSnapshot, want *reuseAliasRow) {
	t.Helper()
	if len(snapshot.rows) != 4 || len(snapshot.keys) != 3 {
		t.Fatalf("snapshot shape = %d rows, %d keys", len(snapshot.rows), len(snapshot.keys))
	}
	for i, row := range snapshot.rows {
		if !reflect.DeepEqual(row, want) {
			t.Fatalf("row %d = %#v, want %#v", i, row, want)
		}
	}
	for i, key := range snapshot.keys {
		if !reflect.DeepEqual(key, want.Key) {
			t.Fatalf("key %d = %x, want %x", i, key, want.Key)
		}
	}
}

func reuseAliasCheckMemento(t *testing.T, f reuseAliasFixture, snapshot reuseAliasSnapshot, want *reuseAliasRow) {
	t.Helper()
	if key := f.table.DecodeKeyVal(snapshot.rawKey).Interface(); !reflect.DeepEqual(key, want.Key) {
		t.Fatalf("copied raw key decodes to %v, want %x", key, want.Key)
	}
	tx := f.db.BeginRead()
	defer tx.Close()
	row, meta, err := tx.DecodeMementoVal(f.table, snapshot.rawKey, snapshot.memento)
	if err != nil || !row.IsValid() || meta != snapshot.meta || meta.SchemaVer != 1 || meta.IsMissing() {
		t.Fatalf("copied memento = (%v, %+v, %v), want meta %+v", row, meta, err, snapshot.meta)
	}
	if !reflect.DeepEqual(row.Interface(), want) {
		t.Fatalf("copied memento row = %#v, want %#v", row.Interface(), want)
	}
}
