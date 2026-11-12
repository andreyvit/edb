package edb

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type boundMember struct {
	ID      uint64            `edb:"-" msgpack:"-"`
	Name    string            `edb:"name"`
	Counts  map[uint64]uint64 `edb:"counts"`
	Balance int64             `edb:"balance"`
}

func TestNormalKVORollout(t *testing.T) {
	for _, format := range []StorageFormat{Fast2, Compact2} {
		layout := row2.Compact2
		if format == Fast2 {
			layout = row2.Fast2
		}
		s := kvo.NewSchema()
		e := kvo.NewEntityType(s, "member", reflect.TypeFor[boundMember]())
		kvo.NewProp(s, 1, "name", kvo.TMachineString, nil)
		kvo.NewProp(s, 2, "counts", kvo.MapOf(kvo.TUint64, kvo.TUint64), nil)
		kvo.NewProp(s, 3, "balance", kvo.TInt64, nil)
		m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) {
			b.Prop(s.MustPropByCode(1))
			b.Prop(s.MustPropByCode(2), kvo.KeepDictionaryZeroValues)
			b.Prop(s.MustPropByCode(3))
		})
		path := filepath.Join(t.TempDir(), "db")
		open := func(storage RecordStorage) (*DB, *TableDef, *IndexDef) {
			s := new(Schema)
			var idx *IndexDef
			table := DefineTable(s, "member", m, func(b *TableBuilder[uint64]) {
				b.SetRecordStorage(storage)
				idx = b.AddIndex[string]("name", nil, IndexOptDeprecatedUnique).IndexDef()
				b.SetIndexer(func(key uint64, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, string(r.GetValue(1).AsBytes())) })
			}).TableDef()
			db, err := Open(path, s, Options{IsTesting: true})
			if err != nil {
				t.Fatal(err)
			}
			return db, table, idx
		}
		db, legacy, _ := open(RecordStorage{Format: format, MessagePackInserts: true})
		db.Write(func(tx *Tx) {
			if _, err := tx.TryPutRecordChecked(legacy, uint64(7), kvo.Encoded(row2.Empty(layout)), ValueMeta{}); err != nil {
				t.Fatal(err)
			}
			if tx.GetRecord(legacy, uint64(7)).MessagePack() == nil {
				t.Fatal("insert ignored MessagePack storage policy")
			}
			tx.DeleteByKey(legacy, uint64(7))
		})
		a := &boundMember{1, "original", map[uint64]uint64{1: 0, 2: 2, 3: 3}, -400}
		db.Write(func(tx *Tx) { Put(tx, a) })
		db.Close()
		db, tbl, idx := open(RecordStorage{Format: format})
		db.Write(func(tx *Tx) {
			if r, _, err := tx.TryGetRecord(tbl, a.ID); err != nil || r.MessagePack() == nil {
				t.Fatal(err)
			}
			Put(tx, &boundMember{ID: 2, Name: "second"})
			c := FullTableScan[boundMember](tx)
			if !c.Next() {
				t.Fatal("missing old row")
			}
			old := bytes.Clone(c.ValueMemento())
			first, _, err := c.TryRecord()
			if err != nil || string(first.GetValue(1).AsBytes()) != a.Name {
				t.Fatal("migration reader on V1", err)
			}
			if !c.Next() {
				t.Fatal("missing new row")
			}
			if v, _, err := c.TryRecord(); err != nil || v.Snapshot().Layout() != layout {
				t.Fatal(err)
			}
			second, _, err := c.TryRecord()
			if err != nil || string(second.GetValue(1).AsBytes()) != "second" {
				t.Fatal("migration reader on V2", err)
			}
			pre := GetMeta[boundMember](tx, a.ID)
			changes := 0
			tx.OnChange(map[*TableDef]ChangeFlags{tbl: ChangeFlagNotify}, func(*Tx, *Change) { changes++ })
			ok, err := tx.TryUpgradeRecord(tbl, a.ID)
			if !ok || err != nil || changes != 0 {
				t.Fatal(ok, err, changes)
			}
			ok, err = tx.TryUpgradeRecord(tbl, a.ID)
			if ok || err != nil {
				t.Fatal(ok, err)
			}
			if GetMeta[boundMember](tx, a.ID) != pre || !reflect.DeepEqual(Get[boundMember](tx, a.ID), a) {
				t.Fatal("conversion changed data or revision")
			}
			row, _, err := tx.DecodeMementoVal(tbl, tbl.EncodeKey(a.ID), old)
			if err != nil || !reflect.DeepEqual(row.Interface(), a) {
				t.Fatal("legacy archive", err)
			}
			_, err = tx.TryEditRecord(tbl, a.ID, func(e *kvo.Editor) error {
				return e.TrySetValue(1, values.Bytes([]byte("new")))
			})
			if err != nil || changes != 1 {
				t.Fatal(err, changes)
			}
			if Lookup[boundMember](tx, idx, "original") != nil || Lookup[boundMember](tx, idx, "new").ID != a.ID {
				t.Fatal("stale index")
			}
			if _, err := tx.TryPutRecordChecked(tbl, a.ID, kvo.Encoded(row2.Empty(layout)), pre); err == nil {
				t.Fatal("accepted stale revision")
			}
			if _, err := tx.TryEditRecord(tbl, a.ID, func(e *kvo.Editor) error { return e.TrySetValue(99, values.Uint(1)) }); err == nil {
				t.Fatal("accepted unknown field")
			}
			stop := errors.New("discard")
			if _, err := tx.TryEditRecord(tbl, a.ID, func(e *kvo.Editor) error {
				e.TrySetValue(1, values.Bytes([]byte("discarded")))
				return stop
			}); !errors.Is(err, stop) {
				t.Fatal(err)
			}
			if changes != 1 {
				t.Fatal("failed edit notified")
			}
			c = FullTableScan[boundMember](tx)
			c.Next()
			r, meta, err := tx.DecodeMementoVal(tbl, c.RawKey(), c.ValueMemento())
			if err != nil || r.Interface().(*boundMember).Name != "new" || meta.ModCount != pre.ModCount+1 {
				t.Fatal("new memento", err, meta)
			}
		})
		db.Close()
		db, tbl, idx = open(RecordStorage{Format: format})
		db.Read(func(tx *Tx) {
			c := ExactIndexScan[boundMember](tx, idx, "new")
			if !c.Next() {
				t.Fatal("index after reopen")
			}
			v, _, err := c.TryRecord()
			if err != nil || v.GetValue(3).AsInt() != -400 || v.Child(row2.Step{Key: 2}).Snapshot().Len() != 3 {
				t.Fatal(err)
			}
		})
		db.Close()
	}
}

func TestLossyUpgradeLeavesLegacyRowIntact(t *testing.T) {
	type row struct {
		ID         uint64 `msgpack:"-" edb:"-"`
		Score      int64  `edb:"score"`
		Historical string `edb:"-"`
	}
	s := kvo.NewSchema()
	entity := kvo.NewEntityType(s, "row", reflect.TypeFor[row]())
	kvo.NewProp(s, 1, "score", kvo.TInt64, nil)
	model := kvo.NewModel(s, entity, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	path := filepath.Join(t.TempDir(), "legacy.db")
	for _, upgrade := range []bool{false, true} {
		schema := new(Schema)
		table := DefineTable(schema, "rows", model, func(b *TableBuilder[uint64]) {
			if upgrade {
				b.SetRecordStorage(RecordStorage{Format: Fast2})
			}
		}).TableDef()
		db, err := Open(path, schema, Options{IsTesting: true})
		if err != nil {
			t.Fatal(err)
		}
		db.Write(func(tx *Tx) {
			want := row{1, 19, "archived field"}
			if !upgrade {
				Put(tx, &want)
				return
			}
			if ok, err := tx.TryUpgradeRecord(table, want.ID); err == nil || ok {
				t.Fatal("lossy upgrade accepted")
			}
			if r, meta, err := tx.TryGetRecord(table, want.ID); err != nil || r.MessagePack() == nil || meta.ModCount != 1 {
				t.Fatal(meta, err)
			}
			if got := Get[row](tx, want.ID); *got != want {
				t.Fatal(got)
			}
		})
		db.Close()
	}
}

func TestRecordStorageRequiresCurrentFormat(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast1, 255} {
		t.Run(format.String(), func(t *testing.T) {
			defer func() {
				if r := recover(); r != "RecordStorage requires Fast2 or Compact2; omit SetRecordStorage for MessagePack rows" {
					t.Fatalf("unexpected configuration error: %v", r)
				}
			}()
			DefineTable(new(Schema), "member", testTableModel[boundMember](), func(b *TableBuilder[uint64]) {
				b.SetRecordStorage(RecordStorage{Format: format})
			}).TableDef()
		})
	}
}
