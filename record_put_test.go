package edb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type keylessRecordRow struct {
	ID   uint64 `edb:"-" msgpack:"-"`
	Name string `edb:"name"`
}
type storedKeyRecordRow struct {
	ID   uint64 `edb:"id" msgpack:"id"`
	Name string `edb:"name"`
}

func keyedRecordTable[T any](t testing.TB, format StorageFormat, storedKey bool) (*DB, *TableDef, *IndexDef, *kvo.Binding) {
	t.Helper()
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "row", reflect.TypeFor[T]())
	kvo.NewProp(s, 1, "name", kvo.TMachineString, nil)
	kvo.NewProp(s, 2, "id", kvo.TUint64, nil)
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(1))
		if storedKey {
			b.Prop(s.MustPropByCode(2), kvo.PrimaryKey)
		}
	})
	sc := new(Schema)
	var idx *IndexDef
	tbl := DefineTable[uint64](sc, "rows", m, func(b *TableBuilder[uint64]) {
		idx = b.AddIndex[string]("name", nil).IndexDef()
		if format != MessagePack {
			b.SetRecordStorage(RecordStorage{Format: format})
		}
		b.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.GetValue(1).AsString()) })
	}).TableDef()
	return setup(t, sc), tbl, idx, kvo.Bind(reflect.TypeFor[T](), m)
}

func requireRecordPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("expected missing/conflicting key panic")
		}
	}()
	f()
}

func TestKeylessRecordPutAndDelete(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, tbl, idx, _ := keyedRecordTable[keylessRecordRow](t, format, false)
			db.Write(TxMetadata{}, func(tx *Tx) {
				src := &keylessRecordRow{ID: 99, Name: "before"}
				tx.PutByKey(tbl, uint64(7), src)
				if src.ID != 99 || Get[keylessRecordRow](tx, uint64(7)).ID != 7 {
					t.Fatal("native key handling")
				}
				r, meta, err := tx.TryGetRecord(tbl, uint64(7))
				if err != nil {
					t.Fatal(err)
				}
				if r.Native().(*keylessRecordRow).ID != 0 {
					t.Fatal("KVO invented a key")
				}
				requireRecordPanic(t, func() { tx.Put(tbl, r) })
				requireRecordPanic(t, func() { tx.DeleteRecord(tbl, r) })
				if _, err := tx.TryPutRecordChecked(tbl, uint64(8), r, ValueMeta{}); err != nil {
					t.Fatal(err)
				}
				before, after := tx.PutByKey(tbl, uint64(7), &keylessRecordRow{Name: "after"})
				if !after.IsModified(before) {
					t.Fatal("replacement not tracked")
				}
				if _, err := tx.TryPutRecordChecked(tbl, uint64(7), r, meta); err == nil {
					t.Fatal("stale record accepted")
				}
				if Get[keylessRecordRow](tx, uint64(7)).Name != "after" || Get[keylessRecordRow](tx, uint64(8)).Name != "before" {
					t.Fatal("replacement affected wrong key")
				}
				if !tx.DeleteByKey(tbl, uint64(8)) || tx.DeleteByKey(tbl, uint64(8)) {
					t.Fatal("keyed delete")
				}
				c := ExactIndexScan[keylessRecordRow](tx, idx, "before")
				if c.Next() {
					t.Fatal("delete left stale index")
				}
			})
		})
	}
}

func TestStoredRecordPrimaryKey(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, tbl, _, binding := keyedRecordTable[storedKeyRecordRow](t, format, true)
			db.Write(TxMetadata{}, func(tx *Tx) {
				src := &storedKeyRecordRow{ID: 7, Name: "stored"}
				r := binding.Record(reflect.ValueOf(src))
				tx.Put(tbl, r)
				got, meta, err := tx.TryGetRecord(tbl, uint64(7))
				if err != nil || got.GetValue(2).AsUint() != 7 || got.Fastest().GetValue(2).AsUint() != 7 {
					t.Fatal("stored PK round trip", err)
				}
				if _, err := tx.TryPutRecordChecked(tbl, uint64(8), got, ValueMeta{}); err == nil {
					t.Fatal("conflicting key accepted")
				}
				requireRecordPanic(t, func() { tx.PutByKey(tbl, uint64(8), src) })
				if GetMeta[storedKeyRecordRow](tx, uint64(7)) != meta || Get[storedKeyRecordRow](tx, uint64(8)) != nil {
					t.Fatal("failed write mutated storage")
				}
				if !tx.DeleteRecord(tbl, got) {
					t.Fatal("record-derived delete")
				}
				var w row2.Writer
				w.Begin(0, "", row2.Object, row2.Fast2)
				w.Put(1, "", values.String("missing"), row2.Data)
				w.End()
				missing := binding.Encoded(row2.Trusted(w.BytesWritten()))
				requireRecordPanic(t, func() { tx.Put(tbl, missing) })
			})
			zero := &storedKeyRecordRow{}
			raw := binding.Encode(nil, reflect.ValueOf(zero).Elem(), row2.Fast2)
			if !binding.Encoded(row2.Trusted(raw)).Has(2) {
				t.Fatal("explicit zero key omitted by codec")
			}
		})
	}
}

func TestNewStoredRecordPrimaryKey(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, table, _, _ := keyedRecordTable[storedKeyRecordRow](t, format, true)
			db.Write(TxMetadata{}, func(tx *Tx) {
				for _, key := range []uint64{7, 8} {
					edit := tx.NewRecordEdit(table, key)
					if !edit.Record().Has(2) || edit.Record().GetValue(2).AsUint() != key {
						t.Fatalf("new record omitted or changed stored key %d", key)
					}
					edit.Editor().SetValue(1, values.String("created"))
					if _, err := tx.TrySaveRecord(edit); err != nil {
						t.Fatal(err)
					}
					got := Get[storedKeyRecordRow](tx, key)
					if got == nil || got.ID != key || got.Name != "created" {
						t.Fatal("new record did not round-trip", got)
					}
				}
			})
		})
	}
}

type newStoredKeyRow[Key any] struct {
	Key Key `msgpack:"key" kvo:"key"`
}
type newStoredCompositeKey struct {
	Tenant uint64 `kvo:"tenant"`
	Name   string `kvo:"name"`
}

func TestNewStoredRecordGeneralKey(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		testNewStoredRecordGeneralKey(t, "some/key", func(*kvo.Schema) kvo.AnyType { return kvo.TMachineString })
	})
	t.Run("composite", func(t *testing.T) {
		testNewStoredRecordGeneralKey(t, newStoredCompositeKey{7, "some/key"}, func(s *kvo.Schema) kvo.AnyType {
			typ := kvo.NewEntityType(s, "key", reflect.TypeFor[newStoredCompositeKey]())
			tenant := kvo.NewUnsignedProp[uint64](s, 2, "tenant", kvo.TUint64)
			name := kvo.NewStringProp[string](s, 3, "name", kvo.TMachineString)
			kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) { b.Prop(tenant); b.Prop(name) })
			return typ
		})
	})
}

func testNewStoredRecordGeneralKey[Key comparable](t *testing.T, key Key, keyType func(*kvo.Schema) kvo.AnyType) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			s := kvo.NewSchema()
			prop := kvo.NewProp(s, 1, "key", keyType(s), nil)
			model := kvo.NewModel(s, kvo.NewEntityType(s, "row", reflect.TypeFor[newStoredKeyRow[Key]]()), func(b *kvo.ModelBuilder) {
				b.Prop(s.MustPropByCode(prop), kvo.PrimaryKey)
			})
			schema := new(Schema)
			table := DefineTable(schema, "rows", model, func(b *TableBuilder[Key]) {
				if format != MessagePack {
					b.SetRecordStorage(RecordStorage{Format: format})
				}
			}).TableDef().Typed[Key]()
			db := setup(t, schema)
			db.Write(TxMetadata{}, func(tx *Tx) {
				edit := table.EditOrCreate(tx, key)
				if !table.Save(tx, edit) || table.Key(edit) != key {
					t.Fatal("new record was not saved under the supplied key")
				}
				if table.Save(tx, table.EditOrCreate(tx, key)) {
					t.Fatal("unchanged record was rewritten")
				}
			})
			db.Read(func(tx *Tx) {
				if row := Get[newStoredKeyRow[Key]](tx, key); row == nil || row.Key != key || !table.Get(tx, key).Has(prop) {
					t.Fatal("stored primary key was lost", row)
				}
			})
		})
	}
}

func TestPutRecordPreservesUnknownMessagePack(t *testing.T) {
	db, tbl, _, binding := keyedRecordTable[keylessRecordRow](t, MessagePack, false)
	raw, err := msgpack.TryEncode(nil, reflect.ValueOf(&keylessRecordRow{Name: "unknown"}))
	if err != nil {
		t.Fatal(err)
	}
	extra := []byte{0xd6, 42, 1, 2, 3, 4}
	raw = msgpack.Trusted(raw).Set("future", extra).Raw()
	db.Write(TxMetadata{}, func(tx *Tx) {
		if _, err := tx.TryPutRecordChecked(tbl, uint64(7), binding.MessagePack(raw), ValueMeta{}); err != nil {
			t.Fatal(err)
		}
		r, _, err := tx.TryGetRecord(tbl, uint64(7))
		if err != nil || !bytes.Equal(r.MessagePack(), raw) {
			t.Fatal("unknown payload rewritten", err)
		}
	})
}
