package edb

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

type typedTestRow[Key any] struct {
	Key   Key    `msgpack:"-" kvo:"-"`
	Name  string `msgpack:"n" kvo:"name"`
	Group string `msgpack:"g" kvo:"group"`
}

type typedStringKey string

type customStringKey string

func (k customStringKey) MarshalBinary() ([]byte, error) { return []byte("key:" + k), nil }
func (k *customStringKey) UnmarshalBinary(raw []byte) error {
	value, ok := strings.CutPrefix(string(raw), "key:")
	if !ok {
		return fmt.Errorf("missing key prefix")
	}
	*k = customStringKey(value)
	return nil
}

func typedTestTable[Key any](t testing.TB, format StorageFormat) (*DB, RowTable[typedTestRow[Key], Key], *IndexDef, *IndexDef) {
	t.Helper()
	models := kvo.NewSchema()
	kvo.NewProp(models, 1, "name", kvo.TMachineString, nil)
	kvo.NewProp(models, 2, "group", kvo.TMachineString, nil)
	model := kvo.NewModel(models, kvo.NewEntityType(models, "item", reflect.TypeFor[typedTestRow[Key]]()), func(b *kvo.ModelBuilder) {
		b.Prop(models.MustPropByCode(1))
		b.Prop(models.MustPropByCode(2))
	})
	schema := new(Schema)
	var byName, byGroup *IndexDef
	table := DefineTable(schema, "items", model, func(b *TableBuilder[Key]) {
		if format != MessagePack {
			b.SetRecordStorage(RecordStorage{Format: format})
		}
		byName = b.AddIndex[string]("name", func(tx *Tx, pk values.Value, r kvo.Record, yield func(string)) {
			if tx == nil {
				t.Fatal("missing transaction")
			}
			yield(r.GetValue(1).AsString())
		}, IndexOptDeprecatedUnique).IndexDef()
		byGroup = b.AddIndex[string]("group", nil).IndexDef()
		b.AddIndex[Key]("key", func(_ *Tx, pk values.Value, r kvo.Record, yield func(Key)) { yield(IndexPrimaryKey[Key](pk)) }).IndexDef()
		b.SetIndexer(func(key Key, r kvo.Record, ib *IndexBuilder) {
			if !reflect.DeepEqual(key, ib.PrimaryKey()) {
				t.Fatal("incorrect typed indexer key")
			}
			ib.Add(byGroup, r.GetValue(2).AsString())
		})
	}).TableDef()
	return setup(t, schema), table.Rows[typedTestRow[Key], Key](), byName, byGroup
}

func TestTypedTablesAndLookup(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			t.Run("named ID", func(t *testing.T) { testTypedTables(t, format, ID(1), ID(2), ID(3)) })
			t.Run("small uint", func(t *testing.T) { testTypedTables(t, format, uint16(256), uint16(257), uint16(258)) })
			t.Run("custom codec", func(t *testing.T) { testTypedTables(t, format, customUintKey(1), customUintKey(2), customUintKey(3)) })
			t.Run("string", func(t *testing.T) { testTypedTables(t, format, "a", "b", "c") })
			t.Run("named string", func(t *testing.T) {
				testTypedTables(t, format, typedStringKey("a"), typedStringKey("b"), typedStringKey("c"))
			})
			t.Run("custom string", func(t *testing.T) {
				testTypedTables(t, format, customStringKey("a"), customStringKey("b"), customStringKey("c"))
			})
			t.Run("composite", func(t *testing.T) { testTypedTables(t, format, AB{1, 1}, AB{1, 2}, AB{1, 3}) })
		})
	}
}

func testTypedTables[Key comparable](t *testing.T, format StorageFormat, first, second, third Key) {
	db, rows, byName, byGroup := typedTestTable[Key](t, format)
	records := rows.Records()
	unique, multiple := records.bindIndex[string](byName), records.bindIndex[string](byGroup)
	native := rows.bindIndex[string](byGroup)
	db.Write(TxMetadata{}, func(tx *Tx) {
		rows.Put(tx, &typedTestRow[Key]{second, "bob", "vip"})
		rows.Put(tx, &typedTestRow[Key]{first, "alice", "vip"})
	})
	db.Read(func(tx *Tx) {
		if r := records.Get(tx, first); r.GetValue(1).AsString() != "alice" {
			t.Fatal("record lookup")
		}
		if r := rows.Get(tx, first); r == nil || r.Key != first || r.Name != "alice" {
			t.Fatal("row lookup", r)
		}
		if !records.Exists(tx, first) || records.Exists(tx, third) || !records.Get(tx, third).Missing() || rows.Get(tx, third) != nil {
			t.Fatal("missing key handling")
		}
		if key, r := unique.Lookup(tx, "bob"); key != second || r.GetValue(1).AsString() != "bob" {
			t.Fatal("unique lookup", key)
		}
		if key, r := multiple.Lookup(tx, "vip"); key != first || r.GetValue(1).AsString() != "alice" {
			t.Fatal("nonunique lookup did not choose first primary key", key)
		}
		if key, r := native.Lookup(tx, "vip"); key != first || r.Key != first || r.Name != "alice" {
			t.Fatal("native nonunique lookup", key, r)
		}
		if key, ok := multiple.LookupKey(tx, "vip"); !ok || key != first {
			t.Fatal("key-only lookup", key, ok)
		}
		if key, r := multiple.Lookup(tx, "none"); key != *new(Key) || !r.Missing() {
			t.Fatal("missing index lookup", key)
		}
		if multiple.Exists(tx, "none") {
			t.Fatal("missing index entry exists")
		}
		raw := rows.Table().EncodeKey(first)
		if rows.GetRaw(tx, raw).Key != first || records.GetRaw(tx, raw).GetValue(1).AsString() != "alice" {
			t.Fatal("raw key lookup")
		}
	})
	db.Write(TxMetadata{}, func(tx *Tx) {
		edit := records.Edit(tx, first)
		edit.Editor().SetValue(1, values.String("ann"))
		if !records.Save(tx, edit) || records.Save(tx, edit) {
			t.Fatal("save change result")
		}
		copy := records.Get(tx, first)
		if records.Put(tx, first, copy) {
			t.Fatal("identical put reported change")
		}
		fresh := records.New(tx, third)
		fresh.Editor().SetValue(1, values.String("chris"))
		if !records.Save(tx, fresh) || records.Get(tx, third).Missing() {
			t.Fatal("new record")
		}
		if !rows.Delete(tx, second) || records.Delete(tx, second) || unique.Exists(tx, "bob") {
			t.Fatal("delete left row or indexes")
		}
	})
}

func TestTypedHandleValidation(t *testing.T) {
	db, rows, byName, _ := typedTestTable[ID](t, MessagePack)
	_ = db
	table := rows.Table()
	for _, f := range []func(){
		func() { table.Typed[uint64]() },
		func() { table.Rows[User, ID]() },
		func() { rows.Records().bindIndex[int](byName) },
		func() { rows.bindIndex[string](usersByName) },
	} {
		assertPanics(t, f)
	}
}

func TestOrdinaryAndCheckedRecordWrites(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, rows, _, _ := typedTestTable[AB](t, format)
			table := rows.Records()
			name := kvo.BindStringProp[string](table.Model().Type().Schema(), 1)
			key := AB{9, 4}
			var retained *RecordEdit
			db.Write(TxMetadata{}, func(tx *Tx) {
				if table.Edit(tx, key) != nil {
					t.Fatal("missing edit")
				}
				e := table.EditOrCreate(tx, key)
				if table.Key(e) != key {
					t.Fatal("composite key")
				}
				e.Set(name, "first")
				if !table.Save(tx, e) || table.Save(tx, e) {
					t.Fatal("save result")
				}
				_, before, err := table.TryGet(tx, key)
				if err != nil {
					t.Fatal(err)
				}
				e = table.EditOrCreate(tx, key)
				if e.Get(name) != "first" {
					t.Fatal("existing row lost")
				}
				e.Set(name, "second")
				if !table.Save(tx, e) {
					t.Fatal("update result")
				}
				if _, err := table.TryPutChecked(tx, key, e.Record(), before); err == nil {
					t.Fatal("stale replacement accepted")
				}
				r, current, err := table.TryGet(tx, key)
				if err != nil {
					t.Fatal(err)
				}
				if table.PutChecked(tx, key, r, current) {
					t.Fatal("unchanged checked write")
				}
				retained = e
			})
			db.Write(TxMetadata{}, func(tx *Tx) {
				if _, err := table.TrySave(tx, retained); err == nil {
					t.Fatal("cross transaction edit accepted")
				}
				if table.Get(tx, key).Get(name) != "second" {
					t.Fatal("committed value")
				}
			})
		})
	}
}
