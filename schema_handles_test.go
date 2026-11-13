package edb

import (
	"reflect"
	"slices"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func handleTestTable[Key any](t testing.TB, format StorageFormat) (*DB, *Table[Key], *Index[string]) {
	t.Helper()
	models := kvo.NewSchema()
	name := kvo.NewStringProp[string](models, 1, "name", kvo.TMachineString)
	group := kvo.NewStringProp[string](models, 2, "group", kvo.TMachineString)
	model := kvo.NewModel(models, kvo.NewEntityType(models, "handle_item", reflect.TypeFor[typedTestRow[Key]]()), func(b *kvo.ModelBuilder) { b.Prop(name); b.Prop(group) })
	schema := new(Schema)
	var index *Index[string]
	table := DefineTable(schema, "items", model, func(b *TableBuilder[Key]) {
		if format != MessagePack {
			b.SetRecordStorage(RecordStorage{Format: format})
		}
		index = b.AddIndex("group", func(_ *Tx, _ values.Value, r kvo.Record, yield func(string)) { yield(r.Get(group)) })
	})
	return setup(t, schema), table, index
}

func TestTypedHandles(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			t.Run("ID", func(t *testing.T) { testTypedHandles(t, format, ID(1), ID(2)) })
			t.Run("string", func(t *testing.T) { testTypedHandles(t, format, "alice", "bob") })
			t.Run("composite", func(t *testing.T) { testTypedHandles(t, format, AB{1, 1}, AB{1, 2}) })
		})
	}
}

func testTypedHandles[Key comparable](t *testing.T, format StorageFormat, first, second Key) {
	db, table, index := handleTestTable[Key](t, format)
	var anyTable AnyTable = table
	var anyIndex AnyIndex = index
	if anyTable.TableDef() != table.TableDef() || anyIndex.IndexDef() != index.IndexDef() {
		t.Fatal("definition identity")
	}
	if db.schema.TableNamed("items") != table.TableDef() {
		t.Fatal("schema identity")
	}
	db.Write(TxMetadata{}, func(tx *Tx) {
		rows := table.Rows[typedTestRow[Key]]()
		rows.Put(tx, &typedTestRow[Key]{second, "bob", "vip"})
		rows.Put(tx, &typedTestRow[Key]{first, "alice", "vip"})
		id, r := table.Lookup(tx, index, "vip")
		if id != first || r.GetValue(1).AsString() != "alice" {
			t.Fatal("first-match lookup")
		}
		got := slices.Collect(table.ExactIndexScan(tx, index, "vip").Keys())
		if !slices.Equal(got, []Key{first, second}) {
			t.Fatalf("keys %v", got)
		}
		got = nil
		for id, r := range table.RangeIndexScan(tx, index, "vip", "vip").Entries() {
			if r.GetValue(2).AsString() != "vip" {
				t.Fatal("record")
			}
			got = append(got, id)
		}
		if !slices.Equal(got, []Key{first, second}) {
			t.Fatal("entries")
		}
		if id, ok := table.LookupKey(tx, index, "missing"); ok || id != *new(Key) {
			t.Fatal("missing lookup")
		}
		for _, tbl := range []AnyTable{table, table.TableDef()} {
			c := tx.TableScan(tbl, FullScan())
			if !c.Next() || c.Table() != table.TableDef() {
				t.Fatal("table cursor identity")
			}
		}
		for _, idx := range []AnyIndex{index, index.IndexDef()} {
			c := tx.IndexScan(idx, ExactScan("vip"))
			if !c.Next() || c.Table() != table.TableDef() {
				t.Fatal("index cursor identity")
			}
		}
		edit := table.Edit(tx, first)
		edit.Editor().SetValue(2, values.String("other"))
		table.Save(tx, edit)
		if got := slices.Collect(table.ExactIndexScan(tx, index, "vip").Keys()); !slices.Equal(got, []Key{second}) {
			t.Fatal("updated index")
		}
		tx.Reindex(table, nil)
		tx.Reindex(table, (*Index[string])(nil))
		table.Delete(tx, second)
		if got := slices.Collect(table.ExactIndexScan(tx, index, "vip").Keys()); len(got) != 0 {
			t.Fatal("deleted index")
		}
	})
	other := &Index[string]{&IndexDef{table: &TableDef{name: "other"}, name: "group"}}
	db.Read(func(tx *Tx) {
		defer func() {
			if recover() == nil {
				t.Error("foreign index accepted")
			}
		}()
		table.ExactIndexScan(tx, other, "vip")
	})
}

//go:noinline
func opaqueTable(t AnyTable) AnyTable { return t }

//go:noinline
func opaqueIndex(i AnyIndex) AnyIndex { return i }

func BenchmarkHandleScanEntry(b *testing.B) {
	db, table, index := handleTestTable[uint64](b, Fast2)
	db.Write(TxMetadata{}, func(tx *Tx) { table.Rows[typedTestRow[uint64]]().Put(tx, &typedTestRow[uint64]{1, "alice", "vip"}) })
	db.Read(func(tx *Tx) {
		at, ai := opaqueTable(table), opaqueIndex(index)
		for _, tc := range []struct {
			name string
			scan func() RawCursor
		}{
			{"table/concrete", func() RawCursor { return tx.newTableCursor(table.TableDef(), FullScan()) }},
			{"table/interface", func() RawCursor { return tx.TableScan(at, FullScan()) }},
			{"index/concrete", func() RawCursor { return tx.newIndexCursor(index.IndexDef(), FullScan()) }},
			{"index/interface", func() RawCursor { return tx.IndexScan(ai, FullScan()) }},
		} {
			b.Run(tc.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					c := tc.scan()
					if !c.Next() {
						b.Fatal("missing row")
					}
				}
			})
		}
	})
}
