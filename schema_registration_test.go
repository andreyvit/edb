package edb

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func TestDefineTableKeyType(t *testing.T) {
	type row struct{ ID uint64 }
	type namedRow struct{ ID ID }
	type otherID uint64
	type alias = ID
	type compoundRow struct{ Key AB }

	tests := []struct {
		name   string
		define func(*Schema, *bool) *TableDef
		want   reflect.Type
	}{
		{"scalar", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[row](), func(*TableBuilder[uint64]) { *called = true }).TableDef()
		}, reflect.TypeFor[uint64]()},
		{"named", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[namedRow](), func(*TableBuilder[ID]) { *called = true }).TableDef()
		}, reflect.TypeFor[ID]()},
		{"alias", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[namedRow](), func(*TableBuilder[alias]) { *called = true }).TableDef()
		}, reflect.TypeFor[ID]()},
		{"compound", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[compoundRow](), func(*TableBuilder[AB]) { *called = true }).TableDef()
		}, reflect.TypeFor[AB]()},
		{"wrong kind", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[row](), func(*TableBuilder[string]) { *called = true }).TableDef()
		}, nil},
		{"underlying type", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[namedRow](), func(*TableBuilder[uint64]) { *called = true }).TableDef()
		}, nil},
		{"different named type", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[namedRow](), func(*TableBuilder[otherID]) { *called = true }).TableDef()
		}, nil},
		{"different compound type", func(s *Schema, called *bool) *TableDef {
			return DefineTable(s, "rows", testTableModel[compoundRow](), func(*TableBuilder[CD]) { *called = true }).TableDef()
		}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := new(Schema)
			called := false
			if tt.want == nil {
				assertRegistrationPanic(t, "DefineTable(rows): Key", func() { tt.define(s, &called) })
				if called || len(s.Tables()) != 0 || s.TableNamed("rows") != nil || len(s.tablesByRowType) != 0 {
					t.Fatal("invalid declaration called builder or registered table")
				}
				return
			}
			tbl := tt.define(s, &called)
			if !called || tbl.KeyType() != tt.want || s.TableNamed("rows") != tbl {
				t.Fatalf("valid declaration: called=%v, key=%v, table=%v", called, tbl.KeyType(), s.TableNamed("rows"))
			}
		})
	}
}

func assertRegistrationPanic(t *testing.T, contains string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		p := recover()
		if p == nil || !strings.Contains(fmt.Sprint(p), contains) {
			t.Errorf("panic = %v, want message containing %q", p, contains)
		}
	}()
	f()
}

func TestDuplicateIndexRegistration(t *testing.T) {
	t.Run("table", func(t *testing.T) {
		var first, second *IndexDef
		tbl := DefineTable(new(Schema), "rows", testTableModel[User](), func(b *TableBuilder[ID]) {
			first = b.AddIndex[string]("email", nil).IndexDef()
			second = b.AddIndex[string]("name", nil).IndexDef()
			assertRegistrationPanic(t, `table rows already has index named "email"`, func() {
				b.AddIndex[string]("email", nil).IndexDef()
			})
		}).TableDef()
		if !reflect.DeepEqual(tbl.Indices(), []*IndexDef{first, second}) || tbl.IndexNamed("email") != first || tbl.IndexNamed("name") != second {
			t.Fatal("duplicate registration changed index list or name lookup")
		}
		if first.pos != 0 || second.pos != 1 || first.Table() != tbl || second.Table() != tbl {
			t.Fatal("incorrect index position or owner")
		}
		// Index names are local to a table, not to the schema.
		DefineTable(tbl.schema, "other_rows", testTableModel[Post](), func(b *TableBuilder[string]) {
			b.AddIndex[string]("email", nil).IndexDef()
		}).TableDef()
	})
	t.Run("KV table", func(t *testing.T) {
		scm := new(Schema)
		DefineKVTable(scm, "rows", nil, nil, func(b *KVTableBuilder) {
			first := b.DefineIndex("email", nil, nil, nil)
			second := b.DefineIndex("name", nil, nil, nil)
			assertRegistrationPanic(t, `table rows already has index named "email"`, func() {
				b.DefineIndex("email", nil, nil, nil)
			})
			tbl := b.table
			if !reflect.DeepEqual(tbl.indices, []*KVIndex{first, second}) || tbl.indicesByName["email"] != first || tbl.indicesByName["name"] != second {
				t.Fatal("duplicate registration changed index list or name lookup")
			}
			if first.Table() != tbl || second.Table() != tbl {
				t.Fatal("incorrect index owner")
			}
		})
		DefineKVTable(scm, "other_rows", nil, nil, func(b *KVTableBuilder) {
			b.DefineIndex("email", nil, nil, nil)
		})
	})
}

func TestIndexCannotChangeTables(t *testing.T) {
	s := new(Schema)
	var index *IndexDef
	first := DefineTable(s, "users", testTableModel[User](), func(b *TableBuilder[ID]) {
		index = b.AddIndex[string]("name", nil).IndexDef()
	}).TableDef()
	second := DefineTable(s, "posts", testTableModel[Post](), func(*TableBuilder[string]) {}).TableDef()
	assertRegistrationPanic(t, "already belongs", func() { second.addIndex(index) })
	if index.Table() != first || first.IndexNamed("name") != index || second.IndexNamed("name") != nil || len(second.Indices()) != 0 {
		t.Fatal("rejected reuse changed index owner or table registration")
	}
}

func TestDefineTableModel(t *testing.T) {
	model := testIndexAccess[User](indexName)
	tbl := DefineTable(new(Schema), "users", model, func(b *TableBuilder[ID]) {
		if b.tbl.Model() != model {
			t.Fatal("model unavailable during configuration")
		}
	}).TableDef()
	if tbl.Model() != model || tbl.projection == nil || tbl.binding != nil {
		t.Fatal("default model access must preserve MessagePack storage")
	}
	assertRegistrationPanic(t, "does not match primary key", func() {
		DefineTable(new(Schema), "widgets", model, func(*TableBuilder[AB]) {}).TableDef()
	})
	assertRegistrationPanic(t, "model is required", func() {
		DefineTable(new(Schema), "users", nil, func(b *TableBuilder[ID]) {
			b.SetRecordStorage(RecordStorage{Format: Fast2})
		}).TableDef()
	})
	assertRegistrationPanic(t, "non-nil native schema", func() {
		DefineTable(new(Schema), "users", model, func(b *TableBuilder[ID]) {
			b.SetNativeSchema(nil)
		}).TableDef()
	})
	shared := tableNativeSchema{}
	assertRegistrationPanic(t, "non-nil native schema", func() {
		DefineTable(new(Schema), "users", model, func(b *TableBuilder[ID]) {
			b.SetNativeSchema(shared)
			b.SetNativeSchema(shared)
		}).TableDef()
	})
	assertRegistrationPanic(t, "model is required", func() {
		DefineTable(new(Schema), "users", nil, func(b *TableBuilder[ID]) {
			b.SetNativeSchema(shared)
		}).TableDef()
	})
}

func testTableModel[Row any]() *kvo.Model {
	s := kvo.NewSchema()
	m := kvo.NewModel(s, kvo.NewEntityType(s, "row"), func(*kvo.ModelBuilder) {})
	m.RegisterNativeType(reflect.TypeFor[Row](), map[kvo.PropCode]string{})
	return m
}

func TestDefineTableUsesFirstNativeBinding(t *testing.T) {
	type first struct{ Key string }
	type second struct{ Key uint64 }
	s := kvo.NewSchema()
	model := kvo.NewModel(s, kvo.NewEntityType(s, "row"), func(*kvo.ModelBuilder) {})
	assertRegistrationPanic(t, "registered native struct", func() { DefineTable(new(Schema), "rows", model, func(*TableBuilder[string]) {}).TableDef() })
	model.RegisterNativeType(reflect.TypeFor[first](), map[kvo.PropCode]string{})
	model.RegisterNativeType(reflect.TypeFor[second](), map[kvo.PropCode]string{})
	table := DefineTable(new(Schema), "rows", model, func(*TableBuilder[string]) {}).TableDef()
	if table.rowType != reflect.TypeFor[first]() || table.KeyType() != reflect.TypeFor[string]() {
		t.Fatal(table.rowType, table.KeyType())
	}
}

func TestTableBuilderIndexRegistration(t *testing.T) {
	var byName, byID *IndexDef
	tbl := DefineTable(new(Schema), "users", testIndexAccess[User](indexName), func(b *TableBuilder[ID]) {
		byName = b.AddIndex("name", func(_ *Tx, _ values.Value, row kvo.Record, yield func(string)) {
			yield(row.Get(indexName))
		}, IndexInitialFillModeBlocking, IndexOptDeprecatedUnique).IndexDef()
		byID = b.AddIndex[ID]("id", nil, IndexOptSkipInitialFill).IndexDef()
		if byName.Table() != b.tbl || byID.Table() != b.tbl || b.tbl.IndexNamed("name") != byName || b.tbl.IndexNamed("id") != byID {
			t.Fatal("returned indexes must already belong to the table")
		}
		assertRegistrationPanic(t, "invalid option", func() {
			b.AddIndex[string]("invalid", nil, IndexOpt(999)).IndexDef()
		})
		if b.tbl.IndexNamed("invalid") != nil || len(b.tbl.Indices()) != 2 {
			t.Fatal("failed registration changed the table")
		}
	}).TableDef()
	if !reflect.DeepEqual(tbl.Indices(), []*IndexDef{byName, byID}) || byName.pos != 0 || byID.pos != 1 {
		t.Fatal("registration order changed")
	}
	if byName.keyType() != reflect.TypeFor[string]() || byID.keyType() != reflect.TypeFor[ID]() {
		t.Fatal("index key type changed")
	}
	if !byName.IsUnique() || byName.initialFillMode != IndexInitialFillModeBlocking || byID.initialFillMode != IndexInitialFillModeSkip {
		t.Fatal("index options changed")
	}
}
