package edb

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

type changeRecordRow struct {
	ID   string   `msgpack:"-" kvo:"-"`
	Name string   `kvo:"name" msgpack:"name"`
	Tags []string `kvo:"tags" msgpack:"tags"`
}

func changeRecordSchema(format StorageFormat, configure ...func(*TableBuilder[string])) (*Schema, *TableDef) {
	s := kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	tags := kvo.NewProp(s, 2, "tags", kvo.ListOf(kvo.TMachineString), nil)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "change", reflect.TypeFor[changeRecordRow]()), func(b *kvo.ModelBuilder) { b.Prop(name); b.Prop(s.MustPropByCode(tags)) })
	schema := new(Schema)
	table := DefineTable(schema, "changes", m, func(b *TableBuilder[string]) {
		if format != MessagePack {
			b.SetRecordStorage(RecordStorage{Format: format})
		}
		for _, f := range configure {
			f(b)
		}
	}).TableDef()
	return schema, table
}

func TestChangeRecordsAndGoStructs(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			schema, table := changeRecordSchema(format)
			db := setup(t, schema)
			var changes []*Change
			input := &changeRecordRow{"key", "first", []string{"vip"}}
			db.Write(func(tx *Tx) {
				tx.OnChange(map[*TableDef]ChangeFlags{table: ChangeFlagNotify | ChangeFlagIncludeKey | ChangeFlagIncludeNewRecord | ChangeFlagIncludeOldRecord | ChangeFlagIncludeNewGoStruct | ChangeFlagIncludeOldGoStruct}, func(tx *Tx, c *Change) {
					if c.Key() != "key" {
						t.Fatalf("key = %v", c.Key())
					}
					for _, side := range []struct {
						r kvo.Record
						v any
					}{{c.OldRecord(), c.OldGoStruct()}, {c.NewRecord(), c.NewGoStruct()}} {
						if side.r.Missing() != (side.v == nil) {
							t.Fatal("inconsistent sides")
						}
						if side.v != nil {
							v := side.v.(*changeRecordRow)
							if v.ID != "key" || side.r.GetValue(1).AsString() != v.Name || len(v.Tags) != 1 || v.Tags[0] != "vip" {
								t.Fatalf("incorrect snapshot: %+v", v)
							}
							before := side.r.GetValue(1).AsString()
							v.Name = "callback mutation"
							v.Tags[0] = "callback mutation"
							if side.r.GetValue(1).AsString() != before {
								t.Fatal("Go struct aliases record")
							}
						}
					}
					if c.Op() == OpDelete && Get[changeRecordRow](tx, "key") != nil {
						t.Fatal("delete callback ran before deletion")
					}
					c.newRecord = c.NewRecord().Own()
					c.oldRecord = c.OldRecord().Own()
					changes = append(changes, c)
				})
				Put(tx, input)
				if input.Name != "first" || input.Tags[0] != "vip" {
					t.Fatal("notification mutated input")
				}
				Put(tx, input) // no-op
				edit := tx.OpenRecordEdit(table, "key")
				edit.Editor().SetValue(1, values.String("second"))
				tx.SaveRecord(edit)
				tx.SaveRecord(edit) // no-op
				tx.DeleteByKey(table, "key")
				if tx.DeleteByKey(table, "key") {
					t.Fatal("second delete succeeded")
				}
			})
			if len(changes) != 3 {
				t.Fatalf("changes = %d", len(changes))
			}
			for i, c := range changes {
				wantOld, wantNew := []string{"", "first", "second"}[i], []string{"first", "second", ""}[i]
				if c.HasOldRecord() != (wantOld != "") || c.HasNewRecord() != (wantNew != "") {
					t.Fatalf("wrong presence at %d", i)
				}
				if wantOld != "" && c.OldRecord().GetValue(1).AsString() != wantOld {
					t.Fatalf("old at %d", i)
				}
				if wantNew != "" && c.NewRecord().GetValue(1).AsString() != wantNew {
					t.Fatalf("new at %d", i)
				}
			}
		})
	}
}

func TestChangeCallbackWritesAndRollback(t *testing.T) {
	schema, table := changeRecordSchema(MessagePack)
	db := setup(t, schema)
	db.Write(func(tx *Tx) { Put(tx, &changeRecordRow{ID: "key", Name: "old"}) })
	calls := 0
	db.Write(func(tx *Tx) {
		tx.OnChange(map[*TableDef]ChangeFlags{table: ChangeFlagNotify | ChangeFlagIncludeOldRecord | ChangeFlagIncludeNewRecord}, func(tx *Tx, c *Change) {
			calls++
			if c.Op() == OpDelete {
				Put(tx, &changeRecordRow{ID: "key", Name: "replacement"})
				if c.OldRecord().GetValue(1).AsString() != "old" {
					t.Fatal("callback write invalidated old record")
				}
			}
		})
		tx.DeleteByKey(table, "key")
	})
	db.Read(func(tx *Tx) {
		if v := Get[changeRecordRow](tx, "key"); v == nil || v.Name != "replacement" {
			t.Fatal("callback replacement was deleted", v)
		}
	})
	if calls != 2 {
		t.Fatal(calls)
	}
	rollback := errors.New("rollback")
	err := db.Tx(true, func(tx *Tx) error {
		tx.OnChange(map[*TableDef]ChangeFlags{table: ChangeFlagNotify | ChangeFlagIncludeNewRecord}, func(_ *Tx, c *Change) {
			calls++
			if c.NewRecord().GetValue(1).AsString() != "uncommitted" {
				t.Fatal("missing uncommitted value")
			}
		})
		Put(tx, &changeRecordRow{ID: "key", Name: "uncommitted"})
		return rollback
	})
	if !errors.Is(err, rollback) || calls != 3 {
		t.Fatal(err, calls)
	}
	db.Read(func(tx *Tx) {
		if Get[changeRecordRow](tx, "key").Name != "replacement" {
			t.Fatal("rollback persisted")
		}
	})
}

func TestChangeRecordRegistrationRequiresModel(t *testing.T) {
	schema := new(Schema)
	table := DefineTable[string](schema, "unmodeled", testTableModel[changeRecordRow](), func(*TableBuilder[string]) {}).TableDef()
	table.kvoModel = nil
	table.projection = nil
	db := setup(t, schema)
	db.Write(func(tx *Tx) {
		for _, flags := range []ChangeFlags{ChangeFlagIncludeOldRecord, ChangeFlagIncludeNewRecord} {
			func() {
				defer func() {
					if err := recover(); err != ErrNoRecordBinding {
						t.Fatalf("registration panic = %v", err)
					}
				}()
				tx.OnChange(map[*TableDef]ChangeFlags{table: ChangeFlagNotify | flags}, func(*Tx, *Change) {})
			}()
		}
		called := false
		tx.OnChange(map[*TableDef]ChangeFlags{table: ChangeFlagNotify | ChangeFlagIncludeNewGoStruct}, func(_ *Tx, c *Change) {
			called = true
			if c.NewGoStruct().(*changeRecordRow).Name != "works" {
				t.Fatal("wrong Go struct")
			}
		})
		Put(tx, &changeRecordRow{ID: "key", Name: "works"})
		if !called {
			t.Fatal("missing notification")
		}
	})
}

// Measures only the notification boundary on already encoded wide values, so
// index construction, storage I/O, and encoding do not obscure decode costs.
func BenchmarkChangeNotifications(b *testing.B) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		for _, mode := range []struct {
			name  string
			flags ChangeFlags
		}{
			{"key", ChangeFlagIncludeKey},
			{"records", ChangeFlagIncludeKey | ChangeFlagIncludeOldRecord | ChangeFlagIncludeNewRecord},
			{"GoStructs", ChangeFlagIncludeKey | ChangeFlagIncludeOldGoStruct | ChangeFlagIncludeNewGoStruct},
		} {
			b.Run(format.String()+"/"+mode.name, func(b *testing.B) {
				schema, table := changeRecordSchema(format)
				db := setup(b, schema)
				input := &changeRecordRow{ID: "key", Name: strings.Repeat("name", 32), Tags: make([]string, 100)}
				for i := range input.Tags {
					input.Tags[i] = "customer-tag"
				}
				db.Write(func(tx *Tx) { Put(tx, input) })
				tx := db.BeginRead()
				defer tx.Close()
				rawKey := table.EncodeKey("key")
				raw := tx.stx.Bucket(table.name, dataBucketName).Get(rawKey)
				var v value
				if err := v.decode(raw, false); err != nil {
					b.Fatal(err)
				}
				tx.OnChange(map[*TableDef]ChangeFlags{table: ChangeFlagNotify | mode.flags}, func(_ *Tx, c *Change) {
					if c.HasNewRecord() && c.NewRecord().GetValue(1).AsString() != input.Name {
						b.Fatal("record")
					}
					if c.HasNewGoStruct() && c.NewGoStruct().(*changeRecordRow).Name != input.Name {
						b.Fatal("Go struct")
					}
				})
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					tx.notifyChange(table, OpPut, rawKey, &v, &v)
				}
			})
		}
	}
}

var changeOpaqueDecodes int

type changeOpaque string

func (s *changeOpaque) UnmarshalMsgpack(raw []byte) error {
	changeOpaqueDecodes++
	var decoded string
	if err := MsgPack.DecodeValue(raw, reflect.ValueOf(&decoded)); err != nil {
		return err
	}
	*s = changeOpaque(decoded)
	return nil
}

type changeProjectedRow struct {
	ID     uint64 `msgpack:"-"`
	Name   string
	Opaque changeOpaque
}

func TestChangeProjectionDecodesGoStructsOnlyOnRequest(t *testing.T) {
	s := kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "projection"), func(b *kvo.ModelBuilder) { b.Prop(name) })
	schema := new(Schema)
	m.RegisterNativeType(reflect.TypeFor[changeProjectedRow](), map[kvo.PropCode]string{1: "Name"})
	table := DefineTable(schema, "projection", m, func(b *TableBuilder[uint64]) {
	}).TableDef()
	db := setup(t, schema)
	db.Write(func(tx *Tx) { Put(tx, &changeProjectedRow{1, "old", "unmodeled"}) })
	for _, goStructs := range []bool{false, true} {
		db.Write(func(tx *Tx) {
			flags := ChangeFlagNotify | ChangeFlagIncludeNewRecord | ChangeFlagIncludeOldRecord
			if goStructs {
				flags |= ChangeFlagIncludeNewGoStruct | ChangeFlagIncludeOldGoStruct
			}
			calls := 0
			tx.OnChange(map[*TableDef]ChangeFlags{table: flags}, func(_ *Tx, c *Change) {
				calls++
				for _, r := range []kvo.Record{c.OldRecord(), c.NewRecord()} {
					if r.MessagePack() == nil || r.Get(name) == "" {
						t.Fatal("expected direct MessagePack record")
					}
				}
				if goStructs {
					for _, v := range []any{c.OldGoStruct(), c.NewGoStruct()} {
						row := v.(*changeProjectedRow)
						if row.ID != 1 || row.Opaque != "unmodeled" {
							t.Fatal("projection lost full struct fields", row)
						}
					}
				}
			})
			changeOpaqueDecodes = 0
			edit := tx.OpenRecordEdit(table, uint64(1))
			next := "encoded"
			if goStructs {
				next = "with structs"
			}
			edit.Editor().SetValue(1, values.String(next))
			tx.SaveRecord(edit)
			want := 0
			if goStructs {
				want = 2
			}
			if calls != 1 || changeOpaqueDecodes != want {
				t.Fatalf("notifications=%d, full decodes=%d, want %d", calls, changeOpaqueDecodes, want)
			}
		})
	}
}
