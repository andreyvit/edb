package edb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

func TestReindexMessagePackProjection(t *testing.T) {
	for _, mode := range []string{"encoded", "migration", "notification"} {
		t.Run(mode, func(t *testing.T) {
			s := kvo.NewSchema()
			name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
			model := kvo.NewModel(s, kvo.NewEntityType(s, "member"), func(b *kvo.ModelBuilder) { b.Prop(name) })
			schema := new(Schema)
			var idx *IndexDef
			reindexing := false
			model.RegisterNativeType(reflect.TypeFor[accessMember](), map[kvo.PropCode]string{1: "Name"})
			tbl := DefineTable(schema, "members", model, func(b *TableBuilder[uint64]) {
				idx = b.AddIndex[string]("name", nil).IndexDef()
				b.SetIndexer(func(id uint64, r kvo.Record, ib *IndexBuilder) {
					if id != 42 {
						t.Fatalf("key = %d", id)
					}
					if reindexing && (r.MessagePack() != nil) != true {
						t.Fatalf("unexpected representation for %s", mode)
					}
					ib.Add(idx, r.Get(name))
				})
				if mode == "migration" {
					b.SetSchemaVersion(2)
					b.AddMigrator(2, func(_ *Tx, _ uint64, edit *kvo.Editor) { edit.Set(name, "migrated") })
				}
			}).TableDef()
			tbl.latestSchemaVer = 1
			db := setup(t, schema)
			db.Write(func(tx *Tx) {
				Put(tx, &accessMember{ID: 42, Name: "original", Private: []byte{1, 2, 3}, Details: &accessDetails{Count: 7, Private: "opaque"}})
				c := tx.TableScan(tbl, FullScan())
				c.Next()
				before := bytes.Clone(c.RawRow())
				notified := false
				if mode == "migration" {
					tbl.latestSchemaVer = 2
				}
				if mode == "notification" {
					tx.OnChange(map[*TableDef]ChangeFlags{tbl: ChangeFlagNotify | ChangeFlagIncludeNewGoStruct}, func(_ *Tx, ch *Change) {
						notified = true
						v := ch.NewGoStruct().(*accessMember)
						if v.Name != "original" || v.ID != 42 || v.Details.Private != "opaque" || !bytes.Equal(v.Private, []byte{1, 2, 3}) {
							t.Fatal("bad notification")
						}
					})
				}
				reindexing = true
				tx.Reindex(tbl, nil)
				reindexing = false
				want := "original"
				if mode == "migration" {
					want = "migrated"
				}
				if !tx.IndexScan(idx, ExactScan(want)).Next() {
					t.Fatal("missing rebuilt entry")
				}
				c = tx.TableScan(tbl, FullScan())
				c.Next()
				if mode != "migration" && !bytes.Equal(before, c.RawRow()) {
					t.Fatal("reindex changed stored bytes")
				}
				got := Get[accessMember](tx, uint64(42))
				if got.Name != want || !bytes.Equal(got.Private, []byte{1, 2, 3}) || got.Details.Private != "opaque" {
					t.Fatalf("lost projected-out data: %+v", got)
				}
				if mode == "notification" && !notified {
					t.Fatal("missing notification")
				}
			})
		})
	}
}
