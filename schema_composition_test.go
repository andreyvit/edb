package edb

import (
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

func TestSchemaComposition(t *testing.T) {
	source := new(Schema)
	var byName *IndexDef
	users := DefineTable(source, "users", testIndexAccess[User](indexName), func(b *TableBuilder[ID]) {
		byName = b.AddIndex[string]("name", nil).IndexDef()
		b.SetIndexer(func(key ID, u kvo.Record, ib *IndexBuilder) { ib.Add(byName, u.Get(indexName)) })
	}).TableDef()
	raw := DefineRawTable(source, "raw", nil)
	settings := AddKVMap(source, "settings")
	version := AddSingletonKey[int](settings, "version")
	sourceDB := setup(t, source)

	combined := new(Schema)
	DefineTable(combined, "posts", testTableModel[Post](), func(*TableBuilder[string]) {}).TableDef()
	combined.Include(source)
	if combined.KVTableNamed("RAW") != raw || combined.TableNamed("USERS") != users || combined.TableByRowType(reflect.TypeFor[*User]()) != users {
		t.Fatal("composition did not preserve table identities and lookups")
	}
	outer := new(Schema)
	DefineTable(outer, "widgets", testTableModel[Widget](), func(*TableBuilder[AB]) {}).TableDef()
	outer.Include(combined)

	for name, db := range map[string]*DB{"source": sourceDB, "combined": setup(t, combined), "outer": setup(t, outer)} {
		t.Run(name, func(t *testing.T) {
			db.Write(func(tx *Tx) {
				Put(tx, &User{ID: 1, Name: name})
				tx.KVPutRaw(raw, []byte("key"), []byte(name))
				v := 7
				SPut(tx, version, &v)
			})
			db.Read(func(tx *Tx) {
				rows := All(IndexScan[User](tx, byName, ExactScan(name)))
				if len(rows) != 1 || rows[0].ID != 1 || rows[0].Name != name {
					t.Fatalf("composed index lookup = %v", rows)
				}
				var v int
				if !SGet(tx, version, &v) || v != 7 || string(tx.KVGetRaw(raw, []byte("key"))) != name {
					t.Fatal("included KV table or singleton did not round-trip")
				}
			})
			db.Write(func(tx *Tx) {
				tx.Reindex(users, byName)
				DeleteByKey[User](tx, ID(1))
			})
			db.Read(func(tx *Tx) {
				if rows := All(IndexScan[User](tx, byName, ExactScan(name))); len(rows) != 0 {
					t.Fatal("index retained deleted row")
				}
			})
		})
	}
}

func TestSchemaRegistrationNamespace(t *testing.T) {
	declarations := []struct {
		name string
		add  func(*Schema, string)
	}{
		{"table", func(s *Schema, name string) {
			DefineTable(s, name, testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
		}},
		{"KV table", func(s *Schema, name string) { DefineRawTable(s, name, nil) }},
		{"singleton map", func(s *Schema, name string) { AddKVMap(s, name) }},
	}
	for _, first := range declarations {
		for _, second := range declarations {
			for _, include := range []bool{false, true} {
				mode := "direct"
				if include {
					mode = "include"
				}
				t.Run(first.name+"/"+second.name+"/"+mode, func(t *testing.T) {
					s := new(Schema)
					first.add(s, "items")
					before := [3]int{len(s.tables), len(s.kvtables), len(s.maps)}
					assertRegistrationPanic(t, "already defined", func() {
						if !include {
							second.add(s, "ITEMS")
							return
						}
						peer := new(Schema)
						DefineTable(peer, "unrelated", testTableModel[Post](), func(*TableBuilder[string]) {}).TableDef()
						second.add(peer, "ITEMS")
						s.Include(peer)
					})
					if got := [3]int{len(s.tables), len(s.kvtables), len(s.maps)}; got != before || s.TableNamed("unrelated") != nil {
						t.Fatal("rejected registration partially changed schema", got, before)
					}
				})
			}
		}
	}
}

func TestSchemaCompositionRowTypeCollision(t *testing.T) {
	s, peer := new(Schema), new(Schema)
	original := DefineTable(s, "users", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
	DefineTable(peer, "posts", testTableModel[Post](), func(*TableBuilder[string]) {}).TableDef()
	DefineTable(peer, "other_users", testTableModel[User](), func(*TableBuilder[ID]) {}).TableDef()
	assertRegistrationPanic(t, "row type", func() { s.Include(peer) })
	if len(s.Tables()) != 1 || s.TableByRowType(reflect.TypeFor[User]()) != original || s.TableNamed("posts") != nil {
		t.Fatal("row type collision partially changed schema")
	}
	for _, name := range []string{"users", strings.ToUpper("users")} {
		if s.TableNamed(name) != original {
			t.Fatal("lost original table", name)
		}
	}
}
