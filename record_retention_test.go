package edb

import (
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

type retentionRow struct {
	Key  string   `msgpack:"-" kvo:"-"`
	Name string   `kvo:"name"`
	Tags []string `kvo:"tags"`
}

// Only owned outputs are inspected beyond the transaction. Churn encourages
// Bolt to reuse freed pages; closing the DB then unmaps even unreused pages.
func TestRecordRetentionAfterPageReuseAndClose(t *testing.T) {
	s := kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	tags := kvo.NewListProp(s, 2, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	m := kvo.NewModel(s, kvo.NewEntityType(s, "retention", reflect.TypeFor[retentionRow]()), func(b *kvo.ModelBuilder) { b.Prop(name); b.Prop(tags) })
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			schema := new(Schema)
			tbl := DefineTable(schema, "rows", m, func(b *TableBuilder[string]) {
				if format != MessagePack {
					b.SetRecordStorage(RecordStorage{Format: format})
				}
			}).TableDef()
			db := setup(t, schema)
			db.reuseTxBuffers = true
			want := &retentionRow{"original-key", strings.Repeat("original", 1024), []string{strings.Repeat("first", 512), "second"}}
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, want) })
			var owned, cloned, decoded kvo.Record
			var native *retentionRow
			var ownedTags []string
			var key, ownedName string
			var bookmark ScanBookmark
			db.Read(func(tx *Tx) {
				c := tx.TableScan(tbl, FullScan())
				if !c.Next() {
					t.Fatal("missing row")
				}
				r := c.Record()
				owned, cloned, decoded = r.Own(), r.Clone(), r.Fastest()
				ownedTags = r.Get(tags).OwnedSlice()
				ownedName = r.GetOwnedString(name.Code()).AsString()
				key = c.Key().(string)
				bookmark = c.Bookmark()
				native = Get[retentionRow](tx, key)
			})
			for round := 0; round < 8; round++ {
				db.Write(TxMetadata{}, func(tx *Tx) {
					for i := 0; i < 100; i++ {
						Put(tx, &retentionRow{fmt.Sprint("churn/", i), strings.Repeat(fmt.Sprint(round), 4096), []string{"changed"}})
					}
					Put(tx, &retentionRow{want.Key, "replaced", []string{"changed"}})
				})
				db.Write(TxMetadata{}, func(tx *Tx) {
					for i := 0; i < 100; i++ {
						DeleteByKey[retentionRow](tx, fmt.Sprint("churn/", i))
					}
				})
			}
			db.Close()
			runtime.GC()
			for _, r := range []kvo.Record{owned, cloned, decoded} {
				if r.Get(name) != want.Name || !slices.Equal(r.Get(tags).Slice(), want.Tags) {
					t.Fatal("retained record changed")
				}
			}
			if !reflect.DeepEqual(native, want) || !slices.Equal(ownedTags, want.Tags) || ownedName != want.Name || key != want.Key || bookmark.position != string(tbl.EncodeKey(want.Key)) {
				t.Fatal("retained native/key/collection changed")
			}
		})
	}
}
