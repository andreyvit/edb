package edb

import (
	"bytes"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func TestPerIndexCallbacks(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			calls := 0
			var idx, lengths *IndexDef
			schema, table := changeRecordSchema(format, func(b *TableBuilder[string]) {
				idx = b.AddIndex[string]("names", func(tx *Tx, pk values.Value, r kvo.Record, yield func(string)) {
					if tx == nil || pk.AsString() != "key" {
						t.Fatal("wrong callback context")
					}
					calls++
					if r.GetValue(1).AsString() == "skip" {
						return
					}
					yield(r.GetValue(1).AsString())
					yield("all")
				}).IndexDef()
				lengths = b.AddIndex[uint64]("lengths", func(_ *Tx, pk values.Value, r kvo.Record, yield func(uint64)) {
					if IndexPrimaryKey[string](pk) != "key" {
						t.Fatal("wrong primary key in second index")
					}
					yield(uint64(len(r.GetValue(1).AsString())))
				}).IndexDef()
			})
			db := setup(t, schema)
			db.Write(func(tx *Tx) {
				checkLength := func(want uint64) {
					t.Helper()
					c := tx.IndexScan(lengths, FullScan())
					if !c.Next() || c.Key() != "key" || c.IndexKey() != want || c.Next() {
						t.Fatalf("second index must contain only length %d for key", want)
					}
				}
				Put(tx, &changeRecordRow{ID: "key", Name: "old"})
				if calls != 1 || !tx.IndexScan(idx, ExactScan("old")).Next() {
					t.Fatal(calls)
				}
				checkLength(3)
				e := tx.OpenRecordEdit(table, "key")
				e.Editor().SetValue(1, values.String("updated"))
				tx.SaveRecord(e)
				if calls != 2 || tx.IndexScan(idx, ExactScan("old")).Next() || !tx.IndexScan(idx, ExactScan("updated")).Next() {
					t.Fatal(calls)
				}
				checkLength(7)
				tx.Reindex(table, nil)
				if calls != 3 || !tx.IndexScan(idx, ExactScan("all")).Next() {
					t.Fatal(calls)
				}
				checkLength(7)
				Put(tx, &changeRecordRow{ID: "key", Name: "skip"})
				checkLength(4)
				if tx.IndexScan(idx, FullScan()).Next() {
					t.Fatal("stale entries")
				}
				Put(tx, &changeRecordRow{ID: "key", Name: "last"})
				tx.DeleteByKey(table, "key")
				if tx.IndexScan(idx, FullScan()).Next() {
					t.Fatal("delete retained entries")
				}
				if tx.IndexScan(lengths, FullScan()).Next() {
					t.Fatal("delete retained second index entries")
				}
				AssertIntegrity(t, tx)
			})
		})
	}
}

var indexBenchmarkValue values.Value
var indexBenchmarkKey AB

func BenchmarkIndexPrimaryKey(b *testing.B) {
	for _, tc := range []struct {
		name string
		key  any
	}{{"uint", uint64(1000)}, {"string", "customer-key"}, {"compound", AB{1000, 3}}} {
		b.Run(tc.name, func(b *testing.B) {
			key := reflect.New(reflect.TypeOf(tc.key)).Elem()
			key.Set(reflect.ValueOf(tc.key))
			raw := flatEncodingOf(key.Type()).encode(nil, key)
			table := &TableDef{keyType: key.Type(), keyEnc: flatEncodingOf(key.Type())}
			b.Run("cell", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					indexBenchmarkValue = indexPrimaryKey(key)
				}
			})
			if tc.name == "compound" {
				cell := indexPrimaryKey(key)
				b.Run("access", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						indexBenchmarkKey = IndexPrimaryKey[AB](cell)
					}
				})
				b.Run("typed-decode", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						indexBenchmarkKey = decodeTypedKey[AB](table, raw)
					}
				})
			}
		})
	}
}

func BenchmarkIndexCallback(b *testing.B) {
	for _, perIndex := range []bool{false, true} {
		name := "table"
		if perIndex {
			name = "index"
		}
		b.Run(name, func(b *testing.B) {
			schema := new(Schema)
			var cb func(*Tx, values.Value, kvo.Record, func(uint64))
			if perIndex {
				cb = func(_ *Tx, pk values.Value, r kvo.Record, yield func(uint64)) { yield(r.Get(indexGroupNumber)) }
			}
			var idx *IndexDef
			table := DefineTable(schema, "rows", testIndexAccess[benchmarkRow](indexGroupNumber), func(tb *TableBuilder[uint64]) {
				idx = tb.AddIndex[uint64]("group", cb).IndexDef()
				if !perIndex {
					tb.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.Get(indexGroupNumber)) })
				}
			}).TableDef()
			db := setup(b, schema)
			row := reflect.ValueOf(&benchmarkRow{Key: 1000, Group: 1000})
			r := table.nativeRecord(row)
			key := table.RowKeyVal(row)
			raw := table.EncodeKey(uint64(1000))
			ts := db.tableState(table)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				ib := makeIndexBuilder(ts, raw)
				if perIndex {
					table.runIndexers(nil, key, r, ib)
				} else {
					table.recordIndexer(r, ib)
				}
				ib.release()
			}
		})
	}
}

func TestIndexCallbackReentrancyAndPanic(t *testing.T) {
	var idx, sibling *IndexDef
	var ts *tableState
	var key []byte
	schema, table := changeRecordSchema(MessagePack, func(b *TableBuilder[string]) {
		idx = b.AddIndex[uint64]("nested", func(tx *Tx, pk values.Value, r kvo.Record, yield func(uint64)) {
			if pk.R == 3 {
				yield(999)
				panic("index failed")
			}
			if pk.R == 1 {
				inner := makeIndexBuilder(ts, key)
				defer inner.release()
				idx.table.runIndexers(tx, reflect.ValueOf(uint64(2)), r, inner)
				if len(inner.rows) != 2 {
					t.Fatal(len(inner.rows))
				}
				if got := idx.DecodeKeyVal(inner.rows[0].KeyRaw).Uint(); got != 2 {
					t.Fatal(got)
				}
				if got := sibling.DecodeKeyVal(inner.rows[1].KeyRaw).String(); got != "2" {
					t.Fatal(got)
				}
			}
			yield(pk.R)
		}, IndexOptDeprecatedUnique).IndexDef()
		sibling = b.AddIndex[string]("sibling", func(_ *Tx, pk values.Value, _ kvo.Record, yield func(string)) {
			yield(fmt.Sprint(pk.R))
		}, IndexOptDeprecatedUnique).IndexDef()
	})
	key = table.EncodeKey("key")
	db := setup(t, schema)
	ts = db.tableState(table)
	invoke := func(n uint64) {
		b := makeIndexBuilder(ts, key)
		defer b.release()
		table.runIndexers(nil, reflect.ValueOf(n), kvo.Record{}, b)
		if len(b.rows) != 2 || idx.DecodeKeyVal(b.rows[0].KeyRaw).Uint() != n || sibling.DecodeKeyVal(b.rows[1].KeyRaw).String() != fmt.Sprint(n) {
			t.Error("nested emission leaked")
		}
	}
	assertRegistrationPanic(t, "index failed", func() { invoke(3) })
	invoke(1)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for range 100 {
				invoke(uint64(i + 10))
			}
		})
	}
	wg.Wait()
}

func TestPerIndexUpgradeChecks(t *testing.T) {
	for _, diverges := range []bool{false, true} {
		t.Run(fmt.Sprint(diverges), func(t *testing.T) {
			calls := 0
			var idx *IndexDef
			schema, table := changeRecordSchema(Fast2, func(b *TableBuilder[string]) {
				idx = b.AddIndex[string]("name", func(_ *Tx, _ values.Value, r kvo.Record, yield func(string)) {
					name := r.GetValue(1).AsString()
					calls++
					if diverges && calls == 3 {
						name = "different"
					}
					yield(name)
				}).IndexDef()
			})
			table.messagePackInserts = true
			db := setup(t, schema)
			db.Write(func(tx *Tx) {
				Put(tx, &changeRecordRow{ID: "key", Name: "original"})
				before := bytes.Clone(table.dataBucketIn(tx).Get(table.EncodeKey("key")))
				ok, err := tx.TryUpgradeRecord(table, "key")
				if diverges {
					if err == nil || ok {
						t.Fatal(ok, err)
					}
					if !bytes.Equal(before, table.dataBucketIn(tx).Get(table.EncodeKey("key"))) {
						t.Fatal("failed upgrade changed row")
					}
				} else if err != nil || !ok {
					t.Fatal(ok, err)
				}
				if !tx.IndexScan(idx, ExactScan("original")).Next() {
					t.Fatal("upgrade lost index")
				}
			})
		})
	}
}
