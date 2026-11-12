package edb

import (
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

type regressionChild struct {
	Count int64 `kvo:"count"`
}
type regressionRow struct {
	ID       uint64           `kvo:"-" msgpack:"-"`
	Name     string           `kvo:"name"`
	Count    int64            `kvo:"count"`
	Counters map[string]int64 `kvo:"counters"`
	Tags     []string         `kvo:"tags"`
	Child    *regressionChild `kvo:"child"`
}

// Exercise mixed old/new writes with an independent Go-state oracle. In
// particular, repeated saves, cached read cursors and attached collection
// editors must survive other writes, page splits, rollbacks and reindexing.
func TestRecordMixedWriteRegression(t *testing.T) {
	s := kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	count := kvo.NewSignedProp[int64](s, 2, "count", kvo.TInt64)
	counters := kvo.NewMapProp(s, 3, "counters", kvo.MapOf(kvo.TMachineString, kvo.TInt64), values.StringCodec[string](), values.SignedCodec[int64]())
	tags := kvo.NewListProp(s, 4, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	ct := kvo.NewEntityType(s, "child", reflect.TypeFor[regressionChild]())
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(count) })
	child := kvo.NewRecordProp(s, 5, "child", ct)
	model := kvo.NewModel(s, kvo.NewEntityType(s, "row", reflect.TypeFor[regressionRow]()), func(b *kvo.ModelBuilder) {
		b.Prop(name)
		b.Prop(count)
		b.Prop(counters, kvo.KeepDictionaryZeroValues)
		b.Prop(tags)
		b.Prop(child)
	})
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			schema := new(Schema)
			var byName, byCount *IndexDef
			table := DefineTable(schema, "rows", model, func(b *TableBuilder[uint64]) {
				if format != MessagePack {
					b.SetRecordStorage(RecordStorage{Format: format})
				}
				byName = b.AddIndex[string]("name", nil).IndexDef()
				byCount = b.AddIndex[int64]("count", nil).IndexDef()
				b.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) {
					ib.Add(byName, r.Get(name))
					ib.Add(byCount, r.Get(count))
					ib.Add(byCount, r.Get(count))
				})
			}).TableDef()
			db := setup(t, schema)
			db.reuseTxBuffers = true
			records := table.Typed[uint64]()
			rng := rand.New(rand.NewPCG(17, 93))
			want := map[uint64]*regressionRow{}
			verify := func(tx *Tx, want map[uint64]*regressionRow) {
				t.Helper()
				AssertIntegrity(t, tx)
				got := map[uint64]*regressionRow{}
				for c := FullTableScan[regressionRow](tx); c.Next(); {
					r := c.Row()
					got[r.ID] = r
					rr := records.Get(tx, r.ID)
					if rr.Get(name) != r.Name || rr.Get(count) != r.Count || !maps.Equal(maps.Collect(rr.Get(counters).Iter()), r.Counters) || !slices.Equal(rr.Get(tags).Slice(), r.Tags) {
						t.Fatalf("record/native mismatch for %d", r.ID)
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("persisted state differs from oracle:\ngot: %#v\nwant: %#v", got, want)
				}
			}
			for batch := 0; batch < 40; batch++ {
				next := make(map[uint64]*regressionRow, len(want))
				for id, r := range want {
					v := *r
					v.Counters = maps.Clone(r.Counters)
					v.Tags = slices.Clone(r.Tags)
					if r.Child != nil {
						c := *r.Child
						v.Child = &c
					}
					next[id] = &v
				}
				rollback := errors.New("planned rollback")
				err := db.Tx(true, func(tx *Tx) error {
					for step := 0; step < 25; step++ {
						id := uint64(rng.IntN(96) + 1)
						// Warm the reusable lookup cursor before mutating this bucket.
						_ = records.Get(tx, id)
						if rng.IntN(5) == 0 {
							records.Delete(tx, id)
							delete(next, id)
							continue
						}
						v := int64(rng.IntN(9) - 4)
						label := fmt.Sprint("group", rng.IntN(7))
						if rng.IntN(3) == 0 {
							r := &regressionRow{ID: id, Name: label, Count: v, Counters: map[string]int64{"zero": 0, "value": v}, Tags: []string{"old"}, Child: &regressionChild{v}}
							Put(tx, r)
							next[id] = r
						} else {
							e := records.EditOrCreate(tx, id)
							r := next[id]
							if r == nil {
								r = &regressionRow{ID: id}
								next[id] = r
							}
							e.Set(name, label)
							r.Name = label
							e.Set(count, v)
							r.Count = v
							e.Map(counters).Set("value", v)
							e.Map(counters).Set("zero", 0)
							if r.Counters == nil {
								r.Counters = map[string]int64{}
							}
							r.Counters["value"] = v
							r.Counters["zero"] = 0
							e.List(tags).AddUnique("vip")
							if !slices.Contains(r.Tags, "vip") {
								r.Tags = append(r.Tags, "vip")
							}
							e.EditOrCreate(child).Set(count, v+1)
							r.Child = &regressionChild{v + 1}
							records.Save(tx, e)
							if records.Save(tx, e) {
								t.Fatal("repeated save changed persisted data")
							}
							// A new mutation through an existing attached editor must save.
							e.Map(counters).Delete("value")
							delete(r.Counters, "value")
							records.Save(tx, e)
						}
						if got := Get[regressionRow](tx, id); !reflect.DeepEqual(got, next[id]) {
							t.Fatalf("batch %d step %d: got %+v, want %+v", batch, step, got, next[id])
						}
					}
					if batch%5 == 0 {
						tx.Reindex(table, nil)
					}
					verify(tx, next)
					if batch%7 == 0 {
						return rollback
					}
					return nil
				})
				if err == nil {
					want = next
				} else if !errors.Is(err, rollback) {
					t.Fatal(err)
				}
				db.Read(func(tx *Tx) { verify(tx, want) })
			}
		})
	}
}
