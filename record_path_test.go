package edb

import (
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type pathRow struct {
	ID      uint64           `msgpack:"-" kvo:"-"`
	Name    string           `msgpack:"name" kvo:"name"`
	Count   int64            `msgpack:"count" kvo:"count"`
	Tags    []string         `msgpack:"tags" kvo:"tags"`
	Labels  map[string]int64 `msgpack:"labels" kvo:"labels"`
	Payload string           `msgpack:"payload" kvo:"payload"`
}

type pathProps struct {
	name    kvo.StringPropRef[string]
	count   kvo.SignedPropRef[int64]
	tags    *kvo.ListPropRef[string]
	labels  *kvo.MapPropRef[string, int64]
	payload kvo.StringPropRef[string]
}

func pathTable(t testing.TB, format StorageFormat) (*DB, *Table[uint64], *Index[string], pathProps) {
	t.Helper()
	s := kvo.NewSchema()
	p := pathProps{
		kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString),
		kvo.NewSignedProp[int64](s, 2, "count", kvo.TInt64),
		kvo.NewListProp(s, 3, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]()),
		kvo.NewMapProp(s, 4, "labels", kvo.MapOf(kvo.TMachineString, kvo.TInt64), values.StringCodec[string](), values.SignedCodec[int64]()),
		kvo.NewStringProp[string](s, 5, "payload", kvo.TMachineString),
	}
	m := kvo.NewModel(s, kvo.NewEntityType(s, "path", reflect.TypeFor[pathRow]()), func(b *kvo.ModelBuilder) {
		b.Prop(p.name)
		b.Prop(p.count)
		b.Prop(p.tags)
		b.Prop(p.labels)
		b.Prop(p.payload)
	})
	sc := new(Schema)
	var idx *Index[string]
	table := DefineTable(sc, "paths", m, func(b *TableBuilder[uint64]) {
		if format != MessagePack {
			b.SetRecordStorage(RecordStorage{Format: format})
		}
		idx = b.AddIndex("name", func(_ *Tx, _ values.Value, r kvo.Record, yield func(string)) { yield(r.Get(p.name)) })
	})
	return setup(t, sc), table, idx, p
}

// This exercises the database path, including the per-index callback and record
// notifications. An invalid native codec makes accidental whole-row MessagePack
// serialization fail, rather than allowing it to hide behind allocation counts.
func TestRecordPathStaysEncoded(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, table, idx, p := pathTable(t, format)
			table.valueEnc = encodingMethod(-1)
			check := func(r kvo.Record) {
				if r.Missing() {
					return
				}
				if format == MessagePack {
					if r.MessagePack() == nil {
						t.Fatal("record became a Go struct")
					}
				} else if r.Snapshot().Missing() {
					t.Fatal("record became a Go struct")
				}
			}
			db.Write(TxMetadata{}, func(tx *Tx) {
				notifications := 0
				tx.OnChange(map[*TableDef]ChangeFlags{table.TableDef(): ChangeFlagNotify | ChangeFlagIncludeOldRecord | ChangeFlagIncludeNewRecord}, func(_ *Tx, c *Change) {
					notifications++
					check(c.OldRecord())
					check(c.NewRecord())
				})
				e := table.New(tx, 1000)
				check(e.Record())
				e.Set(p.name, "before")
				e.Set(p.count, int64(7))
				e.List(p.tags).AddUnique("vip")
				e.Map(p.labels).Set("earned", 42)
				if !table.Save(tx, e) || table.Save(tx, e) {
					t.Fatal("insert/no-op result")
				}
				if n := testing.AllocsPerRun(100, func() { table.Save(tx, e) }); n != 0 {
					t.Fatalf("unchanged save: %g allocations", n)
				}
				r := table.Get(tx, 1000)
				check(r)
				if !pathHasTag(r.Get(p.tags), "vip") {
					t.Fatal("list value lost")
				}
				if v, ok := r.Get(p.labels).Lookup("earned"); !ok || v != 42 {
					t.Fatal("map value lost")
				}
				e = table.Edit(tx, 1000)
				e.Set(p.name, "after")
				if !table.Save(tx, e) {
					t.Fatal("unchanged update")
				}
				if _, ok := table.LookupKey(tx, idx, "before"); ok {
					t.Fatal("stale index")
				}
				if key, ok := table.LookupKey(tx, idx, "after"); !ok || key != 1000 {
					t.Fatal("missing index")
				}
				tx.Reindex(table, nil)
				check(table.Get(tx, 1000))
				if notifications != 3 {
					t.Fatalf("notifications = %d", notifications)
				}
				empty := table.New(tx, 1001)
				if !table.Save(tx, empty) || table.Get(tx, 1001).Missing() {
					t.Fatal("empty insert was mistaken for an unchanged save")
				}
				if table.Save(tx, empty) || notifications != 4 {
					t.Fatal("empty save notified twice")
				}
			})
		})
	}
}

func TestRecordPathReadAllocations(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, table, _, p := pathTable(t, format)
			db.Write(TxMetadata{}, func(tx *Tx) {
				Put(tx, &pathRow{ID: 1000, Name: "Ada", Count: 7, Tags: []string{"vip", "local"}, Labels: map[string]int64{"earned": 42}, Payload: strings.Repeat("x", 512)})
			})
			db.Read(func(tx *Tx) {
				if n := testing.AllocsPerRun(100, func() {
					r := table.Get(tx, 1000)
					if r.Get(p.name) != "Ada" || r.Get(p.count) != 7 || !pathHasTag(r.Get(p.tags), "vip") {
						t.Fatal("scalar/list read")
					}
					if v, ok := r.Get(p.labels).Lookup("earned"); !ok || v != 42 {
						t.Fatal("map read")
					}
				}); n != 0 {
					t.Fatalf("lookup and borrowed fields: %g allocations", n)
				}
			})
		})
	}
}

// One warmed transaction; includes key handling, editor creation, index updates,
// envelopes and bbolt writes, excludes transaction begin/commit and fsync.
func BenchmarkRecordPath(b *testing.B) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		for _, op := range []string{"Read", "OpenSaveUnchanged", "EditSave", "NewSaveDelete"} {
			b.Run(format.String()+"/"+op, func(b *testing.B) {
				db, table, _, p := pathTable(b, format)
				db.Write(TxMetadata{}, func(tx *Tx) {
					Put(tx, &pathRow{ID: 1000, Name: "Ada", Count: 7, Tags: []string{"vip", "local"}, Labels: map[string]int64{"earned": 42}, Payload: strings.Repeat("x", 512)})
				})
				run := func(tx *Tx) {
					table.Get(tx, 1000)
					i := int64(0)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						switch op {
						case "Read":
							r := table.Get(tx, 1000)
							if r.Get(p.count) != 7 || !pathHasTag(r.Get(p.tags), "vip") {
								b.Fatal("read")
							}
							if v, ok := r.Get(p.labels).Lookup("earned"); !ok || v != 42 {
								b.Fatal("map")
							}
						case "OpenSaveUnchanged":
							e := table.Edit(tx, 1000)
							table.Save(tx, e)
						case "EditSave":
							e := table.Edit(tx, 1000)
							e.Set(p.count, i+10)
							table.Save(tx, e)
						case "NewSaveDelete":
							e := table.New(tx, 1001)
							e.Set(p.name, "Grace")
							e.Set(p.count, i+10)
							table.Save(tx, e)
							tx.DeleteByKey(table, uint64(1001))
						}
						i++
					}
					b.StopTimer()
				}
				if op == "Read" {
					db.Read(run)
				} else {
					db.Write(TxMetadata{}, run)
				}
			})
		}
	}
}

func pathHasTag(tags kvo.List[string], want string) bool {
	c := tags.Cursor()
	for {
		v, ok := c.Next()
		if !ok {
			break
		}
		if v == want {
			return true
		}
	}
	return false
}

func TestRecordSaveDetectsDirectCodecEdits(t *testing.T) {
	for _, format := range []StorageFormat{Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, table, _, p := pathTable(t, format)
			db.Write(TxMetadata{}, func(tx *Tx) {
				e := table.New(tx, 1000)
				e.Set(p.count, int64(1))
				table.Save(tx, e)
				e = table.Edit(tx, 1000)
				if table.Save(tx, e) {
					t.Fatal("unchanged save")
				}
				e.Editor().Packed().Set([]row2.Step{{Key: p.count.Code()}}, values.Int(2), row2.Signed)
				if !table.Save(tx, e) || table.Get(tx, 1000).Get(p.count) != 2 {
					t.Fatal("direct codec edit was lost")
				}
			})
		})
	}
}
