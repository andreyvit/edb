package edbindexing_test

import (
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/edbindexing"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type objectID uint64
type label string
type item struct {
	ID   uint64    `msgpack:"-" kvo:"-"`
	Ref  objectID  `msgpack:"r" kvo:"ref"`
	Name label     `msgpack:"n" kvo:"name"`
	Tags []label   `msgpack:"t" kvo:"tags"`
	When time.Time `msgpack:"w" kvo:"when"`
}

var schema = kvo.NewSchema()
var ref = kvo.NewUnsignedProp[objectID](schema, 1, "ref", kvo.NewIntType[objectID]("object_id", nil))
var name = kvo.NewStringProp[label](schema, 2, "name", kvo.TMachineString)
var tags = kvo.NewListProp(schema, 3, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[label]())
var when = kvo.NewTimeProp(schema, 4, "when")
var model = kvo.NewModel(schema, kvo.NewEntityType(schema, "item", reflect.TypeFor[item]()), func(b *kvo.ModelBuilder) {
	b.Prop(ref)
	b.Prop(name)
	b.Prop(tags)
	b.Prop(when)
})
var binding = kvo.Bind(reflect.TypeFor[item](), model)

func records(row *item) map[string]kvo.Record {
	v := reflect.ValueOf(row)
	return map[string]kvo.Record{
		"native":      binding.Record(v),
		"messagepack": binding.MessagePack(msgpack.Encode(nil, v)),
		"fast2":       binding.Encoded(row2.Trusted(binding.Encode(nil, v.Elem(), row2.Fast2))),
		"compact2":    binding.Encoded(row2.Trusted(binding.Encode(nil, v.Elem(), row2.Compact2))),
	}
}

func check[T comparable](t *testing.T, r kvo.Record, f func(*edb.Tx, values.Value, kvo.Record, func(T)), want ...T) {
	t.Helper()
	got := make([]T, 0, 16)
	yield := func(v T) { got = append(got, v) }
	run := func() { got = got[:0]; f(nil, values.Value{}, r, yield) }
	run()
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if n := testing.AllocsPerRun(100, run); n != 0 {
		t.Fatalf("%g callback allocations", n)
	}
}

func TestHelpersAcrossRepresentations(t *testing.T) {
	for _, row := range []*item{
		{Ref: 42, Name: "example", Tags: []label{"vip", "", "vip"}, When: time.Unix(1730000000, 123456789).UTC()},
		{When: time.Time{}.In(time.FixedZone("zero", 3600))},
	} {
		for format, r := range records(row) {
			t.Run(format, func(t *testing.T) {
				check(t, r, edbindexing.ByField(ref), row.Ref)
				check(t, r, edbindexing.ByField(name), row.Name)
				if row.Ref == 0 {
					check(t, r, edbindexing.ByFieldIfNonZero(ref))
				} else {
					check(t, r, edbindexing.ByFieldIfNonZero(ref), row.Ref)
				}
				if row.Name == "" {
					check(t, r, edbindexing.ByFieldIfNonZero(name))
				} else {
					check(t, r, edbindexing.ByFieldIfNonZero(name), row.Name)
				}
				check(t, r, edbindexing.ByScalarList(tags), row.Tags...)
				if row.When.IsZero() {
					check(t, r, edbindexing.ByTimeFieldIfNonZero(when))
				} else {
					check(t, r, edbindexing.ByTimeFieldIfNonZero(when), row.When.UTC())
				}
			})
		}
	}
}

func TestHelperIndexUpdates(t *testing.T) {
	for _, format := range []edb.StorageFormat{edb.MessagePack, edb.Fast2, edb.Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			scm := new(edb.Schema)
			var byRef, byNonzeroRef, byTag *edb.IndexDef
			table := edb.DefineTable(scm, "items", model, func(b *edb.TableBuilder[uint64]) {
				if format != edb.MessagePack {
					b.SetRecordStorage(edb.RecordStorage{Format: format})
				}
				byRef = b.AddIndex("ref", edbindexing.ByField(ref)).IndexDef()
				byNonzeroRef = b.AddIndex("nonzero_ref", edbindexing.ByFieldIfNonZero(ref)).IndexDef()
				byTag = b.AddIndex("tag", edbindexing.ByScalarList(tags)).IndexDef()
			}).TableDef()
			db, err := edb.Open(filepath.Join(t.TempDir(), "db"), scm, edb.Options{IsTesting: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
				edb.Put(tx, &item{ID: 1, Ref: 42, Tags: []label{"vip", "", "vip"}})
				assertKeys := func(idx *edb.IndexDef, want ...any) {
					t.Helper()
					var got []any
					for c := tx.IndexScan(idx, edb.FullScan()); c.Next(); {
						if c.UintKey() != 1 {
							t.Fatal("wrong primary key")
						}
						got = append(got, c.IndexKey())
					}
					if !slices.Equal(got, want) {
						t.Fatalf("%s: got %v, want %v", idx.ShortName(), got, want)
					}
				}
				assertKeys(byRef, objectID(42))
				assertKeys(byNonzeroRef, objectID(42))
				assertKeys(byTag, label(""), label("vip"))
				edit := tx.OpenRecordEdit(table, uint64(1))
				edit.Editor().Set(ref, objectID(0))
				tags.SetSlice(edit.Editor(), nil)
				tx.SaveRecord(edit)
				for range 2 {
					assertKeys(byRef, objectID(0))
					assertKeys(byNonzeroRef)
					assertKeys(byTag)
					tx.Reindex(table, nil)
				}
				tx.DeleteByKey(table, uint64(1))
				assertKeys(byRef)
				assertKeys(byNonzeroRef)
				assertKeys(byTag)
			})
		})
	}
}

var sink objectID
var labelSink label

func BenchmarkHelpers(b *testing.B) {
	row := &item{Ref: 42, Tags: []label{"one", "two", "three"}}
	inlineField := func(_ *edb.Tx, _ values.Value, r kvo.Record, yield func(objectID)) { yield(r.Get(ref)) }
	inlineNonzero := func(_ *edb.Tx, _ values.Value, r kvo.Record, yield func(objectID)) {
		if v := r.Get(ref); v != 0 {
			yield(v)
		}
	}
	inlineList := func(_ *edb.Tx, _ values.Value, r kvo.Record, yield func(label)) {
		for v := range r.Get(tags).Iter() {
			yield(v)
		}
	}
	for format, r := range records(row) {
		b.Run(format, func(b *testing.B) {
			for _, tc := range []struct {
				name string
				f    func(*edb.Tx, values.Value, kvo.Record, func(objectID))
			}{
				{"field/inline", inlineField}, {"field/helper", edbindexing.ByField(ref)},
				{"nonzero/inline", inlineNonzero}, {"nonzero/helper", edbindexing.ByFieldIfNonZero(ref)},
			} {
				b.Run(tc.name, func(b *testing.B) {
					emit := func(v objectID) { sink = v }
					tc.f(nil, values.Value{}, r, emit)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						tc.f(nil, values.Value{}, r, emit)
					}
				})
			}
			for _, tc := range []struct {
				name string
				f    func(*edb.Tx, values.Value, kvo.Record, func(label))
			}{{"list/inline", inlineList}, {"list/helper", edbindexing.ByScalarList(tags)}} {
				b.Run(tc.name, func(b *testing.B) {
					emit := func(v label) { labelSink = v }
					tc.f(nil, values.Value{}, r, emit)
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						tc.f(nil, values.Value{}, r, emit)
					}
				})
			}
		})
	}
}
