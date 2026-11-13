package edb

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

// Index components are concatenated with a length footer. Prefix-related
// strings and primary keys can interleave, so the expected membership must
// come from decoded values, not from another bounded scan.
func TestIndexRangesAgainstValueOracle(t *testing.T) {
	db, rows, _, group := typedTestTable[string](t, MessagePack)
	keys := []string{"a", "aa", "a\x00", "a\x00a", "b", "ba", "z", "\xff", "\xff\x00"}
	groups := []string{"", "a", "a\x00", "a\x00a", "aa", "b", "\xff"}
	db.Write(TxMetadata{}, func(tx *Tx) {
		for i, g := range groups {
			for j, key := range keys {
				rows.Put(tx, &typedTestRow[string]{fmt.Sprintf("%s:%d", key, i), fmt.Sprint(i, ":", j), g})
			}
		}
	})
	for _, lower := range groups {
		for _, upper := range groups {
			for flags := 0; flags < 8; flags++ {
				loInc, hiInc, reverse := flags&1 != 0, flags&2 != 0, flags&4 != 0
				t.Run(fmt.Sprintf("%q-%q/%d", lower, upper, flags), func(t *testing.T) {
					opt := RangeScan(lower, upper, loInc, hiInc)
					opt.Reverse = reverse
					var want []string
					db.Read(func(tx *Tx) {
						all := FullScan()
						all.Reverse = reverse
						for c := tx.IndexScan(group, all); c.Next(); {
							r, _ := c.Row()
							g := r.(*typedTestRow[string]).Group
							if (g > lower || loInc && g == lower) && (g < upper || hiInc && g == upper) {
								want = append(want, c.Key().(string))
							}
						}
						got := AllKeys[string](tx.IndexScan(group, opt))
						if !slices.Equal(got, want) {
							t.Fatalf("scan got %q, want %q", got, want)
						}
					})
					// Reopen after every three entries. Check ordering, membership,
					// and the complete index-key/primary-key continuation together.
					var got []string
					for chunk := range ReadChunks(db, 3, func(tx *Tx) RecordScan[string] {
						return RecordScan[string]{rows.Records().bindIndex[string](group).scan(tx, opt)}
					}) {
						for key := range chunk.Keys() {
							got = append(got, key)
						}
					}
					if !slices.Equal(got, want) {
						t.Fatalf("chunks got %q, want %q", got, want)
					}
					db.Read(func(tx *Tx) {
						var want []string
						lowPK, highPK := rows.Table().EncodeKey("b:1"), rows.Table().EncodeKey("z:2")
						all := FullScan()
						all.Reverse = reverse
						for c := tx.IndexScan(group, all); c.Next(); {
							r, _ := c.Row()
							g := r.(*typedTestRow[string]).Group
							lo, hi := bytes.Compare(c.RawKey(), lowPK), bytes.Compare(c.RawKey(), highPK)
							if (g > lower || g == lower && (lo > 0 || loInc && lo == 0)) && (g < upper || g == upper && (hi < 0 || hiInc && hi == 0)) {
								want = append(want, c.Key().(string))
							}
						}
						got := AllKeys[string](tx.IndexScan(group, opt.PrimaryKeyRange("b:1", "z:2", loInc, hiInc)))
						if !slices.Equal(got, want) {
							t.Fatalf("primary endpoints got %q, want %q", got, want)
						}
					})
				})
			}
		}
	}
}

func TestIndexComponentBoundaries(t *testing.T) {
	type pair struct{ A, B string }
	type row struct {
		ID   uint64
		Pair pair
	}
	schema := new(Schema)
	var idx *IndexDef

	s := kvo.NewSchema()
	a := kvo.NewStringProp[string](s, 1, "A", kvo.TMachineString)
	c := kvo.NewStringProp[string](s, 2, "B", kvo.TMachineString)
	pt := kvo.NewEntityType(s, "pair")
	kvo.NewModel(s, pt, func(b *kvo.ModelBuilder) { b.Prop(a); b.Prop(c) })
	pp := kvo.NewRecordProp(s, 3, "Pair", pt)
	model := kvo.NewModel(s, kvo.NewEntityType(s, "row"), func(b *kvo.ModelBuilder) { b.Prop(pp) })
	model.RegisterNativeType(reflect.TypeFor[row](), map[kvo.PropCode]string{3: "Pair"})
	pt.Model().RegisterNativeType(reflect.TypeFor[pair](), map[kvo.PropCode]string{1: "A", 2: "B"})
	DefineTable(schema, "rows", model, func(b *TableBuilder[uint64]) {
		idx = b.AddIndex[pair]("pair", nil).IndexDef()
		b.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) { p := r.Get(pp); ib.Add(idx, pair{p.Get(a), p.Get(c)}) })
	}).TableDef()
	db := setup(t, schema)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &row{1, pair{"a", "bc"}}, &row{2, pair{"ab", "c"}}, &row{3, pair{"a", "d"}}) })
	for _, reverse := range []bool{false, true} {
		db.Read(func(tx *Tx) {
			opt := ExactScan(pair{"a", "bc"})
			opt.Reverse = reverse
			if got := AllKeys[uint64](tx.IndexScan(idx, opt)); !slices.Equal(got, []uint64{1}) {
				t.Fatalf("exact components: %v", got)
			}
			want := []uint64{1, 3}
			if reverse {
				slices.Reverse(want)
			}
			if got := AllKeys[uint64](tx.IndexScan(idx, opt.Prefix(1))); !slices.Equal(got, want) {
				t.Fatalf("prefix components: %v", got)
			}
			opt = RangeScan(pair{"a", "bc"}, pair{"a", "bc"}, true, true).PrimaryKeyRange(uint64(3), nil, true, true)
			opt.Reverse = reverse
			if got := AllKeys[uint64](tx.IndexScan(idx, opt)); !slices.Equal(got, []uint64{2}) {
				t.Fatalf("equal payload with distinct components: %v", got)
			}
			opt = RangeScan(pair{"a", "bc"}, pair{"a", "bc"}, true, true).PrimaryKeyRange(nil, uint64(1), true, true)
			opt.Reverse = reverse
			want = []uint64{1, 2}
			if reverse {
				slices.Reverse(want)
			}
			if got := AllKeys[uint64](tx.IndexScan(idx, opt)); !slices.Equal(got, want) {
				t.Fatalf("equal payload upper endpoint: %v", got)
			}
		})
	}
}

func TestIndexExactStringBoundaries(t *testing.T) {
	db, rows, name, group := typedTestTable[string](t, MessagePack)
	db.Write(TxMetadata{}, func(tx *Tx) {
		rows.Put(tx, &typedTestRow[string]{"a", "a", "a"})
		rows.Put(tx, &typedTestRow[string]{"b", "a\x01", "aa"})
		rows.Put(tx, &typedTestRow[string]{"z", "a\x01x", "a"})
	})
	for _, reverse := range []bool{false, true} {
		db.Read(func(tx *Tx) {
			opt := ExactScan("a")
			opt.Reverse = reverse
			if got := AllKeys[string](tx.IndexScan(name, opt)); !slices.Equal(got, []string{"a"}) {
				t.Errorf("unique reverse=%v: got %q", reverse, got)
			}
			want := []string{"a", "z"}
			if reverse {
				slices.Reverse(want)
			}
			opt = opt.PrimaryKeyRange("a", "z", true, true)
			if got := AllKeys[string](tx.IndexScan(group, opt)); !slices.Equal(got, want) {
				t.Errorf("primary-key range reverse=%v: got %q, want %q", reverse, got, want)
			}
		})
	}
}
