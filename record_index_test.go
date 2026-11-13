package edb

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func TestRecordIndexAndCursorEdits(t *testing.T) {
	type row struct {
		ID   uint64 `edb:"-" msgpack:"-"`
		Rank uint8  `edb:"rank" msgpack:"r,omitempty"`
	}
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "row", reflect.TypeFor[row]())
	kvo.NewProp(s, 1, "rank", kvo.NewIntType[uint8]("rank", nil), nil)
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	for _, legacy := range []bool{false, true} {
		sc := new(Schema)
		var i *IndexDef
		indexed := 0
		index := func(key uint64, r kvo.Record, b *IndexBuilder) {
			indexed++
			if key != 1 && key != 2 {
				t.Fatalf("incorrect primary key %d", key)
			}
			b.Add(i, r.GetValue(1).AsUint())
		}
		tbl := DefineTable(sc, "rows", m, func(b *TableBuilder[uint64]) {
			b.SetRecordStorage(RecordStorage{Format: Fast2, MessagePackInserts: legacy})
			i = b.AddIndex[uint64]("rank", nil).IndexDef()
			b.SetIndexer(index)
		}).TableDef()
		db := setup(t, sc)
		db.Write(TxMetadata{}, func(tx *Tx) {
			Put(tx, &row{1, 4}, &row{2, 0})
			c := tx.TableScan(tbl, FullScan())
			if _, err := c.TryRecordEdit(); err == nil {
				t.Fatal("unpositioned edit")
			}
			c.Next()
			edit, err := c.TryRecordEdit()
			if err != nil {
				t.Fatal(err)
			}
			stale, err := c.TryRecordEdit()
			if err != nil {
				t.Fatal(err)
			}
			c.Next()
			if err := edit.Editor().TrySetValue(1, values.Uint(7)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.TrySaveRecord(edit); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.TrySaveRecord(stale); err == nil {
				t.Fatal("stale edit saved")
			}
			if indexed != 3 || ExactIndexScan[row](tx, i, uint64(4)).Next() || !ExactIndexScan[row](tx, i, uint64(7)).Next() {
				t.Fatal("index mismatch")
			}
			if !legacy {
				if _, err := tx.TryEditRecord(tbl, uint64(1), func(e *kvo.Editor) error { return e.TrySetValue(1, values.Uint(256)) }); err == nil {
					t.Fatal("saved invalid uint8")
				}
				if indexed != 3 || Get[row](tx, uint64(1)).Rank != 7 {
					t.Fatal("invalid edit reached index")
				}
				var before, after uint8
				tx.OnChange(map[*TableDef]ChangeFlags{tbl: ChangeFlagNotify | ChangeFlagIncludeNewGoStruct | ChangeFlagIncludeOldGoStruct}, func(_ *Tx, ch *Change) { before = ch.OldGoStruct().(*row).Rank; after = ch.NewGoStruct().(*row).Rank })
				if _, err := tx.TryEditRecord(tbl, uint64(1), func(e *kvo.Editor) error { return e.TrySetValue(1, values.Uint(8)) }); err != nil || before != 7 || after != 8 {
					t.Fatal(before, after, err)
				}
			}
		})
		db.Read(func(tx *Tx) {
			c := FullTableScan[row](tx)
			var dst row
			if !c.Next() {
				t.Fatal("empty")
			}
			if _, err := c.RowInto(&dst); err != nil || dst.ID != 1 || dst.Rank == 0 {
				t.Fatal(dst, err)
			}
			if _, err := c.TryRecordEdit(); err == nil {
				t.Fatal("read transaction edit")
			}
			c.Next()
			if _, err := c.RowInto(&dst); err != nil || dst != (row{ID: 2}) {
				t.Fatal(dst, err)
			}
		})
	}
}
