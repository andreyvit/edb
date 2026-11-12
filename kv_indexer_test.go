package edb

import (
	"path/filepath"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestPerIndexMixedRows(t *testing.T) {
	_, model := statrow.Model()
	path := filepath.Join(t.TempDir(), "db")
	codec := statrow.Static
	var encodes, decodes int
	codec.Encode = func(dst []byte, row any, layout row2.Layout) []byte {
		encodes++
		return statrow.Static.Encode(dst, row, layout)
	}
	codec.Decode = func(data []byte, row any) {
		decodes++
		statrow.Static.Decode(data, row)
	}
	open := func(legacy, indexed bool) (*DB, *TableDef, *IndexDef) {
		schema := new(Schema)
		var idx *IndexDef
		table := DefineTable[uint64](schema, "accounts", model, func(b *TableBuilder[uint64]) {
			if indexed {
				idx = b.AddIndex[uint64]("balance", func(_ *Tx, pk values.Value, r kvo.Record, yield func(uint64)) {
					if got := IndexPrimaryKey[uint64](pk); got != r.GetValue(1).AsUint() {
						t.Fatalf("primary key %v differs from account ID %d", got, r.GetValue(1).AsUint())
					}
					yield(r.Child(row2.Step{Key: 9}).GetValue(5).AsUint())
				}, IndexInitialFillModeBlocking).IndexDef()
			}
			b.SetRecordStorage(RecordStorage{Generated: &codec, Format: Fast2, MessagePackInserts: legacy})
		}).TableDef()
		db, err := Open(path, schema, Options{IsTesting: true})
		if err != nil {
			t.Fatal(err)
		}
		return db, table, idx
	}
	first, second := statrow.Sample(1), statrow.Sample(2)
	db, _, _ := open(true, false)
	db.Write(func(tx *Tx) { Put(tx, first) })
	db.Close()
	db, _, _ = open(false, false)
	db.Write(func(tx *Tx) { Put(tx, second) })
	db.Close()
	if encodes != 1 {
		t.Fatalf("generated encoder called %d times, want 1", encodes)
	}
	encodes, decodes = 0, 0
	db, table, idx := open(false, true) // A new index forces startup backfill on both formats.
	defer db.Close()

	assertKey := func(tx *Tx, balance, id uint64) {
		t.Helper()
		c := tx.IndexScan(idx, ExactScan(balance))
		if !c.Next() || c.Key() != id || c.Next() {
			t.Fatalf("balance %d must index only row %d", balance, id)
		}
	}
	db.Write(func(tx *Tx) {
		assertKey(tx, first.Profile.Balance, first.ID)
		assertKey(tx, second.Profile.Balance, second.ID)
		old := GetMeta[statrow.State](tx, second.ID)
		tx.Reindex(table, idx)
		if GetMeta[statrow.State](tx, second.ID) != old {
			t.Fatal("reindex changed revision")
		}
		tx.OnChange(map[*TableDef]ChangeFlags{table: ChangeFlagNotify | ChangeFlagIncludeKey}, func(_ *Tx, c *Change) {
			if c.HasNewGoStruct() || !c.HasKey() {
				t.Fatal("wanted only the changed key")
			}
		})
		for _, r := range []*statrow.State{first, second} {
			edit, err := tx.TryOpenRecordEdit(table, r.ID)
			if err != nil || edit == nil {
				t.Fatal(err)
			}
			newBalance := r.ID + 400
			if err := edit.Editor().TryFastSetPathValue([]row2.Step{{Key: 9}, {Key: 5}}, values.Uint(newBalance)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.TrySaveRecord(edit); err != nil {
				t.Fatal(err)
			}
			assertKey(tx, newBalance, r.ID)
			if tx.IndexScan(idx, ExactScan(r.Profile.Balance)).Next() {
				t.Fatal("stale index entry")
			}
		}
		if encodes != 0 || decodes != 0 {
			t.Fatalf("indexed saves and rebuilds called KVO encoder %d times and decoder %d times", encodes, decodes)
		}
		if ok, err := tx.TryUpgradeRecord(table, first.ID); err != nil || !ok {
			t.Fatal(ok, err)
		}
		assertKey(tx, first.ID+400, first.ID)
		AssertIntegrity(t, tx)
	})
}

func TestPerIndexRefusesLayoutDependentUpgrade(t *testing.T) {
	_, m := statrow.Model()
	schema := new(Schema)
	table := DefineTable[uint64](schema, "accounts", m, func(b *TableBuilder[uint64]) {
		b.SetRecordStorage(RecordStorage{Format: Compact2, MessagePackInserts: true})
		b.AddIndex[uint64]("has_profile", func(_ *Tx, _ values.Value, r kvo.Record, yield func(uint64)) {
			if r.Has(9) {
				yield(1)
			}
		}).IndexDef()
	}).TableDef()
	db := setup(t, schema)
	db.Write(func(tx *Tx) {
		Put(tx, &statrow.State{Key: 1})
		old := GetMeta[statrow.State](tx, uint64(1))
		if ok, err := tx.TryUpgradeRecord(table, uint64(1)); ok || err == nil {
			t.Fatal("converted a row whose index would change")
		}
		if r, meta, err := tx.TryGetRecord(table, uint64(1)); err != nil || r.MessagePack() == nil || meta != old {
			t.Fatal(meta, err)
		}
		AssertIntegrity(t, tx)
	})
}

func TestSetIndexerDeclaration(t *testing.T) {
	type builder = TableBuilder[uint64]
	model := testIndexAccess[statrow.State]()
	record := func(b *builder) { b.SetIndexer(func(uint64, kvo.Record, *IndexBuilder) {}) }
	DefineTable(new(Schema), "valid", model, record).TableDef()
	for _, configure := range []func(*builder){
		func(b *builder) { record(b); record(b) },
		func(b *builder) { b.SetIndexer(nil) },
	} {
		assertPanics(t, func() { DefineTable(new(Schema), "accounts", model, configure).TableDef() })
	}
	assertPanics(t, func() { DefineTable(new(Schema), "accounts", nil, record).TableDef() })
}
