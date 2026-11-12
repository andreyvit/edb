package edb

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type accessDetails struct {
	Count   int8
	Private string
}
type accessMember struct {
	ID      uint64 `msgpack:"-"`
	Name    string
	Details *accessDetails
	History map[string]int64
	Private []byte
}

func TestAccessOnlyTable(t *testing.T) {
	s := kvo.NewSchema()
	members := kvo.NewEntityType(s, "member")
	details := kvo.NewEntityType(s, "details")
	kvo.NewProp(s, 1, "label", kvo.TMachineString, nil)
	kvo.NewProp(s, 2, "details", details, nil)
	count := kvo.NewIntType[int8]("count", nil)
	kvo.NewProp(s, 3, "count", count, nil)
	kvo.NewProp(s, 4, "history", kvo.MapOf(kvo.TMachineString, kvo.TInt64), nil)
	kvo.NewModel(s, details, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(3)) })
	m := kvo.NewModel(s, members, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(1))
		b.Prop(s.MustPropByCode(2))
		b.Prop(s.MustPropByCode(4), kvo.KeepDictionaryZeroValues)
	})
	m.RegisterNativeType(reflect.TypeFor[accessMember](), map[kvo.PropCode]string{1: "Name", 2: "Details", 4: "History"})
	details.Model().RegisterNativeType(reflect.TypeFor[accessDetails](), map[kvo.PropCode]string{3: "Count"})
	path := filepath.Join(t.TempDir(), "members")
	open := func(withKV bool) (*DB, *TableDef, *IndexDef) {
		sc := new(Schema)
		var idx *IndexDef
		model := m
		if !withKV {
			model = testIndexAccess[accessMember](indexName)
		}
		tbl := DefineTable[uint64](sc, "members", model, func(b *TableBuilder[uint64]) {
			idx = b.AddIndex[string]("name", nil, IndexOptDeprecatedUnique).IndexDef()
			if withKV {
				b.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, string(r.GetValue(1).AsBytes())) })
			} else {
				b.SetIndexer(func(key uint64, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.Get(indexName)) })
			}
		}).TableDef()
		db, err := Open(path, sc, Options{IsTesting: true})
		if err != nil {
			t.Fatal(err)
		}
		return db, tbl, idx
	}
	key := uint64(1)
	orig := accessMember{key, "old", &accessDetails{-4, "opaque"}, map[string]int64{"y": 0, "x": 1}, []byte{7, 8}}
	db, tbl, _ := open(false)
	var frozen []byte
	db.Write(func(tx *Tx) {
		Put(tx, &orig)
		Put(tx, &accessMember{ID: 2, Name: "empty"})
		c := tx.TableScan(tbl, FullScan())
		c.Next()
		frozen = bytes.Clone(c.RawRow())
	})
	db.Close()
	db, tbl, idx := open(true)
	child := row2.Step{Key: 2}
	leaf := []row2.Step{child, {Key: 3}}
	db.Write(func(tx *Tx) {
		r, _, err := tx.TryGetRecord(tbl, key)
		if err != nil || r.Child(child).GetValue(3).AsInt() != -4 {
			t.Fatal(err)
		}
		if row2.Trusted(r.GetValue(4).AsBytes()).GetString("y", row2.Signed).AsInt() != 0 || row2.Trusted(r.GetValue(4).AsBytes()).Len() != 2 {
			t.Fatal("dictionary membership")
		}
		if _, err := tx.TryUpgradeRecord(tbl, key); err != ErrNoRecordBinding {
			t.Fatal("access grants storage upgrade", err)
		}
		e, err := tx.TryOpenRecordEdit(tbl, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.TrySaveRecord(e); err != nil {
			t.Fatal(err)
		}
		c := tx.TableScan(tbl, FullScan())
		c.Next()
		if !bytes.Equal(c.RawRow(), frozen) {
			t.Fatal("read or unopened edit rewrote bytes")
		}
		fresh, err := tx.TryNewRecordEdit(tbl, uint64(3))
		if err != nil {
			t.Fatal(err)
		}
		if err = fresh.Editor().TrySetValue(1, values.Bytes([]byte("inserted"))); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.TrySaveRecord(fresh); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.TryNewRecordEdit(tbl, uint64(3)); err == nil {
			t.Fatal("reinsert allowed")
		}
		stale, _ := tx.TryOpenRecordEdit(tbl, key)
		if err = e.Editor().TryFastSetPathValue(leaf, values.Int(-9)); err != nil {
			t.Fatal(err)
		}
		if err = e.Editor().TrySetValue(1, values.Bytes([]byte("new"))); err != nil {
			t.Fatal(err)
		}
		if err = e.Editor().TryFastSetPathValue([]row2.Step{child}, values.Value{}); err == nil {
			t.Fatal("allowed replacement of partially exposed struct")
		}
		var w row2.Writer
		w.Begin(0, "", row2.Dictionary, row2.Fast2)
		w.PutPresent(0, "new", values.Int(0), row2.Signed)
		w.End()
		if err = e.Editor().TrySetValue(4, values.Bytes(w.BytesWritten())); err != nil {
			t.Fatal(err)
		}
		meta, err := tx.TrySaveRecord(e)
		if err != nil || meta.ModCount != 2 {
			t.Fatal(meta, err)
		}
		if _, err = tx.TrySaveRecord(stale); err == nil {
			t.Fatal("stale save")
		}
		c = tx.TableScan(tbl, FullScan())
		c.Next()
		if c.RawRow()[0] != 1 {
			t.Fatal("changed envelope")
		}
		if Lookup[accessMember](tx, idx, "old") != nil {
			t.Fatal("stale index")
		}
		ic := ExactIndexScan[accessMember](tx, idx, "new")
		if !ic.Next() {
			t.Fatal("missing index")
		}
		result, _, err := ic.TryRecord()
		if err != nil || result.Child(child).GetValue(3).AsInt() != -9 {
			t.Fatal(err)
		}
		e, _ = tx.TryOpenRecordEdit(tbl, uint64(2))
		if err = e.Editor().TryFastSetPathValue(leaf, values.Int(128)); err == nil {
			t.Fatal("overflow accepted")
		}
		if _, err = tx.TrySaveRecord(e); err != nil || Get[accessMember](tx, uint64(2)).Details != nil {
			t.Fatal("failed edit created child", err)
		}
		if err = e.Editor().TryFastSetPathValue(leaf, values.Int(7)); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.TrySaveRecord(e); err != nil {
			t.Fatal(err)
		}
	})
	db.Close()
	// The original table declaration can still read both rows and indexes.
	db, _, idx = open(false)
	defer db.Close()
	db.Read(func(tx *Tx) {
		want := orig
		want.Name = "new"
		want.Details = &accessDetails{-9, "opaque"}
		want.History = map[string]int64{"new": 0}
		if got := Lookup[accessMember](tx, idx, "new"); !reflect.DeepEqual(got, &want) {
			t.Fatalf("got %#v want %#v", got, want)
		}
		if got := Lookup[accessMember](tx, idx, "inserted"); got == nil || got.ID != 3 {
			t.Fatal("legacy reader cannot find inserted row")
		}
		if got := Get[accessMember](tx, uint64(2)); got.Details.Count != 7 {
			t.Fatal(got)
		}
	})
}
