package edb

import (
	"errors"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func TestEditRecordAcrossFormats(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, tbl, idx, _ := keyedRecordTable[keylessRecordRow](t, format, false)
			db.Write(func(tx *Tx) {
				Put(tx, &keylessRecordRow{ID: 7, Name: "before"})
				tx.EditRecord(tbl, uint64(7), func(e *kvo.Editor) {
					e.SetValue(1, values.String("after"))
				})
				if !tx.IndexScan(idx, ExactScan("after")).Next() || tx.IndexScan(idx, ExactScan("before")).Next() {
					t.Fatal("edit did not update indexes")
				}
				stop := errors.New("discard edit")
				_, err := tx.TryEditRecord(tbl, uint64(7), func(e *kvo.Editor) error {
					e.SetValue(1, values.String("discarded"))
					return stop
				})
				if !errors.Is(err, stop) || tx.GetRecord(tbl, uint64(7)).GetValue(1).AsString() != "after" {
					t.Fatal("failed edit changed storage", err)
				}
				_, err = tx.TryEditRecord(tbl, uint64(7), func(e *kvo.Editor) error {
					e.SetValue(1, values.String("stale"))
					tx.EditRecord(tbl, uint64(7), func(inner *kvo.Editor) {
						inner.SetValue(1, values.String("fresh"))
					})
					return nil
				})
				if err == nil || tx.GetRecord(tbl, uint64(7)).GetValue(1).AsString() != "fresh" {
					t.Fatal("stale edit overwrote intervening write", err)
				}
				tx.EditRecord(tbl, uint64(8), func(*kvo.Editor) { t.Fatal("edited missing key") })
			})
			db.Read(func(tx *Tx) {
				if _, err := tx.TryEditRecord(tbl, uint64(7), func(*kvo.Editor) error { return nil }); err == nil {
					t.Fatal("edited from a read transaction")
				}
			})
		})
	}
}

func TestRecordConvenienceAndTryAPIs(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, tbl, idx, _ := keyedRecordTable[keylessRecordRow](t, format, false)
			db.Write(func(tx *Tx) {
				edit := tx.NewRecordEdit(tbl, uint64(7))
				edit.Editor().SetValue(1, values.String("before"))
				if err := edit.Editor().TryFastSetPathValue(nil, values.String("invalid")); err == nil {
					t.Fatal("empty edit path accepted")
				}
				assertRecordErrorPanic(t, func() { edit.Editor().FastSetPathValue(nil, values.String("invalid")) })
				tx.SaveRecord(edit)
				stale := tx.OpenRecordEdit(tbl, uint64(7))
				edit.Editor().SetValue(1, values.String("after"))
				tx.SaveRecord(edit)
				if _, err := tx.TrySaveRecord(stale); err == nil {
					t.Fatal("stale save succeeded")
				}
				assertRecordErrorPanic(t, func() { tx.SaveRecord(stale) })
			})
			db.Read(func(tx *Tx) {
				r, meta, err := tx.TryGetRecord(tbl, uint64(7))
				if err != nil || meta.ModCount != 2 || meta != GetMeta[keylessRecordRow](tx, uint64(7)) || r.GetValue(1).AsString() != "after" {
					t.Fatal(meta, err)
				}
				if tx.GetRecord(tbl, uint64(7)).GetValue(1).AsString() != "after" || !tx.GetRecord(tbl, uint64(8)).Missing() {
					t.Fatal("incorrect lookup")
				}
				if tx.GetRecordByKeyRaw(tbl, tbl.EncodeKey(uint64(7))).GetValue(1).AsString() != "after" {
					t.Fatal("incorrect raw-key lookup")
				}
				for _, c := range recordAPICursors(tx, tbl, idx) {
					if !c.Next() {
						t.Fatal("missing row")
					}
					got, gotMeta, err := c.TryRecord()
					if err != nil || gotMeta != meta || got.GetValue(1).AsString() != "after" || c.Record().GetValue(1).AsString() != "after" {
						t.Fatal(gotMeta, err)
					}
				}
				if format != MessagePack && tx.GetRecord(tbl, uint64(7)).GetValue(1).AsString() != "after" {
					t.Fatal("incorrect KVO lookup")
				}
			})
			db.Write(func(tx *Tx) {
				if err := tbl.dataBucketIn(tx).Put(tbl.EncodeKey(uint64(7)), []byte{0xff}); err != nil {
					t.Fatal(err)
				}
			})
			db.Read(func(tx *Tx) {
				if _, _, err := tx.TryGetRecord(tbl, uint64(7)); err == nil {
					t.Fatal("invalid row accepted")
				}
				assertRecordErrorPanic(t, func() { tx.GetRecord(tbl, uint64(7)) })
				assertRecordErrorPanic(t, func() { tx.GetRecordByKeyRaw(tbl, tbl.EncodeKey(uint64(7))) })
				for _, c := range recordAPICursors(tx, tbl, idx) {
					if !c.Next() {
						t.Fatal("missing damaged row")
					}
					if _, _, err := c.TryRecord(); err == nil {
						t.Fatal("invalid row accepted")
					}
					assertRecordErrorPanic(t, func() { c.Record() })
				}
			})
		})
	}
}

type recordAPICursor interface {
	Next() bool
	Record() kvo.Record
	TryRecord() (kvo.Record, ValueMeta, error)
}

func recordAPICursors(tx *Tx, tbl *TableDef, idx *IndexDef) []recordAPICursor {
	return []recordAPICursor{tx.TableScan(tbl, FullScan()), tx.IndexScan(idx, FullScan()), FullTableScan[keylessRecordRow](tx)}
}

func assertRecordErrorPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if _, ok := recover().(error); !ok {
			t.Error("expected error panic")
		}
	}()
	f()
}

func TestRecordReadAllocations(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, tbl, _, _ := keyedRecordTable[keylessRecordRow](t, format, false)
			db.Write(func(tx *Tx) {
				tx.PutByKey(tbl, uint64(1000), &keylessRecordRow{Name: "sample"})
				tx.PutByKey(tbl, uint64(1002), &keylessRecordRow{Name: "other"})
				r, _, err := tx.TryGetRecord(tbl, uint64(1001))
				if err != nil || !r.Missing() {
					t.Fatal("inexact lookup", err)
				}
				tx.PutByKey(tbl, uint64(1000), &keylessRecordRow{Name: "changed"})
				r, _, err = tx.TryGetRecord(tbl, uint64(1000))
				if err != nil || r.GetValue(1).AsString() != "changed" {
					t.Fatal("stale cursor after update", err)
				}
				tx.DeleteByKey(tbl, uint64(1000))
				r, _, err = tx.TryGetRecord(tbl, uint64(1000))
				if err != nil || !r.Missing() {
					t.Fatal("stale cursor after delete", err)
				}
				tx.PutByKey(tbl, uint64(1000), &keylessRecordRow{Name: "sample"})
			})
			db.Read(func(tx *Tx) {
				var retained kvo.Record
				read := func() {
					retained = tx.GetRecord(tbl, uint64(1000))
					if retained.GetValue(1).AsString() != "sample" {
						t.Fatal("unexpected record")
					}
				}
				if n := testing.AllocsPerRun(100, read); n != 0 {
					t.Errorf("GetRecord: %g allocations", n)
				}
				var key uint64 = 1000
				keyValue := reflect.ValueOf(&key).Elem()
				if n := testing.AllocsPerRun(100, func() {
					if key == 1000 {
						key = 1002
					} else {
						key = 1000
					}
					r := tx.GetRecordByKeyVal(tbl, keyValue)
					want := "sample"
					if key == 1002 {
						want = "other"
					}
					if r.GetValue(1).AsString() != want {
						t.Fatal("reflected lookup returned wrong row")
					}
				}); n != 0 {
					t.Errorf("reflected key: %g allocations", n)
				}
				c := tx.TableScan(tbl, FullScan())
				if !c.Next() {
					t.Fatal("empty scan")
				}
				if n := testing.AllocsPerRun(100, func() {
					retained = c.Record()
					if retained.GetValue(1).AsString() != "sample" {
						t.Fatal("unexpected record")
					}
				}); n != 0 {
					t.Errorf("cursor Record: %g allocations", n)
				}
			})
		})
	}
}

func BenchmarkRecordRead(b *testing.B) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		b.Run(format.String(), func(b *testing.B) {
			db, tbl, _, _ := keyedRecordTable[keylessRecordRow](b, format, false)
			db.Write(func(tx *Tx) { tx.PutByKey(tbl, uint64(1000), &keylessRecordRow{Name: "sample"}) })
			db.Read(func(tx *Tx) {
				read := func() {
					r, _, err := tx.TryGetRecord(tbl, uint64(1000))
					if err != nil || r.GetValue(1).AsString() != "sample" {
						b.Fatal(err)
					}
				}
				read()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					read()
				}
				b.StopTimer()
			})
		})
	}
}

// Advance and read different rows on every invocation: warming a cursor must
// not turn an exhausted no-op loop into a misleading zero-allocation result.
func TestRecordIndexScanAllocations(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, tbl, idx, _ := keyedRecordTable[keylessRecordRow](t, format, false)
			db.Write(func(tx *Tx) {
				for i := uint64(1); i <= 1000; i++ {
					tx.PutByKey(tbl, i, &keylessRecordRow{Name: "sample"})
				}
			})
			db.Read(func(tx *Tx) {
				for _, opt := range []ScanOptions{FullScan(), ExactScan("sample"), RangeScan("sample", "sample", true, true)} {
					c := tx.IndexScan(idx, opt)
					if !c.Next() {
						t.Fatal("empty scan")
					}
					_ = c.Record() // initialize the data cursor before counting
					count := 0
					allocs := testing.AllocsPerRun(100, func() {
						if !c.Next() {
							t.Fatal("exhausted before measuring")
						}
						if c.Record().GetValue(1).AsString() != "sample" {
							t.Fatal("wrong row")
						}
						count++
					})
					if allocs != 0 || count != 101 {
						t.Fatalf("%g allocations across %d reads", allocs, count)
					}
				}
			})
		})
	}
}
