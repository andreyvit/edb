package edb

import (
	"fmt"
	"testing"
)

func TestTypedGetAllocations(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			t.Run("uint64", func(t *testing.T) { testTypedGetAllocations(t, format, ID(1000)) })
			t.Run("string", func(t *testing.T) { testTypedGetAllocations(t, format, typedStringKey("alice")) })
		})
	}
}

func testTypedGetAllocations[Key any](t *testing.T, format StorageFormat, key Key) {
	db, rows, _, _ := typedTestTable[Key](t, format)
	records := rows.Records()
	db.Write(func(tx *Tx) { rows.Put(tx, &typedTestRow[Key]{key, "alice", "vip"}) })
	db.Read(func(tx *Tx) {
		if n := testing.AllocsPerRun(100, func() {
			if r := records.Get(tx, key); r.GetValue(1).AsString() != "alice" {
				t.Fatal("incorrect record")
			}
		}); n != 0 {
			t.Fatalf("typed Get: %g allocations", n)
		}
	})
}

// Includes opening/closing a read transaction, scanning a nonunique index,
// retrieving each row, and joining by primary key. Both APIs do identical work;
// using the same table for the second lookup isolates the access API overhead.
func BenchmarkTypedJoinedScan(b *testing.B) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		for _, count := range []int{1, 1000} {
			b.Run(fmt.Sprintf("%s/%d", format, count), func(b *testing.B) {
				db, rows, _, byGroup := typedTestTable[ID](b, format)
				records := rows.Records()
				index := &Index[string]{byGroup}
				db.Write(func(tx *Tx) {
					for i := range count {
						rows.Put(tx, &typedTestRow[ID]{ID(1000 + i), fmt.Sprint(i), "vip"})
					}
				})
				for _, mode := range []string{"Rows", "RecordCursor", "TypedRecords", "TypedKeys"} {
					b.Run(mode, func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							var visited int
							db.Read(func(tx *Tx) {
								switch mode {
								case "Rows":
									for c := ExactIndexScan[typedTestRow[ID]](tx, byGroup, "vip"); c.Next(); {
										r := c.Row()
										if joined := Get[typedTestRow[ID]](tx, r.Key); joined.Name == r.Name {
											visited++
										}
									}
								case "RecordCursor":
									for c := tx.IndexScan(byGroup, ExactScan("vip")); c.Next(); {
										r := c.Record()
										if joined := tx.GetRecordByKeyRaw(rows.Table(), c.RawKey()); joined.GetValue(1).AsString() == r.GetValue(1).AsString() {
											visited++
										}
									}
								case "TypedRecords":
									for key, r := range records.ExactIndexScan(tx, index, "vip").Entries() {
										if joined := records.Get(tx, key); joined.GetValue(1).AsString() == r.GetValue(1).AsString() {
											visited++
										}
									}
								case "TypedKeys":
									for range records.ExactIndexScan(tx, index, "vip").Keys() {
										visited++
									}
								}
							})
							if visited != count {
								b.Fatal("incorrect result", visited)
							}
						}
					})
				}
			})
		}
	}
}

func BenchmarkTypedKeys(b *testing.B) {
	b.Run("uint64", func(b *testing.B) { benchmarkTypedKeys(b, func(i int) uint64 { return uint64(i + 1000) }) })
	b.Run("string", func(b *testing.B) {
		benchmarkTypedKeys(b, func(i int) string { return fmt.Sprintf("customer-%04d", i) })
	})
	b.Run("composite", func(b *testing.B) { benchmarkTypedKeys(b, func(i int) AB { return AB{1, i + 1000} }) })
	b.Run("custom", func(b *testing.B) {
		benchmarkTypedKeys(b, func(i int) customUintKey { return customUintKey(i + 1000) })
	})
}

func benchmarkTypedKeys[Key any](b *testing.B, makeKey func(int) Key) {
	db, rows, _, _ := typedTestTable[Key](b, Fast2)
	records := rows.Records()
	db.Write(func(tx *Tx) {
		for i := range 1000 {
			rows.Put(tx, &typedTestRow[Key]{makeKey(i), fmt.Sprint(i), "vip"})
		}
	})
	b.Run("Get", func(b *testing.B) {
		db.Read(func(tx *Tx) {
			key := makeKey(500)
			records.Get(tx, key) // Warm transaction-local lookup metadata.
			b.ReportAllocs()
			for b.Loop() {
				if records.Get(tx, key).Missing() {
					b.Fatal("missing")
				}
			}
		})
	})
	b.Run("Scan1000", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var count int
			db.Read(func(tx *Tx) {
				for range records.Scan(tx).Keys() {
					count++
				}
			})
			if count != 1000 {
				b.Fatal(count)
			}
		}
	})
}
