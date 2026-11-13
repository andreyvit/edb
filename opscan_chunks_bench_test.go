package edb

import (
	"fmt"
	"testing"
)

func BenchmarkResumableScan(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db, rows, _, group := typedTestTable[ID](b, Fast2)
			records := rows.Records()
			index := records.bindIndex[string](group)
			db.Write(TxMetadata{}, func(tx *Tx) {
				for i := range count {
					rows.Put(tx, &typedTestRow[ID]{ID(i + 1), fmt.Sprint(i), "vip"})
				}
			})
			for _, mode := range []string{"Keys", "KeyUnion", "KeyIntersection", "ChunkKeys", "ChunkJoin"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						visited := 0
						if mode == "ChunkKeys" || mode == "ChunkJoin" {
							for chunk := range ReadChunks(db, 1000, func(tx *Tx) RecordScan[ID] { return index.Exact(tx, "vip") }) {
								if mode == "ChunkKeys" {
									for range chunk.Keys() {
										visited++
									}
								} else {
									for id, r := range chunk.Entries() {
										if records.Get(chunk, id).GetValue(1).AsString() == r.GetValue(1).AsString() {
											visited++
										}
									}
								}
							}
						} else {
							db.Read(func(tx *Tx) {
								scan := index.Exact(tx, "vip")
								switch mode {
								case "Keys":
									for range scan.Keys() {
										visited++
									}
								case "KeyUnion":
									for range scan.KeyCursor().Union(scan.KeyCursor()).Keys() {
										visited++
									}
								case "KeyIntersection":
									for range scan.KeyCursor().Intersect(scan.KeyCursor()).Keys() {
										visited++
									}
								}
							})
						}
						if visited != count {
							b.Fatal(visited, count)
						}
					}
				})
			}
		})
	}
}

func TestChunkAndKeyMergeAllocationBudgets(t *testing.T) {
	db, rows, _, group := typedTestTable[ID](t, Fast2)
	index := rows.Records().bindIndex[string](group)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for i := ID(1); i <= 1000; i++ {
			rows.Put(tx, &typedTestRow[ID]{i, fmt.Sprint(i), "vip"})
		}
	})
	db.Read(func(tx *Tx) {
		for _, merge := range []bool{false, true} {
			run := func(limit int) {
				if merge {
					c := index.Exact(tx, "vip").KeyCursor().Intersect(rows.Through(tx, ID(limit)).KeyCursor())
					count := 0
					for c.Next() {
						count++
						_ = c.Key()
					}
					if count != limit {
						t.Fatal(count)
					}
				} else {
					c := index.Exact(tx, "vip").Chunk(limit)
					for range c.Keys() {
					}
					c.More()
					_ = c.Bookmark()
				}
			}
			small := testing.AllocsPerRun(20, func() { run(1) })
			large := testing.AllocsPerRun(20, func() { run(1000) })
			// Includes fixed cursor/buffer setup and boxing the range bound.
			// Permit its one-allocation variation, not per-key allocations.
			if large > small+1 {
				t.Fatalf("merge=%v: allocations grow with rows: one=%g, thousand=%g", merge, small, large)
			}
		}
	})
}

// RawKey isolates merging and its ordering preflight from materializing owned
// string/composite Go keys. Both inputs are the same exact index query.
func BenchmarkVariableKeyMerge(b *testing.B) {
	b.Run("String", func(b *testing.B) {
		benchmarkVariableKeyMerge(b, func(i int) string { return fmt.Sprintf("customer-%06d", i) })
	})
	b.Run("Composite", func(b *testing.B) { benchmarkVariableKeyMerge(b, func(i int) AB { return AB{1, i + 1} }) })
}
func benchmarkVariableKeyMerge[Key any](b *testing.B, key func(int) Key) {
	for _, count := range []int{1000, 10000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			db, rows, _, group := typedTestTable[Key](b, Fast2)
			index := rows.Records().bindIndex[string](group)
			db.Write(TxMetadata{}, func(tx *Tx) {
				for i := range count {
					rows.Put(tx, &typedTestRow[Key]{key(i), fmt.Sprint(i), "vip"})
				}
			})
			b.ReportAllocs()
			for b.Loop() {
				visited := 0
				db.Read(func(tx *Tx) {
					scan := index.Exact(tx, "vip")
					c := scan.KeyCursor().Intersect(scan.KeyCursor())
					for c.Next() {
						if len(c.RawKey()) == 0 {
							b.Fatal("missing key")
						}
						visited++
					}
				})
				if visited != count {
					b.Fatal(visited, count)
				}
			}
		})
	}
}
