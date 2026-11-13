package edb

import (
	"fmt"
	"slices"
	"testing"
)

func TestReadChunks(t *testing.T) {
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			db, rows, _, group := typedTestTable[ID](t, format)
			records := rows.Records()
			index := records.bindIndex[string](group)
			db.Write(TxMetadata{}, func(tx *Tx) {
				for i := ID(1); i <= 7; i++ {
					rows.Put(tx, &typedTestRow[ID]{i, fmt.Sprint(i), "vip"})
				}
			})
			for _, reverse := range []bool{false, true} {
				for _, size := range []int{1, 2, 7, 20} {
					var got []ID
					var transactions []*Tx
					for chunk := range ReadChunks(db, size, func(tx *Tx) RecordScan[ID] {
						scan := index.Exact(tx, "vip")
						if reverse {
							scan = scan.Reversed()
						}
						return scan
					}) {
						transactions = append(transactions, chunk.Tx())
						for id, r := range chunk.Entries() {
							if records.Get(chunk, id).GetValue(1).AsString() != r.GetValue(1).AsString() {
								t.Fatal("join disagrees")
							}
							got = append(got, id)
						}
						if chunk.Count() > size {
							t.Fatal("oversized chunk")
						}
					}
					want := []ID{1, 2, 3, 4, 5, 6, 7}
					if reverse {
						slices.Reverse(want)
					}
					if !slices.Equal(got, want) {
						t.Fatal(size, reverse, got)
					}
					if len(transactions) != (7+size-1)/size {
						t.Fatal("chunk count", len(transactions))
					}
					for _, tx := range transactions {
						if !tx.closed {
							t.Fatal("reader leaked")
						}
					}
				}
			}
			// The same helper infers native chunks, including owned rows and entries.
			var got []ID
			for chunk := range ReadChunks(db, 3, func(tx *Tx) RowScan[typedTestRow[ID], ID] { return rows.Scan(tx) }) {
				for id, row := range chunk.Entries() {
					if row.Key != id {
						t.Fatal("key")
					}
					got = append(got, id)
				}
			}
			if !slices.Equal(got, []ID{1, 2, 3, 4, 5, 6, 7}) {
				t.Fatal(got)
			}
		})
	}
}

func TestChunksManualResume(t *testing.T) {
	db, rows, _, group := typedTestTable[string](t, Compact2)
	records := rows.Records()
	db.Write(TxMetadata{}, func(tx *Tx) {
		for _, k := range []string{"a", "aa", "b", "c"} {
			rows.Put(tx, &typedTestRow[string]{k, k, "vip"})
		}
	})
	var mark ScanBookmark
	var got []string
	for {
		more := false
		db.Read(func(tx *Tx) {
			chunk := records.bindIndex[string](group).Exact(tx, "vip").Resume(mark).Chunk(2)
			// Partial consumption can continue using another iterator on the chunk.
			for k := range chunk.Keys() {
				got = append(got, k)
				break
			}
			for k := range chunk.Keys() {
				got = append(got, k)
			}
			before := chunk.Bookmark()
			more = chunk.More()
			if chunk.More() != more || chunk.Bookmark() != before {
				t.Fatal("lookahead changed bookmark")
			}
			mark = chunk.Bookmark()
			assertPanics(t, func() {
				for range chunk.Keys() {
				}
			})
		})
		if !more {
			break
		}
	}
	if !slices.Equal(got, []string{"a", "aa", "b", "c"}) {
		t.Fatal(got)
	}
	db.Read(func(tx *Tx) {
		assertPanics(t, func() { records.Scan(tx).Chunk(0) })
		empty := records.bindIndex[string](group).Exact(tx, "missing").Resume(mark).Chunk(2)
		// Changing query bounds is allowed, and an empty page retains its bookmark.
		for range empty.Keys() {
			t.Fatal("not empty")
		}
		if empty.More() || empty.Bookmark() != mark {
			t.Fatal("empty chunk lost resume position")
		}
	})
}

func TestReadChunksEarlyStopAndPanic(t *testing.T) {
	db, rows, _, _ := typedTestTable[ID](t, Fast2)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for i := ID(1); i <= 4; i++ {
			rows.Put(tx, &typedTestRow[ID]{i, fmt.Sprint(i), "vip"})
		}
	})
	query := func(tx *Tx) RecordScan[ID] { return rows.Records().Scan(tx) }
	var got []ID
	for chunk := range ReadChunks(db, 3, query) {
		for id := range chunk.Keys() {
			got = append(got, id)
			break
		}
	}
	if !slices.Equal(got, []ID{1, 2, 3, 4}) {
		t.Fatal("partial chunks", got)
	}
	var tx *Tx
	for chunk := range ReadChunks(db, 2, query) {
		tx = chunk.Tx()
		break
	}
	if !tx.closed {
		t.Fatal("break leaked reader")
	}
	calls := 0
	for chunk := range ReadChunks(db, 2, query) {
		tx = chunk.Tx()
		calls++
	}
	if calls != 1 || !tx.closed {
		t.Fatal("unconsumed chunk did not stop")
	}
	assertPanics(t, func() {
		for chunk := range ReadChunks(db, 2, query) {
			tx = chunk.Tx()
			panic("test")
		}
	})
	if !tx.closed {
		t.Fatal("panic leaked reader")
	}
	assertPanics(t, func() { ReadChunks(db, 0, query) })
	db.Read(func(other *Tx) {
		assertPanics(t, func() {
			for range ReadChunks(db, 2, func(*Tx) RecordScan[ID] { return query(other) }) {
			}
		})
	})
}

func TestReadChunksSnapshotChangeAndInitialResume(t *testing.T) {
	db, rows, _, _ := typedTestTable[ID](t, Fast2)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for _, id := range []ID{1, 3, 5} {
			rows.Put(tx, &typedTestRow[ID]{id, fmt.Sprint(id), "vip"})
		}
	})
	var initial ScanBookmark
	db.Read(func(tx *Tx) { c := rows.Scan(tx).Raw(); c.Next(); initial = c.Bookmark() })
	calls := 0
	var got []ID
	var prior *Tx
	for chunk := range ReadChunks(db, 1, func(tx *Tx) RecordScan[ID] {
		calls++
		if calls == 2 {
			if !prior.closed {
				t.Fatal("prior chunk still open")
			}
			// The new snapshot has already opened here; writes are performed below
			// between manual chunks instead. This factory only checks reader closure.
		}
		return rows.Records().Scan(tx).Resume(initial)
	}) {
		prior = chunk.Tx()
		for id := range chunk.Keys() {
			got = append(got, id)
		}
	}
	if !slices.Equal(got, []ID{3, 5}) {
		t.Fatal(got)
	}
	db.Write(TxMetadata{}, func(tx *Tx) { rows.Put(tx, &typedTestRow[ID]{2, "2", "vip"}); rows.Delete(tx, 3) })
	db.Read(func(tx *Tx) {
		if got := slices.Collect(rows.Scan(tx).Resume(initial).Keys()); !slices.Equal(got, []ID{2, 5}) {
			t.Fatal("new snapshot", got)
		}
	})
}

func TestChunkKeysNeverFetchRows(t *testing.T) {
	db, rows, _, group := typedTestTable[AB](t, MessagePack)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for _, key := range []AB{{1, 1}, {1, 2}, {2, 1}} {
			rows.Put(tx, &typedTestRow[AB]{key, fmt.Sprint(key), "vip"})
			putRawTableValue(t, tx, rows.Table(), rows.Table().EncodeKey(key), []byte{0xff})
		}
	})
	var got []AB
	for chunk := range ReadChunks(db, 2, func(tx *Tx) RecordScan[AB] { return rows.Records().bindIndex[string](group).Exact(tx, "vip") }) {
		for key := range chunk.Keys() {
			got = append(got, key)
		}
		chunk.More()
		if chunk.raw.(*RawIndexCursor).dcur != nil {
			t.Fatal("key-only chunk fetched row")
		}
	}
	if !slices.Equal(got, []AB{{1, 1}, {1, 2}, {2, 1}}) {
		t.Fatal(got)
	}
}
