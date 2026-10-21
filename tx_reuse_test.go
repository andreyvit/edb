package edb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestTxSkipsBufferTrackingWhenReuseDisabled(t *testing.T) {
	db := openTxReuseDB(t, basicSchema, false)

	db.Write(func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
		if tx.valueBufs != nil {
			t.Fatalf("valueBufs tracked with reuse disabled: %d", len(tx.valueBufs))
		}
		if tx.indexValueBufs != nil {
			t.Fatalf("indexValueBufs tracked with reuse disabled: %d", len(tx.indexValueBufs))
		}
	})

	db.Read(func(tx *Tx) {
		c := ExactIndexScan[User](tx, usersByName, "foo")
		if !c.Next() {
			t.Fatal("ExactIndexScan returned no rows")
		}
		if tx.indexKeyBufs != nil {
			t.Fatalf("indexKeyBufs tracked with reuse disabled: %d", len(tx.indexKeyBufs))
		}
	})
}

func TestTxTracksAndReleasesBuffersWhenReuseEnabled(t *testing.T) {
	db := openTxReuseDB(t, basicSchema, true)

	tx := db.BeginUpdate()
	defer tx.Close()
	Put(tx, &User{ID: 1, Name: "foo", Email: "foo@example.com"})
	if len(tx.valueBufs) == 0 {
		t.Fatalf("valueBufs not tracked")
	}
	if len(tx.indexValueBufs) == 0 {
		t.Fatalf("indexValueBufs not tracked")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	tx.Close()
	if tx.valueBufs != nil || tx.indexValueBufs != nil {
		t.Fatalf("write buffers not released")
	}

	tx = db.BeginRead()
	defer tx.Close()
	c := ExactIndexScan[User](tx, usersByName, "foo")
	if !c.Next() {
		t.Fatal("ExactIndexScan returned no rows")
	}
	if len(tx.indexKeyBufs) == 0 {
		t.Fatalf("indexKeyBufs not tracked")
	}
	tx.Close()
	if tx.indexKeyBufs != nil {
		t.Fatalf("index key buffers not released")
	}
}

func TestTxReuseBuffersTransactionOutcomes(t *testing.T) {
	for _, mode := range []string{"commit", "rollback", "write-panic", "error", "panic", "commit-despite-error", "commit-despite-panic"} {
		t.Run(mode, func(t *testing.T) {
			db := openTxReuseDB(t, basicSchema, true)
			row := &User{ID: 1, Name: "retained", Email: "retained@example.com"}
			var captured *Tx
			mutate := func(tx *Tx) {
				captured = tx
				Put(tx, row)
				_ = ExactIndexScan[User](tx, usersByName, row.Name)
			}
			wantErr := errors.New("reject transaction")
			committed := mode == "commit" || strings.HasPrefix(mode, "commit-despite-")
			switch mode {
			case "commit", "rollback":
				tx := db.BeginUpdate()
				defer tx.Close()
				mutate(tx)
				if committed {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
					assertTxReuseTracked(t, tx)
				}
				tx.Close()
			case "write-panic":
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					db.Write(func(tx *Tx) {
						mutate(tx)
						panic(wantErr)
					})
				}()
				if recoveredErr, ok := recovered.(error); !ok || !errors.Is(recoveredErr, wantErr) {
					t.Fatalf("panic = %v, wanted %v", recovered, wantErr)
				}
			default:
				err := db.Tx(true, func(tx *Tx) error {
					mutate(tx)
					if committed {
						tx.CommitDespiteError()
					}
					if strings.HasSuffix(mode, "panic") {
						panic(wantErr)
					}
					return wantErr
				})
				if strings.HasSuffix(mode, "panic") {
					var panicErr panicked
					if !errors.As(err, &panicErr) {
						t.Fatalf("error = %v, wanted recovered panic %v", err, wantErr)
					}
					if reason, ok := panicErr.reason.(error); !ok || !errors.Is(reason, wantErr) {
						t.Fatalf("panic reason = %v, wanted %v", panicErr.reason, wantErr)
					}
				} else if !errors.Is(err, wantErr) {
					t.Fatalf("error = %v, wanted %v", err, wantErr)
				}
			}
			assertTxReuseReleased(t, captured)
			captured.Close()
			assertTxReuseReleased(t, captured)
			db.Read(func(tx *Tx) {
				var expected *User
				if committed {
					expected = row
				}
				deepEqual(t, Get[User](tx, row.ID), expected)
				deepEqual(t, Lookup[User](tx, usersByEmail, row.Email), expected)
				deepEqual(t, Lookup[User](tx, usersByName, row.Name), expected)
			})
		})
	}
}

func TestTxReuseBuffersManagedReadCleanup(t *testing.T) {
	for _, panicRead := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%v", panicRead), func(t *testing.T) {
			db := openTxReuseDB(t, basicSchema, true)
			db.Write(func(tx *Tx) { Put(tx, &User{ID: 1, Name: "reader", Email: "reader@example.com"}) })
			wantErr := errors.New("stop reading")
			var captured *Tx
			var err error
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				err = db.Tx(false, func(tx *Tx) error {
					captured = tx
					c := ExactIndexScan[User](tx, usersByName, "reader")
					if !c.Next() || c.Row().ID != 1 || len(tx.indexKeyBufs) == 0 {
						return errors.New("missing row or tracked scan boundary")
					}
					if panicRead {
						panic(wantErr)
					}
					return wantErr
				})
			}()
			if panicRead {
				if recoveredErr, ok := recovered.(error); !ok || !errors.Is(recoveredErr, wantErr) {
					t.Fatalf("panic = %v, wanted %v", recovered, wantErr)
				}
			} else if recovered != nil || !errors.Is(err, wantErr) {
				t.Fatalf("error = %v, panic = %v, wanted error %v", err, recovered, wantErr)
			}
			assertTxReuseReleased(t, captured)
		})
	}
}

type txReuseGrowthRow struct {
	Key     string   `msgpack:"-"`
	Payload []byte   `msgpack:"p"`
	Entries []string `msgpack:"e"`
}

func TestTxReuseBuffersGrowthAndNoopWrites(t *testing.T) {
	schema := &Schema{}
	index := AddIndex[string]("entry").Unique()
	AddTable(schema, "growth", 1, func(row *txReuseGrowthRow, ib *IndexBuilder) {
		for _, entry := range row.Entries {
			ib.Add(index, entry)
		}
	}, nil, []*Index{index})
	db := openTxReuseDB(t, schema, true)
	var previous *Tx
	var previousRows []*txReuseGrowthRow
	for round := range 3 {
		rows := make([]*txReuseGrowthRow, byteBufferListCapacityInitial+1)
		tx := db.BeginUpdate()
		defer tx.Close()
		cursors := make([]Cursor[txReuseGrowthRow], len(rows))
		for i := range rows {
			size := []int{1, valueBufferCapacityInitial + 1, valueBufferCapacityLimit + 1}[i%3]
			row := &txReuseGrowthRow{
				Key:     fmt.Sprintf("row-%03d-%s", i, strings.Repeat("k", keyBufferCapacityInitial+1)),
				Payload: bytes.Repeat([]byte{byte(round + i + 1)}, size),
			}
			entries := 1
			if i == 0 {
				entries = indexRowsBufferCapacityInitial + 1
			}
			for entry := range entries {
				row.Entries = append(row.Entries, fmt.Sprintf("%d/%03d/%03d/%s", round, i, entry, strings.Repeat("x", keyBufferCapacityInitial+1)))
			}
			rows[i] = row
			if !Put(tx, row) {
				t.Fatalf("round %d row %d was not modified", round, i)
			}
			values, indexValues := len(tx.valueBufs), len(tx.indexValueBufs)
			if Put(tx, row) {
				t.Fatalf("round %d row %d no-op changed data", round, i)
			}
			if len(tx.valueBufs) != values || len(tx.indexValueBufs) != indexValues {
				t.Fatalf("round %d row %d no-op retained buffers", round, i)
			}
			cursors[i] = ExactIndexScan[txReuseGrowthRow](tx, index, row.Entries[0])
		}
		assertTxReuseTracked(t, tx)
		if len(tx.valueBufs) <= byteBufferListCapacityInitial || len(tx.indexValueBufs) <= indexRowsBufferCapacityInitial || len(tx.indexKeyBufs) <= byteBufferListCapacityInitial {
			t.Fatalf("buffer lists did not grow: values=%d indexValues=%d keys=%d", len(tx.valueBufs), len(tx.indexValueBufs), len(tx.indexKeyBufs))
		}
		oversized := false
		for _, buf := range tx.valueBufs {
			oversized = oversized || cap(buf.data) > valueBufferCapacityLimit
		}
		if !oversized {
			t.Fatal("no oversized value buffer")
		}
		if previous != nil {
			previous.Close()
		}
		for i, cursor := range cursors {
			if !cursor.Next() {
				t.Fatalf("cursor %d lost its boundary", i)
			}
			deepEqual(t, cursor.Row(), rows[i])
			if cursor.Next() {
				t.Fatalf("cursor %d returned extra rows", i)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		assertTxReuseTracked(t, tx)
		tx.Close()
		assertTxReuseReleased(t, tx)
		db.Read(func(tx *Tx) {
			if got := Count(FullTableScan[txReuseGrowthRow](tx).Raw()); got != len(rows) {
				t.Fatalf("row count = %d, wanted %d", got, len(rows))
			}
			for _, row := range rows {
				deepEqual(t, Get[txReuseGrowthRow](tx, row.Key), row)
				deepEqual(t, Lookup[txReuseGrowthRow](tx, index, row.Entries[0]), row)
				for _, entry := range row.Entries {
					key, ok := LookupKey[string](tx, index, entry)
					if !ok || key != row.Key {
						t.Fatalf("index %q points to %q, found=%v", entry, key, ok)
					}
				}
			}
			for _, row := range previousRows {
				for _, entry := range row.Entries {
					if LookupExists(tx, index, entry) {
						t.Fatalf("stale index entry %q", entry)
					}
				}
			}
		})
		previous, previousRows = tx, rows
	}
}

type txReuseEncodingRow struct {
	Key   int `msgpack:"-"`
	Value any `msgpack:"v"`
}

func TestTxReuseBuffersEncodingFailure(t *testing.T) {
	schema := &Schema{}
	index := AddIndex[int]("key").Unique()
	AddTable(schema, "encoding", 1, func(row *txReuseEncodingRow, ib *IndexBuilder) {
		ib.Add(index, row.Key)
	}, nil, []*Index{index})
	db := openTxReuseDB(t, schema, true)
	var captured *Tx
	var values, indexValues int
	err := db.Tx(true, func(tx *Tx) error {
		captured = tx
		defer func() {
			values, indexValues = len(tx.valueBufs), len(tx.indexValueBufs)
		}()
		Put(tx, &txReuseEncodingRow{Key: 1, Value: make(chan int)})
		return nil
	})
	var panicErr panicked
	if !errors.As(err, &panicErr) || !strings.Contains(err.Error(), "chan int") {
		t.Fatalf("error = %v, wanted encoding panic", err)
	}
	if values != 0 || indexValues != 0 {
		t.Fatalf("encoding panic retained buffers: values=%d indexValues=%d", values, indexValues)
	}
	assertTxReuseReleased(t, captured)
	row := &txReuseEncodingRow{Key: 2, Value: "valid"}
	db.Write(func(tx *Tx) { Put(tx, row) })
	db.Read(func(tx *Tx) {
		if Get[txReuseEncodingRow](tx, 1) != nil || LookupExists(tx, index, 1) {
			t.Fatal("encoding failure persisted a row or index entry")
		}
		deepEqual(t, Get[txReuseEncodingRow](tx, row.Key), row)
		deepEqual(t, Lookup[txReuseEncodingRow](tx, index, row.Key), row)
	})
}

func openTxReuseDB(t *testing.T, schema *Schema, reuse bool) *DB {
	t.Helper()
	path := InMemory
	if !testing.Short() {
		path = filepath.Join(t.TempDir(), "reuse.db")
	}
	db, err := Open(path, schema, Options{IsTesting: true, ReuseTxBuffers: reuse})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

func assertTxReuseTracked(t *testing.T, tx *Tx) {
	t.Helper()
	if len(tx.valueBufs) == 0 || len(tx.indexValueBufs) == 0 || len(tx.indexKeyBufs) == 0 {
		t.Fatal("transaction lost its buffers before Close")
	}
	seen := make(map[*byteBuffer]bool)
	for _, buffers := range [][]*byteBuffer{tx.valueBufs, tx.indexValueBufs, tx.indexKeyBufs} {
		for _, buf := range buffers {
			if buf == nil || seen[buf] {
				t.Fatalf("invalid or duplicate buffer owner %p", buf)
			}
			seen[buf] = true
		}
	}
}

func assertTxReuseReleased(t *testing.T, tx *Tx) {
	t.Helper()
	if tx == nil || !tx.closed {
		t.Fatal("transaction did not close")
	}
	if tx.valueBufs != nil || tx.indexValueBufs != nil || tx.indexKeyBufs != nil || tx.valueBufsOwner != nil || tx.indexValueBufsOwner != nil || tx.indexKeyBufsOwner != nil {
		t.Fatal("transaction retained buffer ownership after Close")
	}
}
