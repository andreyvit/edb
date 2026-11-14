package edb

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func runReplicationRejectedBatch(t *testing.T, db *DB, callers int, base ID) ID {
	t.Helper()
	db.bdb.MaxBatchSize = callers
	db.bdb.MaxBatchDelay = time.Hour
	beforeWrite := errors.New("pre-write rejection")
	afterWrite := errors.New("post-write rejection")
	var entered, preID, postID atomic.Int32
	attempts := make([]atomic.Int32, callers)
	type result struct {
		index int
		err   error
	}
	done := make(chan result, callers)
	for i := range callers {
		go func() {
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
				attempts[i].Add(1)
				rank := entered.Add(1)
				if callers == 3 && rank == 2 {
					preID.Store(int32(i + 1))
					return beforeWrite
				}
				id := base + ID(i+1)
				Put(tx, &User{ID: id, Name: "batch", Email: fmt.Sprint(id)})
				if rank == int32(callers) {
					postID.Store(int32(i + 1))
					return afterWrite
				}
				return nil
			})
			done <- result{i, err}
		}()
	}
	results := make([]error, callers)
	for range callers {
		r := <-done
		results[r.index] = r.err
	}
	winner := -1
	for i, err := range results {
		switch int32(i + 1) {
		case preID.Load():
			if !errors.Is(err, beforeWrite) || attempts[i].Load() != 1 {
				t.Fatalf("pre-write result=%v attempts=%d", err, attempts[i].Load())
			}
		case postID.Load():
			if !errors.Is(err, afterWrite) || attempts[i].Load() != 1 {
				t.Fatalf("post-write result=%v attempts=%d", err, attempts[i].Load())
			}
		default:
			if err != nil || attempts[i].Load() != int32(callers) {
				t.Fatalf("survivor result=%v attempts=%d", err, attempts[i].Load())
			}
			winner = i
		}
	}
	if winner < 0 || postID.Load() == 0 {
		t.Fatal("batch did not exercise the failure sequence")
	}
	return base + ID(winner+1)
}

func TestReplicationRetry_releases_rejected_attempt_before_admission(t *testing.T) {
	for _, limit := range []string{"slot"} {
		for _, callers := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/%d", limit, callers), func(t *testing.T) {
				sink := newOrderedReplicationSink()
				opt := replicationCaptureOptions(sink)
				sink.capacity = 1
				db := openReplicationTest(t, basicSchema, opt)
				winner := runReplicationRejectedBatch(t, db, callers, 10)
				sink.mu.Lock()
				groups := slices.Clone(sink.delivered)
				cancelled := 0
				for _, slot := range sink.slots {
					if slot.cancelled {
						cancelled++
					}
				}
				sink.mu.Unlock()
				if len(groups) != 1 || cancelled != callers-1 || groups[0].TransactionID != 1 || len(groups[0].Changes) != 1 || !bytes.Equal(groups[0].Changes[0].Key, usersTable.EncodeKey(winner)) {
					t.Fatalf("groups=%+v cancelled=%d", groups, cancelled)
				}
				requireReplicationID(t, db, 1)
				if err := sink.Drain(); err != nil {
					t.Fatal(err)
				}
				replicationAssertClean(t, db)
			})
		}
	}
}

func TestReplicationApply_duplicate_does_not_commit_empty_transaction(t *testing.T) {
	db := openReplicationTest(t, basicSchema, replicationApplyOptions())
	g := &ReplicationGroup{TransactionID: 1, SchemaRev: 3}
	if err := db.ApplyReplication(g); err != nil {
		t.Fatal(err)
	}
	var before, after int
	db.Read(TxMetadata{}, func(tx *Tx) { before = tx.btx.ID() })
	for range 3 {
		if err := db.ApplyReplication(g); err != nil {
			t.Fatal(err)
		}
	}
	db.Read(TxMetadata{}, func(tx *Tx) { after = tx.btx.ID() })
	if after != before {
		t.Fatalf("duplicate physical commits=%d", after-before)
	}
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 2, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 2)
	db.Read(TxMetadata{}, func(tx *Tx) {
		if tx.btx.ID() != before+1 {
			t.Fatal("new empty filtered group did not commit")
		}
	})
}

func TestReplicationShard_checks_budget_before_copy(t *testing.T) {
	for _, mode := range []string{"oversized", "full-buffer"} {
		t.Run(mode, func(t *testing.T) {
			sink := &replicationTestSink{}
			opt := replicationCaptureOptions(sink)
			shard := make([]byte, 4<<20)
			wantErr := ErrReplicationGroupSize
			wantID := uint64(0)
			allocationLimit := uint64(1 << 20)
			key := usersTable.EncodeKey(ID(2))
			if mode == "oversized" {
				sink.maxGroupBytes = 1024
				sink.maxBufferedBytes = 1024
				opt.Capture.Shard = func(ReplicationObject, []byte, ReplicationValue) ([]byte, error) { return shard, nil }
			} else {
				shard = make([]byte, 512<<10)
				sink.maxBufferedBytes = 1 << 20
				opt.Capture.Shard = func(_ ReplicationObject, k []byte, _ ReplicationValue) ([]byte, error) {
					if bytes.Equal(k, key) {
						return shard, nil
					}
					return []byte("seed"), nil
				}
				wantErr = ErrReplicationOverload
				wantID = 1
				allocationLimit = 256 << 10
			}
			db := openReplicationTest(t, basicSchema, opt)
			if mode == "full-buffer" {
				db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
			}
			tx := db.BeginUpdate(TxMetadata{})
			defer tx.Close()
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err := safelyCall(func(tx *Tx) error { Put(tx, &User{ID: 2}); return nil }, tx)
			runtime.ReadMemStats(&after)
			if err == nil || !errors.Is(tx.Commit(), wantErr) {
				t.Fatalf("write=%v commit=%v", err, tx.replicationError())
			}
			tx.Close()
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= allocationLimit {
				t.Fatalf("rejected shard allocated %d bytes", allocated)
			}
			requireReplicationID(t, db, wantID)
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			replicationAssertClean(t, db)
		})
	}
}

func TestDelete_handler_failure_rolls_back_complete_mutation(t *testing.T) {
	for _, capture := range []bool{false, true} {
		for _, nested := range []bool{false, true} {
			for _, caught := range []bool{false, true} {
				t.Run(fmt.Sprintf("capture=%t/nested=%t/caught=%t", capture, nested, caught), func(t *testing.T) {
					sink := &replicationTestSink{}
					var opt *ReplicationOptions
					if capture {
						opt = replicationCaptureOptions(sink)
					}
					db := openReplicationTest(t, basicSchema, opt)
					db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "seed", Email: "seed@example.com"}) })
					err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
						tx.CommitDespiteError()
						tx.OnChange(map[*TableDef]ChangeFlags{usersTable: ChangeFlagNotify}, func(tx *Tx, c *Change) {
							if c.op != OpDelete {
								return
							}
							if nested {
								Put(tx, &User{ID: 2, Email: "child@example.com"})
							}
							panic("handler failed")
						})
						remove := func(tx *Tx) error { tx.DeleteByKey(usersTable, ID(1)); return nil }
						if caught {
							_ = safelyCall(remove, tx)
							return nil
						}
						return remove(tx)
					})
					if err == nil {
						t.Fatal("handler failure committed")
					}
					db.Read(TxMetadata{}, func(tx *Tx) {
						if Get[User](tx, ID(1)) == nil || Lookup[User](tx, usersByEmail, "seed@example.com") == nil || Lookup[User](tx, usersByName, "seed") == nil || Get[User](tx, ID(2)) != nil {
							t.Fatal("partial delete or nested write committed")
						}
					})
					if capture {
						requireReplicationID(t, db, 1)
						if len(sink.snapshot()) != 1 {
							t.Fatal("failed delete published")
						}
					}
					for _, g := range sink.snapshot() {
						releaseTestGroup(g)
					}
					if capture {
						replicationAssertClean(t, db)
					}
				})
			}
		}
	}
}
