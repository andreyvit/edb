package edb

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type reuseBatchCall struct {
	role int32
	txs  []*Tx
	rows []*User
	err  error
}

func TestReuseBatchLifecycle(t *testing.T) {
	for _, mode := range []struct {
		name   string
		panic  bool
		commit bool
	}{
		{name: "error_retry"},
		{name: "panic_retry", panic: true},
		{name: "commit_despite_error", commit: true},
		{name: "commit_despite_panic", panic: true, commit: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			db := reuseBatchOpen(t, 2)
			churn := reuseBatchOpen(t, 1)
			rejected := errors.New("reuse batch rejected")
			ready := make(chan struct{}, 1)
			resume := make(chan struct{})
			start := make(chan struct{})
			var order atomic.Int32
			var first atomic.Pointer[Tx]
			var workers sync.WaitGroup
			calls := make([]reuseBatchCall, 2)
			for i := range calls {
				call := &calls[i]
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					call.err = db.Tx(true, func(tx *Tx) error {
						if call.role == 0 {
							call.role = order.Add(1)
						}
						if len(call.txs) > 0 {
							prior := call.txs[len(call.txs)-1]
							if err := reuseBatchRetained(prior); err != nil {
								return err
							}
							if prior.BoltTx() == tx.BoltTx() || prior.BoltTx().DB() != nil {
								return errors.New("retry did not replace the rolled-back Bolt transaction")
							}
						}
						call.txs = append(call.txs, tx)
						id := ID(len(call.txs))
						if call.role == 2 {
							id = 100
						}
						row := reuseBatchUser(id)
						call.rows = append(call.rows, row)
						Put(tx, row)
						if call.role == 1 {
							if len(call.txs) == 1 {
								first.Store(tx)
							}
							return reuseBatchRows(tx, []*User{row}, nil)
						}
						prior := first.Load()
						if prior == nil || prior.BoltTx() != tx.BoltTx() {
							return errors.New("callers did not share a Bolt transaction")
						}
						if err := reuseBatchRetained(prior); err != nil {
							return err
						}
						ready <- struct{}{}
						select {
						case <-resume:
						case <-time.After(10 * time.Second):
							return errors.New("pool churn gate timed out")
						}
						if err := reuseBatchRetained(prior); err != nil {
							return err
						}
						if err := reuseBatchRows(tx, []*User{reuseBatchUser(1), row}, nil); err != nil {
							return err
						}
						if mode.commit {
							tx.CommitDespiteError()
						}
						if mode.panic {
							panic(rejected)
						}
						return rejected
					})
				}()
			}
			done := make(chan struct{})
			go func() {
				workers.Wait()
				close(done)
			}()
			var release sync.Once
			defer func() {
				release.Do(func() { close(resume) })
				<-done
			}()
			close(start)
			select {
			case <-ready:
			case <-done:
				t.Fatalf("batch ended before churn gate: caller errors: %v; %v", calls[0].err, calls[1].err)
			case <-time.After(10 * time.Second):
				release.Do(func() { close(resume) })
				<-done
				t.Fatalf("shared batch missed churn gate: caller errors: %v; %v", calls[0].err, calls[1].err)
			}
			reuseBatchChurn(t, churn)
			release.Do(func() { close(resume) })
			<-done

			var accepted, absent []*User
			for _, call := range calls {
				for _, tx := range call.txs {
					reuseBatchReleased(t, tx)
				}
				if call.role == 1 {
					attempts := 2
					if mode.commit {
						attempts = 1
					}
					if call.err != nil || len(call.txs) != attempts {
						t.Fatalf("successful caller: attempts=%d, want=%d, error=%v", len(call.txs), attempts, call.err)
					}
					accepted = append(accepted, call.rows[len(call.rows)-1])
					absent = append(absent, call.rows[:len(call.rows)-1]...)
				} else {
					if len(call.txs) != 1 {
						t.Fatalf("failed callback ran %d times", len(call.txs))
					}
					if mode.panic {
						var p panicked
						if !errors.As(call.err, &p) {
							t.Fatalf("panic result: %v", call.err)
						}
						if reason, ok := p.reason.(error); !ok || !errors.Is(reason, rejected) {
							t.Fatalf("panic reason: %v, wanted %v", p.reason, rejected)
						}
					} else if !errors.Is(call.err, rejected) {
						t.Fatalf("error result: %v", call.err)
					}
					if mode.commit {
						accepted = append(accepted, call.rows...)
					} else {
						absent = append(absent, call.rows...)
					}
				}
			}
			reuseBatchVerify(t, db, accepted, absent)
		})
	}
}

func reuseBatchOpen(t *testing.T, size int) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "reuse.db"), basicSchema, Options{IsTesting: true, ReuseTxBuffers: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if db.Bolt() == nil || !db.reuseTxBuffers {
		t.Fatal("test requires Bolt and buffer reuse")
	}
	db.Bolt().MaxBatchSize = size
	db.Bolt().MaxBatchDelay = time.Minute
	return db
}

func reuseBatchUser(id ID) *User {
	return &User{ID: id, Email: fmt.Sprintf("reuse-%03d@example.com", id), Name: fmt.Sprintf("reuse-name-%03d", id)}
}

func reuseBatchRetained(tx *Tx) error {
	if !tx.closed || !tx.managed {
		return errors.New("prior managed wrapper is not closed")
	}
	if len(tx.valueBufs) == 0 || len(tx.indexValueBufs) == 0 || len(tx.indexKeyBufs) == 0 ||
		tx.valueBufsOwner == nil || tx.indexValueBufsOwner == nil || tx.indexKeyBufsOwner == nil {
		return errors.New("managed wrapper released buffers before Batch returned")
	}
	return nil
}

func reuseBatchReleased(t *testing.T, tx *Tx) {
	t.Helper()
	if !tx.closed || tx.valueBufs != nil || tx.indexValueBufs != nil || tx.indexKeyBufs != nil ||
		tx.valueBufsOwner != nil || tx.indexValueBufsOwner != nil || tx.indexKeyBufsOwner != nil {
		t.Error("wrapper did not close and release all buffer lists")
	}
	if tx.BoltTx() == nil || tx.BoltTx().DB() != nil {
		t.Error("underlying Bolt transaction remains open")
	}
}

func reuseBatchChurn(t *testing.T, db *DB) {
	t.Helper()
	var accepted, absent []*User
	rejected := errors.New("churn rejected")
	for i := range ID(16) {
		row := reuseBatchUser(200 + i)
		var wrappers []*Tx
		err := db.Tx(true, func(tx *Tx) error {
			wrappers = append(wrappers, tx)
			Put(tx, row)
			All(IndexScan[User](tx, usersByName, ExactScan(row.Name)))
			if i%2 != 0 {
				return rejected
			}
			return nil
		})
		for _, tx := range wrappers {
			reuseBatchReleased(t, tx)
		}
		if i%2 == 0 {
			if err != nil {
				t.Fatal(err)
			}
			accepted = append(accepted, row)
		} else {
			if !errors.Is(err, rejected) {
				t.Fatalf("churn rejection: %v", err)
			}
			absent = append(absent, row)
		}
	}
	reuseBatchVerify(t, db, accepted, absent)
}

func reuseBatchVerify(t *testing.T, db *DB, accepted, absent []*User) {
	t.Helper()
	var wrapper *Tx
	err := db.Tx(false, func(tx *Tx) error {
		wrapper = tx
		return reuseBatchRows(tx, accepted, absent)
	})
	if err != nil {
		t.Error(err)
	}
	reuseBatchReleased(t, wrapper)
}

func reuseBatchRows(tx *Tx, accepted, absent []*User) error {
	if n := len(All(TableScan[User](tx, FullScan()))); n != len(accepted) {
		return fmt.Errorf("row count=%d, want=%d", n, len(accepted))
	}
	for _, idx := range []*Index{usersByEmail, usersByName} {
		if n := len(All(IndexScan[User](tx, idx, FullScan()))); n != len(accepted) {
			return fmt.Errorf("index %s count=%d, want=%d", idx.FullName(), n, len(accepted))
		}
	}
	for _, row := range accepted {
		if got := Get[User](tx, row.ID); !reflect.DeepEqual(got, row) {
			return fmt.Errorf("row %d: got=%+v, want=%+v", row.ID, got, row)
		}
		for idx, key := range map[*Index]string{usersByEmail: row.Email, usersByName: row.Name} {
			if got := All(IndexScan[User](tx, idx, ExactScan(key))); !reflect.DeepEqual(got, []*User{row}) {
				return fmt.Errorf("index %s key %q: got=%v, want=%+v", idx.FullName(), key, got, row)
			}
		}
	}
	for _, row := range absent {
		if Get[User](tx, row.ID) != nil {
			return fmt.Errorf("rejected row %d persisted", row.ID)
		}
		for idx, key := range map[*Index]string{usersByEmail: row.Email, usersByName: row.Name} {
			if LookupExists(tx, idx, key) {
				return fmt.Errorf("rejected row %d retained index %s", row.ID, idx.FullName())
			}
		}
	}
	return nil
}
