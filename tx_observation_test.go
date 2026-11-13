package edb

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type observationRecorder struct {
	mu     sync.Mutex
	events []TxObservation
}

func (r *observationRecorder) observe(event TxObservation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *observationRecorder) take() []TxObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	events := r.events
	r.events = nil
	return events
}

func observationOpen(t *testing.T, memory, reuse bool, observe func(TxObservation)) *DB {
	t.Helper()
	path := InMemory
	if !memory {
		path = filepath.Join(t.TempDir(), "observed.db")
	}
	db, err := Open(path, basicSchema, Options{IsTesting: true, ReuseTxBuffers: reuse, ObserveTransaction: observe})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if db.Bolt() != nil {
		db.Bolt().MaxBatchSize = 1
	}
	return db
}

func observationPanic(f func()) (result any) {
	defer func() { result = recover() }()
	f()
	return nil
}

func assertObservation(t *testing.T, events []TxObservation, metadata TxMetadata) {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("observations = %v, wanted one", events)
	}
	if event := events[0]; event.Metadata != metadata || !event.Writable || event.Duration <= 0 {
		t.Fatalf("observation = %+v, wanted metadata %+v and positive write duration", event, metadata)
	}
}

func TestTransactionObservationLifecycle(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, reuse := range []bool{false, true} {
			for _, mode := range []string{"write", "write-panic", "success", "noop", "error", "check-error", "panic", "commit-despite-error", "commit-despite-panic", "begin-commit", "begin-close", "begin-noop", "begin-panic"} {
				t.Run(fmt.Sprintf("memory=%v/reuse=%v/%s", memory, reuse, mode), func(t *testing.T) {
					var recorder observationRecorder
					var captured *Tx
					observed := 0
					db := observationOpen(t, memory, reuse, func(event TxObservation) {
						recorder.observe(event)
						if event.Metadata.Kind != "system:test" {
							return
						}
						observed++
						if mode == "begin-commit" {
							if captured.closed || (reuse && len(captured.valueBufs) == 0) {
								t.Error("direct Commit released wrapper before observation")
							}
						} else if !captured.closed || captured.valueBufs != nil || captured.indexValueBufs != nil || captured.indexKeyBufs != nil {
							t.Error("observed before wrapper cleanup")
						}
						if captured.BoltTx() != nil && captured.BoltTx().DB() != nil {
							t.Error("observed before Bolt finalization")
						}
					})
					assertObservation(t, recorder.take(), TxMetadata{Kind: "startup:edb_schema", Tenant: "none"})
					metadata := TxMetadata{Kind: "system:test", Tenant: "shop"}
					wantErr := errors.New("rejected")
					committed := mode == "write" || mode == "success" || mode == "begin-commit" || strings.HasPrefix(mode, "commit-despite-")
					mutated := mode != "noop" && mode != "check-error" && mode != "begin-noop"
					mutate := func(tx *Tx) {
						captured = tx
						if tx.Metadata() != metadata {
							t.Errorf("metadata = %+v", tx.Metadata())
						}
						copy := tx.Metadata()
						copy.Kind = "changed"
						if tx.Metadata() != metadata {
							t.Error("metadata accessor is mutable")
						}
						if mutated {
							Put(tx, &User{ID: 1, Name: "observed", Email: "observed@example.com"})
						}
						if observed != 0 {
							t.Error("observed before callback completed")
						}
					}
					switch {
					case strings.HasPrefix(mode, "begin-"):
						p := observationPanic(func() {
							tx := db.BeginUpdate(metadata)
							defer tx.Close()
							mutate(tx)
							if mode == "begin-panic" {
								panic(wantErr)
							}
							if committed {
								if err := tx.Commit(); err != nil {
									t.Fatal(err)
								}
								if observed != 1 {
									t.Error("Commit did not observe synchronously")
								}
								_ = tx.Commit()
							}
						})
						if (mode == "begin-panic" && p != wantErr) || (mode != "begin-panic" && p != nil) {
							t.Fatalf("panic = %v", p)
						}
					case strings.HasPrefix(mode, "write"):
						p := observationPanic(func() {
							db.Write(metadata, func(tx *Tx) {
								mutate(tx)
								if mode == "write-panic" {
									panic(wantErr)
								}
							})
						})
						if (mode == "write-panic" && p != wantErr) || (mode == "write" && p != nil) {
							t.Fatalf("panic = %v", p)
						}
					default:
						err := db.Tx(metadata, true, func(tx *Tx) error {
							mutate(tx)
							if strings.HasPrefix(mode, "commit-despite-") {
								tx.CommitDespiteError()
							}
							if strings.HasSuffix(mode, "panic") {
								panic(wantErr)
							}
							if strings.HasSuffix(mode, "error") {
								return wantErr
							}
							return nil
						})
						if strings.HasSuffix(mode, "panic") {
							var p panicked
							if !errors.As(err, &p) || p.reason != wantErr {
								t.Fatalf("panic error = %v", err)
							}
						} else if strings.HasSuffix(mode, "error") {
							if err != wantErr {
								t.Fatalf("error = %v", err)
							}
						} else if err != nil {
							t.Fatal(err)
						}
					}
					captured.Close()
					captured.Close()
					assertTxReuseReleased(t, captured)
					assertObservation(t, recorder.take(), metadata)
					if observed != 1 || db.PendingWriterCount.Load() != 0 {
						t.Fatalf("observed=%d pending=%d", observed, db.PendingWriterCount.Load())
					}
					db.Read(func(tx *Tx) {
						if (Get[User](tx, ID(1)) != nil) != (committed && mutated) {
							t.Error("unexpected persisted row")
						}
					})
					_ = db.ReadErr(func(*Tx) error { return wantErr })
					_ = db.Tx(TxMetadata{}, false, func(tx *Tx) error {
						if tx.Metadata() != (TxMetadata{}) {
							t.Error("read metadata normalized")
						}
						return wantErr
					})
					if events := recorder.take(); len(events) != 0 {
						t.Fatalf("read observations = %v", events)
					}
				})
			}
		}
	}
}

func TestTransactionObservationBatchRetry(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(fmt.Sprint(reuse), func(t *testing.T) {
			var recorder observationRecorder
			db := observationOpen(t, false, reuse, recorder.observe)
			recorder.take()
			db.Bolt().MaxBatchSize = 2
			db.Bolt().MaxBatchDelay = time.Second
			type call struct {
				metadata  TxMetadata
				role      int32
				txs       []*Tx
				memoCalls int
				err       error
			}
			calls := []call{{metadata: TxMetadata{Kind: "system:first", Tenant: "none"}}, {metadata: TxMetadata{Kind: "system:second", Tenant: "mixed"}}}
			var order atomic.Int32
			var wg sync.WaitGroup
			rejected := errors.New("rejected")
			for i := range calls {
				call := &calls[i]
				wg.Add(1)
				go func() {
					defer wg.Done()
					call.err = db.Tx(call.metadata, true, func(tx *Tx) error {
						if call.role == 0 {
							call.role = order.Add(1)
						}
						call.txs = append(call.txs, tx)
						if tx.Metadata() != call.metadata {
							return errors.New("retry metadata changed")
						}
						_, err := tx.Memo("once", func() (any, error) { call.memoCalls++; return 7, nil })
						if err != nil {
							return err
						}
						Put(tx, &User{ID: ID(call.role), Name: fmt.Sprint(call.role), Email: fmt.Sprint(call.role)})
						if call.role == 2 {
							return rejected
						}
						return nil
					})
				}()
			}
			wg.Wait()
			events := recorder.take()
			if len(events) != 2 || calls[0].txs[0].BoltTx() != calls[1].txs[0].BoltTx() {
				t.Fatalf("not one shared batch: events=%v", events)
			}
			for _, call := range calls {
				if call.memoCalls != 1 || (call.role == 1 && (call.err != nil || len(call.txs) != 2)) || (call.role == 2 && (call.err != rejected || len(call.txs) != 1)) {
					t.Fatalf("call = %+v", call)
				}
				for _, tx := range call.txs {
					assertTxReuseReleased(t, tx)
					if tx.BoltTx().DB() != nil {
						t.Fatal("batch storage still open")
					}
				}
				var matching []TxObservation
				for _, event := range events {
					if event.Metadata == call.metadata {
						matching = append(matching, event)
					}
				}
				assertObservation(t, matching, call.metadata)
			}
			db.Read(func(tx *Tx) {
				if Get[User](tx, ID(1)) == nil || Get[User](tx, ID(2)) != nil {
					t.Error("retry persisted wrong rows")
				}
			})
		})
	}
}

func TestTransactionObservationFallbackAndShutdown(t *testing.T) {
	for _, memory := range []bool{false, true} {
		t.Run(fmt.Sprint(memory), func(t *testing.T) {
			var recorder observationRecorder
			db := observationOpen(t, memory, true, recorder.observe)
			assertObservation(t, recorder.take(), TxMetadata{Kind: "startup:edb_schema", Tenant: "none"})
			for _, test := range []struct{ metadata, want TxMetadata }{
				{TxMetadata{}, TxMetadata{Kind: "system:unknown", Tenant: "unknown"}},
				{TxMetadata{Kind: "system:test"}, TxMetadata{Kind: "system:test", Tenant: "unknown"}},
				{TxMetadata{Tenant: "none"}, TxMetadata{Kind: "system:unknown", Tenant: "none"}},
				{TxMetadata{Tenant: "mixed"}, TxMetadata{Kind: "system:unknown", Tenant: "mixed"}},
				{TxMetadata{Tenant: "shop"}, TxMetadata{Kind: "system:unknown", Tenant: "shop"}},
			} {
				metadata, want := test.metadata, test.want
				db.Write(metadata, func(tx *Tx) {
					if tx.Metadata() != want {
						t.Errorf("metadata = %+v, wanted %+v", tx.Metadata(), want)
					}
				})
				assertObservation(t, recorder.take(), want)
			}
			metadata := TxMetadata{Kind: "system:original", Tenant: "shop"}
			tx := db.BeginUpdate(metadata)
			metadata.Kind = "system:changed"
			metadata.Tenant = "changed"
			tx.Close()
			assertObservation(t, recorder.take(), TxMetadata{Kind: "system:original", Tenant: "shop"})
			callbackCalled := false
			db.CloseWithSafeToQuitCallback(func() {
				callbackCalled = true
				events := recorder.take()
				if memory {
					if len(events) != 0 {
						t.Errorf("memory shutdown observations = %v", events)
					}
				} else {
					assertObservation(t, events, TxMetadata{Kind: "system:edb_shutdown_freelist", Tenant: "none"})
				}
			})
			db.Close()
			if !callbackCalled || len(recorder.take()) != 0 {
				t.Error("shutdown callback or repeated Close observations")
			}
		})
	}
}

func TestTransactionObservationObserverPanic(t *testing.T) {
	var recorder observationRecorder
	var logs atomic.Int32
	db, err := Open(InMemory, basicSchema, Options{
		ObserveTransaction: func(event TxObservation) {
			recorder.observe(event)
			panic("observer")
		},
		Logf: func(format string, args ...any) {
			if strings.Contains(format, "observer panicked") {
				logs.Add(1)
				panic("logger")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	recorder.take()
	wantErr := errors.New("user error")
	metadata := TxMetadata{Kind: "system:panic_test", Tenant: "none"}
	if p := observationPanic(func() { db.Write(metadata, func(*Tx) { panic(wantErr) }) }); p != wantErr {
		t.Fatalf("user panic replaced: %v", p)
	}
	if err := db.Tx(metadata, true, func(*Tx) error { return wantErr }); err != wantErr {
		t.Fatalf("user error replaced: %v", err)
	}
	var recovered panicked
	if err := db.Tx(metadata, true, func(*Tx) error { panic(wantErr) }); !errors.As(err, &recovered) || recovered.reason != wantErr {
		t.Fatalf("recovered user panic replaced: %v", err)
	}
	db.Write(metadata, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "committed"}) })
	tx := db.BeginUpdate(metadata)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx.Close()
	db.BeginUpdate(metadata).Close()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.Write(metadata, func(*Tx) {})
		}()
	}
	wg.Wait()
	if events := recorder.take(); len(events) != 14 || logs.Load() != 1 {
		t.Fatalf("observations=%d logs=%d", len(events), logs.Load())
	}
	db.Read(func(tx *Tx) {
		if Get[User](tx, ID(1)) == nil {
			t.Error("observer panic rolled back successful write")
		}
	})
}

type observationStorage struct {
	storage
	begin func(bool) (storageTx, error)
}

func (s *observationStorage) BeginTx(writable bool) (storageTx, error) {
	return s.begin(writable)
}

type observationStorageTx struct {
	storageTx
	commit   func() error
	rollback func() error
}

func (tx *observationStorageTx) Commit() error { return tx.commit() }
func (tx *observationStorageTx) Rollback() error {
	if tx.rollback != nil {
		return tx.rollback()
	}
	return tx.storageTx.Rollback()
}

func TestTransactionObservationAcquisitionFailures(t *testing.T) {
	for _, entry := range []string{"tx", "write", "begin"} {
		for _, mode := range []string{"closed", "begin-error", "close-during-acquisition"} {
			t.Run(entry+"/"+mode, func(t *testing.T) {
				var recorder observationRecorder
				db := observationOpen(t, true, true, recorder.observe)
				recorder.take()
				base := db.storage
				wantErr := errors.New("begin failed")
				var acquired *memTx
				if mode == "closed" {
					db.Close()
				} else {
					db.storage = &observationStorage{storage: base, begin: func(writable bool) (storageTx, error) {
						if mode == "begin-error" {
							return nil, wantErr
						}
						stx, err := base.BeginTx(writable)
						if err != nil {
							return nil, err
						}
						acquired = stx.(*memTx)
						db.closed.Store(true)
						return stx, nil
					}}
				}
				var result error
				p := observationPanic(func() {
					switch entry {
					case "tx":
						result = db.Tx(TxMetadata{}, true, func(*Tx) error { t.Error("callback called"); return nil })
					case "write":
						db.Write(TxMetadata{}, func(*Tx) { t.Error("callback called") })
					case "begin":
						db.BeginUpdate(TxMetadata{}).Close()
					}
				})
				if mode == "close-during-acquisition" {
					db.closed.Store(false)
					if acquired == nil || !acquired.closed || acquired.base.writer {
						t.Fatal("provisional storage transaction leaked")
					}
				}
				if mode == "begin-error" && entry == "tx" {
					if p != nil || result != wantErr {
						t.Fatalf("error=%v panic=%v", result, p)
					}
				} else if p == nil {
					t.Fatal("missing acquisition panic")
				}
				assertObservation(t, recorder.take(), TxMetadata{Kind: "system:unknown", Tenant: "unknown"})
				db.storage = base
			})
		}
	}
}

func TestTransactionObservationCommitFailure(t *testing.T) {
	for _, entry := range []string{"tx", "write", "begin"} {
		for _, panicCommit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/panic=%v", entry, panicCommit), func(t *testing.T) {
				var recorder observationRecorder
				db := observationOpen(t, true, true, recorder.observe)
				recorder.take()
				base := db.storage
				wantErr := errors.New("commit failed")
				var acquired storageTx
				db.storage = &observationStorage{storage: base, begin: func(writable bool) (storageTx, error) {
					stx, err := base.BeginTx(writable)
					acquired = stx
					return &observationStorageTx{storageTx: stx, commit: func() error {
						if panicCommit {
							panic(wantErr)
						}
						_ = stx.Rollback()
						return wantErr
					}}, err
				}}
				var result error
				p := observationPanic(func() {
					switch entry {
					case "tx":
						result = db.Tx(TxMetadata{}, true, func(*Tx) error { return nil })
					case "write":
						db.Write(TxMetadata{}, func(*Tx) {})
					case "begin":
						tx := db.BeginUpdate(TxMetadata{})
						defer tx.Close()
						commitPanic := observationPanic(func() { result = tx.Commit() })
						if panicCommit {
							if events := recorder.take(); len(events) != 0 {
								t.Error("nonterminal Commit panic observed before rollback")
							}
							panic(commitPanic)
						}
						assertObservation(t, recorder.take(), TxMetadata{Kind: "system:unknown", Tenant: "unknown"})
					}
				})
				if panicCommit {
					if p != wantErr {
						t.Fatalf("commit panic = %v", p)
					}
				} else if entry == "write" {
					if err, ok := p.(error); !ok || !errors.Is(err, wantErr) {
						t.Fatalf("commit panic = %v", p)
					}
				} else if p != nil || result != wantErr {
					t.Fatalf("commit error=%v panic=%v", result, p)
				}
				events := recorder.take()
				if entry == "begin" && !panicCommit {
					if len(events) != 0 {
						t.Error("Close repeated failed Commit observation")
					}
				} else {
					assertObservation(t, events, TxMetadata{Kind: "system:unknown", Tenant: "unknown"})
				}
				if !acquired.(*memTx).closed {
					t.Error("failed commit leaked storage transaction")
				}
				db.storage = base
			})
		}
	}
}

func TestTransactionObservationInitializationFailure(t *testing.T) {
	var recorder observationRecorder
	base := newMemStorage()
	defer base.Close()
	wantErr := errors.New("initialization commit failed")
	storage := &observationStorage{storage: base, begin: func(writable bool) (storageTx, error) {
		stx, err := base.BeginTx(writable)
		return &observationStorageTx{storageTx: stx, commit: func() error {
			_ = stx.Rollback()
			return wantErr
		}}, err
	}}
	p := observationPanic(func() { _, _ = openWithStorage(storage, basicSchema, Options{ObserveTransaction: recorder.observe}) })
	if err, ok := p.(error); !ok || !errors.Is(err, wantErr) {
		t.Fatalf("initialization panic = %v", p)
	}
	assertObservation(t, recorder.take(), TxMetadata{Kind: "startup:edb_schema", Tenant: "none"})
	if base.(*memStorage).writer {
		t.Fatal("initialization leaked writer")
	}
}

func TestTransactionObservationTiming(t *testing.T) {
	for _, entry := range []string{"tx", "write", "begin-commit", "begin-close"} {
		t.Run(entry, func(t *testing.T) {
			var event TxObservation
			var observerElapsed time.Duration
			db := observationOpen(t, true, true, func(observed TxObservation) {
				if observed.Metadata.Kind == "system:timing" {
					event = observed
					start := time.Now()
					time.Sleep(15 * time.Millisecond)
					observerElapsed = time.Since(start)
				}
			})
			base := db.storage
			var acquisitionStarted, finalizationEnded time.Time
			db.storage = &observationStorage{storage: base, begin: func(writable bool) (storageTx, error) {
				acquisitionStarted = time.Now()
				time.Sleep(15 * time.Millisecond)
				stx, err := base.BeginTx(writable)
				return &observationStorageTx{storageTx: stx, commit: func() error {
					result := stx.Commit()
					time.Sleep(15 * time.Millisecond)
					finalizationEnded = time.Now()
					return result
				}, rollback: func() error {
					result := stx.Rollback()
					if entry != "begin-commit" {
						time.Sleep(15 * time.Millisecond)
						finalizationEnded = time.Now()
					}
					return result
				}}, err
			}}
			metadata := TxMetadata{Kind: "system:timing", Tenant: "none"}
			start := time.Now()
			switch entry {
			case "tx":
				if err := db.Tx(metadata, true, func(*Tx) error { return nil }); err != nil {
					t.Fatal(err)
				}
			case "write":
				db.Write(metadata, func(*Tx) {})
			default:
				tx := db.BeginUpdate(metadata)
				if entry == "begin-commit" {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				tx.Close()
			}
			elapsed := time.Since(start)
			if event.Duration < finalizationEnded.Sub(acquisitionStarted) || event.Duration > elapsed-observerElapsed {
				t.Fatalf("duration=%v acquisition/finalization=%v total=%v observer=%v", event.Duration, finalizationEnded.Sub(acquisitionStarted), elapsed, observerElapsed)
			}
			db.storage = base
		})
	}
}

func TestTransactionObservationReleasesWriterBeforeObserver(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, entry := range []string{"tx", "write", "begin-commit", "begin-close"} {
			t.Run(fmt.Sprintf("memory=%v/%s", memory, entry), func(t *testing.T) {
				entered := make(chan struct{})
				resume := make(chan struct{})
				db := observationOpen(t, memory, true, func(event TxObservation) {
					if event.Metadata.Kind == "system:blocked_observer" {
						close(entered)
						<-resume
					}
				})
				metadata := TxMetadata{Kind: "system:blocked_observer", Tenant: "none"}
				done := make(chan struct{})
				go func() {
					defer close(done)
					switch entry {
					case "tx":
						if err := db.Tx(metadata, true, func(*Tx) error { return nil }); err != nil {
							t.Error(err)
						}
					case "write":
						db.Write(metadata, func(*Tx) {})
					default:
						tx := db.BeginUpdate(metadata)
						defer tx.Close()
						if entry == "begin-commit" {
							if err := tx.Commit(); err != nil {
								t.Error(err)
							}
						}
					}
				}()
				<-entered
				select {
				case <-done:
					t.Error("request returned before synchronous observation")
				default:
				}
				acquired := make(chan error, 1)
				go func() {
					stx, err := db.storage.BeginTx(true)
					if err == nil {
						err = stx.Rollback()
					}
					acquired <- err
				}()
				select {
				case err := <-acquired:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("observer retained writer lock")
				}
				close(resume)
				<-done
			})
		}
	}
}

func TestTransactionObservationBoltBeginFailure(t *testing.T) {
	for _, entry := range []string{"tx", "write", "begin"} {
		t.Run(entry, func(t *testing.T) {
			var recorder observationRecorder
			db := observationOpen(t, false, true, recorder.observe)
			recorder.take()
			if err := db.Bolt().Close(); err != nil {
				t.Fatal(err)
			}
			var result error
			p := observationPanic(func() {
				switch entry {
				case "tx":
					result = db.Tx(TxMetadata{}, true, func(*Tx) error { t.Error("callback called"); return nil })
				case "write":
					db.Write(TxMetadata{}, func(*Tx) { t.Error("callback called") })
				case "begin":
					db.BeginUpdate(TxMetadata{}).Close()
				}
			})
			if entry == "tx" {
				if p != nil || result == nil {
					t.Fatalf("error=%v panic=%v", result, p)
				}
			} else if p == nil {
				t.Fatal("missing begin panic")
			}
			assertObservation(t, recorder.take(), TxMetadata{Kind: "system:unknown", Tenant: "unknown"})
			if db.PendingWriterCount.Load() != 0 {
				t.Fatal("pending writer count leaked")
			}
			db.Close()
			assertObservation(t, recorder.take(), TxMetadata{Kind: "system:edb_shutdown_freelist", Tenant: "none"})
		})
	}
}

func TestTransactionObservationBatchQueueTiming(t *testing.T) {
	var recorder observationRecorder
	db := observationOpen(t, false, true, recorder.observe)
	recorder.take()
	db.Bolt().MaxBatchSize = 2
	db.Bolt().MaxBatchDelay = time.Second
	done := make(chan error, 2)
	go func() {
		done <- db.Tx(TxMetadata{Kind: "system:queued", Tenant: "none"}, true, func(*Tx) error { return nil })
	}()
	deadline := time.Now().Add(5 * time.Second)
	for db.PendingWriterCount.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("writer did not enter queue")
		}
		time.Sleep(time.Millisecond)
	}
	queuedAt := time.Now()
	time.Sleep(15 * time.Millisecond)
	peerEntered := make(chan struct{})
	peerResume := make(chan struct{})
	go func() {
		done <- db.Tx(TxMetadata{Kind: "system:peer", Tenant: "none"}, true, func(*Tx) error {
			close(peerEntered)
			<-peerResume
			return nil
		})
	}()
	<-peerEntered
	premature := recorder.take()
	time.Sleep(15 * time.Millisecond)
	peerFinished := time.Now()
	close(peerResume)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if len(premature) != 0 {
		t.Fatalf("observed before peer finalization: %v", premature)
	}
	events := recorder.take()
	if len(events) != 2 {
		t.Fatalf("observations = %v", events)
	}
	for _, event := range events {
		if event.Metadata.Kind == "system:queued" && event.Duration < peerFinished.Sub(queuedAt) {
			t.Fatalf("duration=%v excludes queue/peer interval=%v", event.Duration, peerFinished.Sub(queuedAt))
		}
	}
}
