package edb

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
)

func assertReadObservation(t *testing.T, events []TxObservation, metadata TxMetadata) {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("observations = %v, wanted one", events)
	}
	if event := events[0]; event.Metadata != metadata || event.Writable || event.Duration <= 0 {
		t.Fatalf("observation = %+v, wanted metadata %+v and positive read duration", event, metadata)
	}
}

func TestReadTransactionObservationLifecycle(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, reuse := range []bool{false, true} {
			for _, mode := range []string{"read", "read-error", "read-panic", "tx", "tx-error", "tx-panic", "begin", "begin-panic"} {
				t.Run(fmt.Sprintf("memory=%v/reuse=%v/%s", memory, reuse, mode), func(t *testing.T) {
					var recorder observationRecorder
					var captured *Tx
					observed := 0
					db := observationOpenWithOptions(t, memory, reuse, Options{ObserveReadTransaction: func(event TxObservation) {
						recorder.observe(event)
						if event.Metadata.Kind != "system:test" {
							return
						}
						observed++
						if captured == nil || !captured.closed || captured.valueBufs != nil || captured.indexValueBufs != nil || captured.indexKeyBufs != nil {
							t.Error("observed before wrapper cleanup")
						}
						if captured != nil && captured.BoltTx() != nil && captured.BoltTx().DB() != nil {
							t.Error("observed before Bolt finalization")
						}
					}})
					if events := recorder.take(); len(events) != 0 {
						t.Fatalf("startup read observations = %v", events)
					}
					metadata := TxMetadata{Kind: "system:test", Tenant: "shop"}
					wantErr := errors.New("rejected")
					inspect := func(tx *Tx) {
						captured = tx
						if tx.Metadata() != metadata {
							t.Errorf("metadata = %+v", tx.Metadata())
						}
						if observed != 0 {
							t.Error("observed before callback completed")
						}
					}
					switch {
					case mode == "read":
						db.Read(metadata, inspect)
					case mode == "read-error":
						if err := db.ReadErr(metadata, func(tx *Tx) error { inspect(tx); return wantErr }); err != wantErr {
							t.Fatalf("error = %v", err)
						}
					case mode == "read-panic":
						if p := observationPanic(func() { db.Read(metadata, func(tx *Tx) { inspect(tx); panic(wantErr) }) }); p != wantErr {
							t.Fatalf("panic = %v", p)
						}
					case strings.HasPrefix(mode, "tx"):
						p := observationPanic(func() {
							err := db.Tx(metadata, false, func(tx *Tx) error {
								inspect(tx)
								if mode == "tx-panic" {
									panic(wantErr)
								}
								if mode == "tx-error" {
									return wantErr
								}
								return nil
							})
							if mode == "tx-error" && err != wantErr {
								t.Fatalf("error = %v", err)
							}
							if mode == "tx" && err != nil {
								t.Fatalf("error = %v", err)
							}
						})
						if (mode == "tx-panic" && p != wantErr) || (mode != "tx-panic" && p != nil) {
							t.Fatalf("panic = %v", p)
						}
					case strings.HasPrefix(mode, "begin"):
						p := observationPanic(func() {
							tx := db.BeginRead(metadata)
							defer tx.Close()
							inspect(tx)
							if mode == "begin-panic" {
								panic(wantErr)
							}
						})
						if (mode == "begin-panic" && p != wantErr) || (mode == "begin" && p != nil) {
							t.Fatalf("panic = %v", p)
						}
					}
					captured.Close()
					captured.Close()
					assertTxReuseReleased(t, captured)
					assertReadObservation(t, recorder.take(), metadata)
					if observed != 1 {
						t.Fatalf("observed = %d", observed)
					}
					db.Write(metadata, func(*Tx) {})
					if events := recorder.take(); len(events) != 0 {
						t.Fatalf("write observations = %v", events)
					}
				})
			}
		}
	}
}

func TestReadTransactionObservationFallbackAndAcquisitionFailure(t *testing.T) {
	for _, mode := range []string{"begin-error", "closed"} {
		t.Run(mode, func(t *testing.T) {
			var recorder observationRecorder
			db := observationOpenWithOptions(t, true, true, Options{ObserveReadTransaction: recorder.observe})
			db.Read(TxMetadata{}, func(tx *Tx) {
				if tx.Metadata() != (TxMetadata{}) {
					t.Errorf("metadata = %+v", tx.Metadata())
				}
			})
			assertReadObservation(t, recorder.take(), TxMetadata{Kind: "system:unknown", Tenant: "unknown"})
			base := db.storage
			wantErr := errors.New("begin failed")
			if mode == "closed" {
				db.Close()
			} else {
				db.storage = &observationStorage{storage: base, begin: func(bool) (storageTx, error) {
					return nil, wantErr
				}}
			}
			p := observationPanic(func() { db.BeginRead(TxMetadata{}) })
			if mode == "closed" {
				if p != "database closed" {
					t.Fatalf("panic = %v", p)
				}
			} else if err, ok := p.(error); !ok || !errors.Is(err, wantErr) {
				t.Fatalf("panic = %v", p)
			}
			assertReadObservation(t, recorder.take(), TxMetadata{Kind: "system:unknown", Tenant: "unknown"})
			db.storage = base
		})
	}
}

func TestReadTransactionObservationObserverPanic(t *testing.T) {
	var events atomic.Int32
	var logs atomic.Int32
	db, err := Open(InMemory, basicSchema, Options{
		ObserveReadTransaction: func(event TxObservation) {
			events.Add(1)
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
	metadata := TxMetadata{Kind: "system:panic_test", Tenant: "none"}
	db.Read(metadata, func(*Tx) {})
	wantErr := errors.New("user error")
	if p := observationPanic(func() { db.Read(metadata, func(*Tx) { panic(wantErr) }) }); p != wantErr {
		t.Fatalf("user panic replaced: %v", p)
	}
	db.Read(metadata, func(*Tx) {})
	if events.Load() != 3 || logs.Load() != 1 {
		t.Fatalf("observations=%d logs=%d", events.Load(), logs.Load())
	}
}
