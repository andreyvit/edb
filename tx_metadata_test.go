package edb

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadMetadata(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, capture := range []bool{false, true} {
			for _, want := range []TxMetadata{{}, {Kind: "system:read", Tenant: "shop"}} {
				for _, entry := range []string{"begin", "read", "read-err", "tx"} {
					t.Run(fmt.Sprintf("memory=%v/capture=%v/%+v/%s", memory, capture, want, entry), func(t *testing.T) {
						path := InMemory
						if !memory {
							path = filepath.Join(t.TempDir(), "metadata.db")
						}
						var recorder observationRecorder
						db, err := Open(path, basicSchema, Options{IsTesting: true, CaptureTxStack: capture, ObserveTransaction: recorder.observe})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(db.Close)
						recorder.take()
						metadata := want
						var captured *Tx
						check := func(tx *Tx) {
							captured = tx
							metadata.Kind = "system:changed"
							metadata.Tenant = "changed"
							copy := tx.Metadata()
							copy.Kind = "system:copy"
							copy.Tenant = "copy"
							if got := tx.Metadata(); got != want || tx.IsWritable() {
								t.Errorf("metadata = %+v, writable = %v; wanted %+v, false", got, tx.IsWritable(), want)
							}
							if capture {
								if !strings.Contains(string(tx.stack), "TestReadMetadata") {
									t.Errorf("missing read origin stack: %s", tx.stack)
								}
							} else if len(tx.stack) != 0 {
								t.Error("captured origin stack without opt-in")
							}
						}
						wantErr := errors.New("read failed")
						switch entry {
						case "begin":
							tx := db.BeginRead(metadata)
							defer tx.Close()
							check(tx)
							tx.Close()
						case "read":
							db.Read(metadata, check)
						case "read-err":
							err = db.ReadErr(metadata, func(tx *Tx) error { check(tx); return wantErr })
						case "tx":
							err = db.Tx(metadata, false, func(tx *Tx) error { check(tx); return wantErr })
						}
						if (entry == "read-err" || entry == "tx") && err != wantErr {
							t.Errorf("error = %v, wanted %v", err, wantErr)
						}
						if captured == nil || !captured.closed || db.DescribeOpenTxns() != "NO OPEN TRANSACTIONS" {
							t.Fatal("read transaction missing or leaked")
						}
						if events := recorder.take(); len(events) != 0 {
							t.Fatalf("read emitted write observations: %v", events)
						}
					})
				}
			}
		}
	}
}

func TestReadPanicCleanup(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, entry := range []string{"read", "read-err", "tx"} {
			t.Run(fmt.Sprintf("memory=%v/%s", memory, entry), func(t *testing.T) {
				db := observationOpen(t, memory, true, nil)
				metadata := TxMetadata{Kind: "system:read_panic", Tenant: "shop"}
				wantPanic := errors.New("read panic")
				var captured *Tx
				fail := func(tx *Tx) {
					captured = tx
					if tx.Metadata() != metadata {
						t.Errorf("metadata = %+v, wanted %+v", tx.Metadata(), metadata)
					}
					panic(wantPanic)
				}
				got := observationPanic(func() {
					switch entry {
					case "read":
						db.Read(metadata, fail)
					case "read-err":
						_ = db.ReadErr(metadata, func(tx *Tx) error { fail(tx); return nil })
					case "tx":
						_ = db.Tx(metadata, false, func(tx *Tx) error { fail(tx); return nil })
					}
				})
				if got != wantPanic {
					t.Fatalf("panic = %v, wanted %v", got, wantPanic)
				}
				if captured == nil || !captured.closed || db.DescribeOpenTxns() != "NO OPEN TRANSACTIONS" {
					t.Fatal("panicking read leaked its transaction")
				}
			})
		}
	}
}

func TestDBTxPanicStackWithoutOriginCapture(t *testing.T) {
	for _, memory := range []bool{false, true} {
		for _, metadata := range []TxMetadata{{}, {Kind: "system:write_panic", Tenant: "shop"}} {
			t.Run(fmt.Sprintf("memory=%v/%+v", memory, metadata), func(t *testing.T) {
				db := observationOpen(t, memory, true, nil)
				wantPanic := errors.New("write panic")
				var captured *Tx
				err := db.Tx(metadata, true, func(tx *Tx) error {
					captured = tx
					if len(tx.stack) != 0 {
						t.Error("captured origin stack without opt-in")
					}
					panic(wantPanic)
				})
				var p panicked
				if !errors.As(err, &p) || p.reason != wantPanic {
					t.Fatalf("error = %v, wanted recovered %v", err, wantPanic)
				}
				for _, frame := range []string{"runtime/debug.Stack", "safelyCall", "TestDBTxPanicStackWithoutOriginCapture"} {
					if !strings.Contains(p.stack, frame) {
						t.Errorf("panic stack missing %q: %s", frame, p.stack)
					}
				}
				if !strings.Contains(err.Error(), p.stack) {
					t.Error("panic error omitted stack")
				}
				if captured == nil || !captured.closed || db.DescribeOpenTxns() != "NO OPEN TRANSACTIONS" {
					t.Fatal("panicking write leaked its transaction")
				}
			})
		}
	}
}
