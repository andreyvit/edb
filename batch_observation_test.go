package edb

import (
	"errors"
	"testing"
	"time"
)

func TestBatchObservationCommit(t *testing.T) {
	var (
		events   = make(chan BatchObservation, 4)
		results  = make(chan error, 2)
		rejected = errors.New("check rejected")
		db       = observationOpenWithOptions(t, false, false, Options{TxObserveBatch: func(e BatchObservation) { events <- e }})
	)
	db.Bolt().MaxBatchSize = 2
	db.Bolt().MaxBatchDelay = time.Second
	go func() {
		results <- db.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: 1, Name: "batch", Email: "batch@example.com"}); return nil })
	}()
	go func() { results <- db.Tx(TxMetadata{}, true, func(*Tx) error { return rejected }) }()
	var failures int
	for range 2 {
		switch err := <-results; err {
		case nil:
		case rejected:
			failures++
		default:
			t.Fatal(err)
		}
	}
	if failures != 1 || len(events) != 1 {
		t.Fatalf("failures=%d events=%d", failures, len(events))
	}
	event := <-events
	if event.Outcome != BatchCommitted || event.CallbacksRun != 2 || event.CommitStats == nil || event.CommitStats.PageAllocBytes <= 0 {
		t.Fatalf("commit: %+v", event)
	}
}

func TestBatchObservationAbortRetry(t *testing.T) {
	var (
		events   = make(chan BatchObservation, 4)
		rejected = errors.New("rollback")
		db       = observationOpenWithOptions(t, false, false, Options{TxObserveBatch: func(e BatchObservation) { events <- e; panic("observer") }})
	)
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: 1, Name: "abort", Email: "abort@example.com"}); return rejected })
	if err != rejected || len(events) != 2 {
		t.Fatalf("err=%v events=%d", err, len(events))
	}
	for range 2 {
		event := <-events
		if event.Outcome != BatchCallbackAborted || event.CallbacksRun != 1 || event.CommitStats != nil {
			t.Fatalf("abort: %+v", event)
		}
	}
	if err = db.Tx(TxMetadata{}, true, func(*Tx) error { return nil }); err != nil || len(events) != 1 {
		t.Fatalf("commit after retry: err=%v events=%d", err, len(events))
	}
}
