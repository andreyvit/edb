package edb

import (
	"log/slog"
	"time"
)

type TxMetadata struct {
	Kind   string
	Tenant string
}

type TxObservation struct {
	Metadata TxMetadata
	Writable bool
	Duration time.Duration
}

type transactionObservation struct {
	db       *DB
	observer func(TxObservation)
	metadata TxMetadata
	start    time.Time
	writable bool
}

func (metadata TxMetadata) withDefaults() TxMetadata {
	if metadata.Kind == "" {
		metadata.Kind = "system:unknown"
	}
	if metadata.Tenant == "" {
		metadata.Tenant = "unknown"
	}
	return metadata
}

func (db *DB) startObservation(metadata TxMetadata, writable bool) transactionObservation {
	observer := db.observeReadTransaction
	if writable {
		observer = db.observeWriteTransaction
	}
	if observer == nil {
		return transactionObservation{}
	}
	return transactionObservation{
		db:       db,
		observer: observer,
		metadata: metadata.withDefaults(),
		start:    time.Now(),
		writable: writable,
	}
}

func (observation *transactionObservation) finish() {
	db := observation.db
	if db == nil {
		return
	}
	observer := observation.observer
	observation.db = nil
	observation.observer = nil
	event := TxObservation{
		Metadata: observation.metadata,
		Writable: observation.writable,
		Duration: time.Since(observation.start),
	}
	defer func() {
		if recover() != nil {
			db.logObserverPanic()
		}
	}()
	observer(event)
}

func (db *DB) logObserverPanic() {
	defer func() { _ = recover() }()
	if db.observerPanicLogged.CompareAndSwap(false, true) {
		if db.logf != nil {
			db.logf("db: transaction observer panicked; further observer panics will not be logged")
		} else {
			slog.Error("db: transaction observer panicked; further observer panics will not be logged")
		}
	}
}
