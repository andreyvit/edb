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
	metadata TxMetadata
	start    time.Time
}

func (metadata TxMetadata) forWrite() TxMetadata {
	if metadata.Kind == "" {
		metadata.Kind = "system:unknown"
	}
	if metadata.Tenant == "" {
		metadata.Tenant = "unknown"
	}
	return metadata
}

func (db *DB) startObservation(metadata TxMetadata, writable bool) transactionObservation {
	if !writable || db.observeTransaction == nil {
		return transactionObservation{}
	}
	return transactionObservation{db: db, metadata: metadata.forWrite(), start: time.Now()}
}

func (observation *transactionObservation) finish() {
	db := observation.db
	if db == nil {
		return
	}
	observation.db = nil
	event := TxObservation{
		Metadata: observation.metadata,
		Writable: true,
		Duration: time.Since(observation.start),
	}
	defer func() {
		if recover() != nil {
			db.logObserverPanic()
		}
	}()
	db.observeTransaction(event)
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
