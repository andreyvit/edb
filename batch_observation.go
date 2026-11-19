package edb

import (
	"time"
	"weak"

	"go.etcd.io/bbolt"
)

type BatchOutcome uint8

const (
	BatchCommitted BatchOutcome = iota + 1
	BatchCallbackAborted
)

type BatchObservation struct {
	Outcome      BatchOutcome
	CallbacksRun int
	CommitStats  *BatchCommitStats
}

type BatchCommitStats struct {
	PageAllocBytes int64
	WriteDuration  time.Duration
}

type batchAttempt struct {
	tx    weak.Pointer[bbolt.Tx]
	count *int
}

func (db *DB) batchCallbackEnter(tx *bbolt.Tx) int {
	key := weak.Make(tx)
	if db.batchAttempt.tx != key {
		count := new(int)
		db.batchAttempt = batchAttempt{tx: key, count: count}
		tx.OnCommit(func() {
			stats := tx.Stats()
			db.batchObserve(BatchObservation{
				Outcome:      BatchCommitted,
				CallbacksRun: *count,
				CommitStats:  &BatchCommitStats{PageAllocBytes: stats.GetPageAlloc(), WriteDuration: stats.GetWriteTime()},
			})
		})
	}
	count := db.batchAttempt.count
	*count++
	return *count
}

func (db *DB) batchObserve(event BatchObservation) {
	defer func() {
		if recover() != nil {
			db.logObserverPanic()
		}
	}()
	db.txObserveBatch(event)
}
