package edb

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
	"go.etcd.io/bbolt"
)

func captureCall(call func()) (err error) {
	defer func() {
		if value := recover(); value != nil {
			if cause, ok := value.(error); ok {
				err = fmt.Errorf("edb capture: %w", cause)
			} else {
				err = fmt.Errorf("edb capture panic: %v", value)
			}
		}
	}()
	call()
	return nil
}

func (r *replicationState) poisonCapture(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	first := r.captureErr == nil
	if first {
		r.captureErr = err
	}
	r.mu.Unlock()
	if !first {
		return
	}
	if sink, ok := r.opt.Capture.Sink.(ReplicationPoisoner); ok {
		captureCall(func() { sink.Poison(err) })
	}
}

func (tx *Tx) syncCapturePoison() {
	r := tx.db.replication
	r.mu.Lock()
	err := r.captureErr
	r.mu.Unlock()
	if err == nil {
		if state, ok := r.opt.Capture.Sink.(interface{ ReplicationError() error }); ok {
			panicErr := captureCall(func() { err = state.ReplicationError() })
			err = errors.Join(err, panicErr)
		}
	}
	if err != nil {
		tx.captureFailure(err)
	}
}

func (tx *Tx) captureFailure(err error) {
	tx.db.replication.poisonCapture(err)
	if tx.replication.transactionID != 0 {
		return
	}
	bucket := tx.btx.Bucket([]byte(replicationBucket))
	checkpoint := row2.Open(bucket.Get(replicationCommittedKey))
	head := checkpoint.Get(replicationCommittedProp, row2.Word).AsUint()
	if head == ^uint64(0) {
		panic(tx.replication.poison(ErrReplicationTransactionID))
	}
	editor := row2.Edit(checkpoint)
	editor.Set([]row2.Step{{Key: replicationCommittedProp}}, values.Uint(head+1), row2.Word)
	ensure(tx.replication.poison(bucket.Put(replicationCommittedKey, editor.Snapshot().Raw())))
	tx.replication.transactionID = head + 1
}

func (db *DB) ReplicationPoisoned() (bool, error) {
	if db.IsClosed() {
		return false, errors.New("edb: database closed")
	}
	if db.bdb == nil {
		return false, errors.New("edb: replication status requires Bolt storage")
	}
	if db.replication != nil {
		db.replication.mu.Lock()
		failed := db.replication.captureErr != nil
		db.replication.mu.Unlock()
		if failed {
			return true, nil
		}
		if db.replication.opt.Capture != nil {
			if state, ok := db.replication.opt.Capture.Sink.(interface{ ReplicationError() error }); ok {
				var err error
				panicErr := captureCall(func() { err = state.ReplicationError() })
				if err != nil || panicErr != nil {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func ReadReplicationStatus(path string) (uint64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if info.Size() == 0 {
		return 0, nil
	}
	db, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return 0, err
	}
	var id uint64
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(replicationBucket))
		if bucket == nil {
			return nil
		}
		data := bucket.Get(replicationCommittedKey)
		if len(data) == 0 {
			return nil
		}
		checkpoint, err := row2.TryOpen(data)
		if err != nil {
			return err
		}
		id = checkpoint.Get(replicationCommittedProp, row2.Word).AsUint()
		return nil
	})
	return id, errors.Join(err, db.Close())
}
