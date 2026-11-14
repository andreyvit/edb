package edb

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

type bestEffortTestSink struct {
	mu      sync.Mutex
	failure error
}

type bestEffortTestReservation struct{}

func (s *bestEffortTestSink) Reserve() (ReplicationReservation, error) {
	return &bestEffortTestReservation{}, nil
}
func (s *bestEffortTestSink) Drain() error { return nil }
func (s *bestEffortTestSink) Poison(err error) {
	s.mu.Lock()
	if s.failure == nil {
		s.failure = err
	}
	s.mu.Unlock()
}
func (s *bestEffortTestSink) ReplicationError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failure
}
func (*bestEffortTestReservation) Stage(change ReplicationChange) (ReplicationChange, error) {
	return copyTestChange(change), nil
}
func (*bestEffortTestReservation) Publish(uint64, uint64) {}
func (*bestEffortTestReservation) Cancel()                {}

func TestBestEffortCaptureCallbackFailuresCommitDataAndPersistPoison(t *testing.T) {
	failure := errors.New("replication callback failed")
	cases := []struct {
		name      string
		configure func(*ReplicationCaptureOptions)
	}{
		{"allocator_error", func(opt *ReplicationCaptureOptions) {
			opt.NextTransactionID = func(uint64) (uint64, error) { return 0, failure }
		}},
		{"allocator_panic", func(opt *ReplicationCaptureOptions) {
			opt.NextTransactionID = func(uint64) (uint64, error) { panic(failure) }
		}},
		{"allocator_zero", func(opt *ReplicationCaptureOptions) {
			opt.NextTransactionID = func(uint64) (uint64, error) { return 0, nil }
		}},
		{"include_panic", func(opt *ReplicationCaptureOptions) { opt.Include = func(ReplicationObject) bool { panic(failure) } }},
		{"shard_error", func(opt *ReplicationCaptureOptions) {
			opt.Shard = func(ReplicationObject, []byte, ReplicationValue) ([]byte, error) { return nil, failure }
		}},
		{"shard_panic", func(opt *ReplicationCaptureOptions) {
			opt.Shard = func(ReplicationObject, []byte, ReplicationValue) ([]byte, error) { panic(failure) }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				path = filepath.Join(t.TempDir(), "source.db")
				sink = &bestEffortTestSink{}
				opt  = replicationCaptureOptions(sink)
			)
			opt.Capture.BestEffort = true
			tc.configure(opt.Capture)
			db, err := Open(path, basicSchema, Options{IsTesting: true, Replication: opt})
			if err != nil {
				t.Fatal(err)
			}
			err = db.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: 1, Name: "committed"}); return nil })
			if err != nil {
				t.Fatalf("replication callback aborted source: %v", err)
			}
			db.Read(TxMetadata{}, func(tx *Tx) {
				if row := Get[User](tx, ID(1)); row == nil || row.Name != "committed" {
					t.Fatal("source data missing")
				}
			})
			if poisoned, err := db.ReplicationPoisoned(); err != nil || !poisoned {
				t.Fatalf("runtime poison: %v %v", poisoned, err)
			}
			id, err := db.ReplicationStatus()
			if err != nil || id == 0 {
				t.Fatalf("failed capture did not advance the database checkpoint: %d %v", id, err)
			}
			db.Close()
			replacement := &bestEffortTestSink{}
			opt = replicationCaptureOptions(replacement)
			opt.Capture.BestEffort = true
			db, err = Open(path, basicSchema, Options{IsTesting: true, Replication: opt})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if replacement.ReplicationError() != nil {
				t.Fatal("restart restored runtime poison")
			}
			if poisoned, err := db.ReplicationPoisoned(); err != nil || poisoned {
				t.Fatalf("poison survived reopening: %v %v", poisoned, err)
			}
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 2, Name: "still writable"}) })
		})
	}
}

func TestBestEffortDoesNotMaskBusinessOrStorageErrors(t *testing.T) {
	var (
		sink     = &bestEffortTestSink{}
		opt      = replicationCaptureOptions(sink)
		business = errors.New("business failure")
		storage  = errors.New("primary storage failure")
	)
	opt.Capture.BestEffort = true
	db := openReplicationTest(t, basicSchema, opt)
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: 1}); return business })
	if !errors.Is(err, business) {
		t.Fatalf("business error changed: %v", err)
	}
	err = db.Tx(TxMetadata{}, true, func(tx *Tx) error {
		tx.CommitDespiteError()
		tx.replication.poison(storage)
		func() { defer func() { recover() }(); Put(tx, &User{ID: 2}) }()
		return nil
	})
	if !errors.Is(err, storage) {
		t.Fatalf("primary storage error masked: %v", err)
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(1)) != nil || Get[User](tx, ID(2)) != nil {
			t.Fatal("failed source write committed")
		}
	})
}
