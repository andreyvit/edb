package edb

import (
	"errors"
	"slices"
	"testing"
)

func TestReplicationIDAllocatorUsesCommittedCheckpointOncePerTransaction(t *testing.T) {
	var (
		sink     = &replicationTestSink{}
		opt      = replicationCaptureOptions(sink)
		heads    []uint64
		rollback = errors.New("rollback")
	)
	opt.Capture.NextTransactionID = func(previous uint64) (uint64, error) {
		heads = append(heads, previous)
		return previous + 10, nil
	}
	db := openReplicationTest(t, basicSchema, opt)
	db.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "first"})
		Put(tx, &User{ID: 2, Name: "second"})
	})
	requireReplicationID(t, db, 10)
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
		Put(tx, &User{ID: 3, Name: "rolled back"})
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 10)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 3, Name: "retry"}) })
	requireReplicationID(t, db, 20)
	if !slices.Equal(heads, []uint64{0, 10, 10}) {
		t.Fatalf("allocator heads: %v", heads)
	}
	groups := sink.snapshot()
	if len(groups) != 2 || groups[0].TransactionID != 10 || groups[1].TransactionID != 20 || len(groups[0].Changes) != 2 || sink.cancelled != 1 {
		t.Fatal("allocation escaped physical commit/rollback boundaries")
	}
	replicationAssertClean(t, db)
}

func TestReplicationIDAllocatorFailureCannotCommit(t *testing.T) {
	var (
		failure = errors.New("allocator failure")
		cases   = []struct {
			name     string
			allocate func(uint64) (uint64, error)
			want     error
		}{
			{"zero", func(uint64) (uint64, error) { return 0, nil }, ErrReplicationTransactionID},
			{"same", func(previous uint64) (uint64, error) { return previous, nil }, ErrReplicationTransactionID},
			{"lower", func(previous uint64) (uint64, error) { return previous - 1, nil }, ErrReplicationTransactionID},
			{"error", func(uint64) (uint64, error) { return 0, failure }, failure},
			{"panic", func(uint64) (uint64, error) { panic(failure) }, nil},
		}
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				sink   = &replicationTestSink{}
				opt    = replicationCaptureOptions(sink)
				reject bool
			)
			opt.Capture.NextTransactionID = func(previous uint64) (uint64, error) {
				if reject {
					return tc.allocate(previous)
				}
				return previous + 10, nil
			}
			db := openReplicationTest(t, basicSchema, opt)
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
			reject = true
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
				tx.CommitDespiteError()
				func() {
					defer func() { recover() }()
					Put(tx, &User{ID: 2})
				}()
				return nil
			})
			if err == nil || tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("allocator error lost: %v", err)
			}
			requireReplicationID(t, db, 10)
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Get[User](tx, ID(2)) != nil {
					t.Fatal("allocator failure persisted data")
				}
			})
			if len(sink.snapshot()) != 1 || sink.cancelled != 1 {
				t.Fatal("failed ID published or reservation leaked")
			}
			replicationAssertClean(t, db)
		})
	}
}

func TestReplicationApplyPreservesIDWithoutCallingAllocator(t *testing.T) {
	var (
		sink  = &replicationTestSink{}
		opt   = replicationCaptureOptions(sink)
		heads []uint64
	)
	opt.Apply = replicationApplyOptions().Apply
	opt.Capture.NextTransactionID = func(previous uint64) (uint64, error) {
		heads = append(heads, previous)
		return previous + 10, nil
	}
	db := openReplicationTest(t, basicSchema, opt)
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 500, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	requireReplicationID(t, db, 500)
	if len(heads) != 0 || len(sink.snapshot()) != 0 {
		t.Fatal("apply allocated or recaptured an upstream ID")
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
	requireReplicationID(t, db, 510)
	if !slices.Equal(heads, []uint64{500}) {
		t.Fatalf("allocator ignored applied checkpoint: %v", heads)
	}
}
