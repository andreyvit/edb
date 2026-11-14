package edb

import (
	"slices"
	"sync"
	"testing"
)

type orderedReplicationSlot struct {
	sink      *orderedReplicationSink
	group     *ReplicationGroup
	staged    []ReplicationChange
	cancelled bool
}
type orderedReplicationSink struct {
	mu        sync.Mutex
	cond      *sync.Cond
	slots     []*orderedReplicationSlot
	head      int
	capacity  int
	arrivals  []uint64
	delivered []*ReplicationGroup
}

func newOrderedReplicationSink() *orderedReplicationSink {
	s := new(orderedReplicationSink)
	s.cond = sync.NewCond(&s.mu)
	return s
}
func (s *orderedReplicationSink) Reserve() (ReplicationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	active := 0
	for _, slot := range s.slots[s.head:] {
		if !slot.cancelled {
			active++
		}
	}
	if s.capacity > 0 && active >= s.capacity {
		return nil, ErrReplicationOverload
	}
	slot := &orderedReplicationSlot{sink: s}
	s.slots = append(s.slots, slot)
	return slot, nil
}
func (r *orderedReplicationSlot) Stage(c ReplicationChange) (ReplicationChange, error) {
	s := r.sink
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.group != nil || r.cancelled {
		panic("reservation already resolved")
	}
	owned := copyTestChange(c)
	r.staged = append(r.staged, owned)
	return owned, nil
}
func (r *orderedReplicationSlot) Publish(id, rev uint64) {
	s := r.sink
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.group != nil || r.cancelled {
		panic("reservation already resolved")
	}
	r.group = &ReplicationGroup{TransactionID: id, SchemaRev: rev, Changes: r.staged}
	r.staged = nil
	s.arrivals = append(s.arrivals, id)
	s.consume()
}
func (r *orderedReplicationSlot) Cancel() {
	s := r.sink
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.group != nil || r.cancelled {
		panic("reservation already resolved")
	}
	r.cancelled = true
	r.staged = nil
	s.consume()
}
func (s *orderedReplicationSink) consume() {
	for s.head < len(s.slots) {
		r := s.slots[s.head]
		if r.group == nil && !r.cancelled {
			break
		}
		if r.group != nil {
			s.delivered = append(s.delivered, r.group)
		}
		s.head++
	}
	s.cond.Broadcast()
}
func (s *orderedReplicationSink) Drain() error {
	s.mu.Lock()
	for s.head < len(s.slots) {
		s.cond.Wait()
	}
	groups := slices.Clone(s.delivered)
	s.mu.Unlock()
	for _, g := range groups {
		releaseTestGroup(g)
	}
	return nil
}
func (s *orderedReplicationSink) ids() (arrivals, delivered []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arrivals = slices.Clone(s.arrivals)
	for _, g := range s.delivered {
		delivered = append(delivered, g.TransactionID)
	}
	return
}
func TestReplicationReservations_reordered_commit_and_late_cancel(t *testing.T) {
	sink := newOrderedReplicationSink()
	opt := replicationCaptureOptions(sink)
	sink.capacity = 2
	db := openReplicationTest(t, basicSchema, opt)
	first := db.BeginUpdate(TxMetadata{})
	committed, resume := make(chan struct{}), make(chan struct{})
	first.btx.OnCommit(func() { close(committed); <-resume })
	Put(first, &User{ID: 1})
	done := make(chan error, 1)
	go func() { done <- first.Commit() }()
	var finishOnce sync.Once
	var commitErr error
	finish := func() { finishOnce.Do(func() { close(resume); commitErr = <-done; first.Close() }) }
	t.Cleanup(finish)
	<-committed
	runReplicationRejectedBatch(t, db, 3, 10)
	arrivals, delivered := sink.ids()
	if !slices.Equal(arrivals, []uint64{2}) || len(delivered) != 0 {
		t.Fatalf("passed pending head: arrivals=%v delivered=%v", arrivals, delivered)
	}
	finish()
	if commitErr != nil {
		t.Fatal(commitErr)
	}
	arrivals, delivered = sink.ids()
	if !slices.Equal(arrivals, []uint64{2, 1}) || !slices.Equal(delivered, []uint64{1, 2}) {
		t.Fatalf("arrivals=%v delivered=%v", arrivals, delivered)
	}
	if err := sink.Drain(); err != nil {
		t.Fatal(err)
	}
	aborted := db.BeginUpdate(TxMetadata{})
	defer aborted.Close()
	owner := db.newTx(TxMetadata{}, &boltStorageTx{btx: aborted.btx}, true, nil, nil)
	releaseOwner := func() { owner.Close(); owner.releaseReplication(); owner.release() }
	t.Cleanup(releaseOwner)
	Put(aborted, &User{ID: 3, Name: "aborted"})
	aborted.Close()
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 4, Name: "committed"}) })
	_, delivered = sink.ids()
	if !slices.Equal(delivered, []uint64{1, 2}) {
		t.Fatal("passed unresolved aborted owner", delivered)
	}
	releaseOwner()
	_, delivered = sink.ids()
	if !slices.Equal(delivered, []uint64{1, 2, 3}) {
		t.Fatal("late cancellation did not unblock prefix", delivered)
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(3)) != nil || Get[User](tx, ID(4)) == nil {
			t.Fatal("wrong committed attempt")
		}
	})
	requireReplicationID(t, db, 3)
	if err := sink.Drain(); err != nil {
		t.Fatal(err)
	}
	replicationAssertClean(t, db)
}
