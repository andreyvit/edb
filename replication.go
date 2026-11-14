package edb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
	"go.etcd.io/bbolt"
)

var (
	ErrReplicationOverload      = errors.New("edb: replication RAM capacity exhausted")
	ErrReplicationGroupSize     = errors.New("edb: replication group exceeds limit")
	ErrReplicationSchema        = errors.New("edb: unsupported replication schema revision")
	ErrReplicationShardChange   = errors.New("edb: shard change requires explicit delete and put")
	ErrReplicationTransactionID = errors.New("edb: replication transaction ID must advance")
	ErrReplicationPoisoned      = errors.New("edb: replication capture is poisoned")
)

var errIncompleteMutation = errors.New("edb: incomplete storage mutation")

// ReplicationObject carries local table/map descriptors for stable wire IDs.
// A replication log decoder must resolve wire IDs to destination descriptors before ApplyReplication.
type ReplicationObject struct {
	Table   *TableDef
	KVTable *KVTable
	Map     *KVMap
}
type ReplicationOp uint8

const (
	ReplicationPut ReplicationOp = iota + 1
	ReplicationDelete
)

// ReplicationValue excludes the stored row's local index tail. Present distinguishes
// a missing value from a singleton's empty/nil PUT. Flags identifies the row codec.
type ReplicationValue struct {
	Present   bool
	Flags     uint64
	SchemaVer uint64
	ModCount  uint64
	Data      []byte
}
type ReplicationChange struct {
	Object  ReplicationObject
	Op      ReplicationOp
	Ordinal uint64
	Key     []byte
	Value   ReplicationValue
	Shard   []byte
}

type ReplicationGroup struct {
	TransactionID uint64
	SchemaRev     uint64
	Changes       []ReplicationChange
}

// ReplicationSink admits physical capture attempts into one database's ordered
// queue. EDB calls Reserve synchronously under Bolt's serialized writer, so
// successful reservation order is physical-attempt order, before commit.
// Publications/cancellations may arrive later and out of order. Consume slots
// strictly in reservation order, waiting for pending heads and skipping cancelled
// ones; sorting only groups already published does not establish a safe prefix.
// Exactly one Publish or Cancel follows each successful reservation in normal
// process execution. See _docs/REPLICATION-CONSUMER-CONTRACT.md for the full contract.
type ReplicationSink interface {
	// Reserve must enqueue a pending slot before returning. Fail admission instead
	// of waiting for the current transaction's commit/drain. No I/O or DB reentry.
	Reserve() (ReplicationReservation, error)
	// Drain runs after write admission stops. Wait for all accepted slots to resolve,
	// consume them in order, and durably sync. Final Publish/Cancel calls may overlap.
	Drain() error
}

type ReplicationReservation interface {
	Stage(ReplicationChange) (ReplicationChange, error)
	Publish(transactionID, schemaRev uint64)
	Cancel()
}

// ReplicationOptions enables independent capture and apply capabilities. Neither
// capability changes local-write or row-migration policy. Nil disables both.
// Open copies these options; callbacks and descriptors must remain immutable.
type ReplicationOptions struct {
	Capture *ReplicationCaptureOptions
	Apply   *ReplicationApplyOptions
}

type ReplicationPoisoner interface {
	Poison(error)
}

type ReplicationCaptureOptions struct {
	BestEffort        bool
	NextTransactionID func(lastCommittedID uint64) (uint64, error)
	SchemaRev         uint64
	Include           func(ReplicationObject) bool
	// Shard resolves one stable shard per object/key. Empty success means global;
	// unknown attribution must return an error. Missing raw deletes pass Present=false.
	Shard func(ReplicationObject, []byte, ReplicationValue) ([]byte, error)
	Sink  ReplicationSink
}

// ReplicationApplyOptions accepts complete groups from a consumer-validated history.
// Apply never invokes row migrators or captures a new local change group.
type ReplicationApplyOptions struct {
	SchemaRev        uint64
	SupportsRevision func(uint64) bool
}
type replicationState struct {
	writers    sync.WaitGroup
	opt        ReplicationOptions
	mu         sync.Mutex
	attempts   map[*bbolt.Tx]*replicationAttempt
	captureErr error
}
type replicationAttempt struct {
	btx           *bbolt.Tx
	refs          int
	committed     bool
	err           error
	transactionID uint64
	ordinal       uint64
	reservation   ReplicationReservation
	buckets       map[replicationBucketKey]*replicationStorageBucket
}
type replicationBucketKey struct {
	name string
	sub  string
}

const (
	replicationBucket               = "\x00edb.replication"
	replicationCommittedProp uint64 = 1
)

var (
	replicationCommittedKey = []byte("committed")
)

func (db *DB) initReplication(opt *ReplicationOptions) error {
	if opt == nil || opt.Capture == nil && opt.Apply == nil {
		return nil
	}
	if _, ok := db.storage.(*boltStorage); !ok {
		return errors.New("edb: replication requires Bolt storage")
	}
	o := *opt
	if opt.Capture != nil {
		c := *opt.Capture
		if c.Sink == nil || c.Shard == nil {
			return errors.New("edb: capture requires sink and shard resolver")
		}
		if c.BestEffort {
			if _, ok := c.Sink.(ReplicationPoisoner); !ok {
				return errors.New("edb: best-effort capture requires a poisonable sink")
			}
		}
		o.Capture = &c
	}
	if opt.Apply != nil {
		a := *opt.Apply
		o.Apply = &a
	}
	db.replication = &replicationState{opt: o, attempts: make(map[*bbolt.Tx]*replicationAttempt)}
	return nil
}
func (db *DB) prepareReplication(tx *Tx) error {
	if db.replication == nil || db.replication.opt.Capture == nil || !db.replication.opt.Capture.BestEffort {
		return db.prepareReplicationStrict(tx)
	}
	var err error
	panicErr := captureCall(func() { err = db.prepareReplicationStrict(tx) })
	if err = errors.Join(err, panicErr); err != nil {
		db.replication.poisonCapture(err)
	}
	return nil
}

func (db *DB) prepareReplicationStrict(tx *Tx) error {
	if db.replication == nil {
		return nil
	}
	b, err := tx.btx.CreateBucketIfNotExists([]byte(replicationBucket))
	if err != nil {
		return err
	}
	if b.Get(replicationCommittedKey) == nil {
		if err := b.Put(replicationCommittedKey, row2.Empty(row2.Compact2).Raw()); err != nil {
			return err
		}
	}

	if capture := db.replication.opt.Capture; capture != nil {
		if _, err := row2.TryOpen(b.Get(replicationCommittedKey)); err != nil {
			return err
		}
	}
	return nil
}
func (tx *Tx) attachReplication() {
	r := tx.db.replication
	if r == nil || !tx.IsWritable() {
		return
	}
	{
		r.mu.Lock()
		a := r.attempts[tx.btx]
		if a == nil {
			a = &replicationAttempt{btx: tx.btx}
			r.attempts[tx.btx] = a
		}
		a.refs++
		tx.replication = a
		r.mu.Unlock()
	}
	tx.stx = &replicationStorageTx{storageTx: tx.stx, tx: tx}
}
func (db *DB) discardReplication(btx *bbolt.Tx) {
	r := db.replication
	if r == nil {
		return
	}
	r.mu.Lock()
	a := r.attempts[btx]
	r.mu.Unlock()
	if a != nil {
		r.discard(a)
	}
}
func (r *replicationState) discard(a *replicationAttempt) {
	var reservation ReplicationReservation
	r.mu.Lock()
	if !a.committed {
		reservation, a.reservation = a.reservation, nil
		a.buckets = nil
	}
	r.mu.Unlock()
	if reservation != nil {
		if r.opt.Capture != nil && r.opt.Capture.BestEffort {
			if err := captureCall(reservation.Cancel); err != nil {
				r.poisonCapture(err)
			}
		} else {
			reservation.Cancel()
		}
	}
}
func (tx *Tx) releaseReplication() {
	a := tx.replication
	if a == nil {
		return
	}
	r := tx.db.replication
	r.mu.Lock()
	a.refs--
	last := a.refs == 0
	if last {
		delete(r.attempts, a.btx)
	}
	discard := last && !a.committed && a.reservation != nil
	r.mu.Unlock()
	if discard {
		r.discard(a)
	}
	tx.replication = nil
}
func (tx *Tx) replicationError() error {
	if tx.localWriteRejected {
		return ErrLocalWriteRejected
	}
	if tx.replication != nil && tx.replication.err != nil {
		return tx.replication.err
	}
	if tx.mutationFailed {
		return errIncompleteMutation
	}
	return nil
}
func (a *replicationAttempt) poison(err error) error {
	if err != nil && a.err == nil {
		a.err = err
	}
	return err
}
func (tx *Tx) poisonReplication(err error) error {
	if tx.replication != nil {
		return tx.replication.poison(err)
	}
	return err
}
func (v value) replicationValue(present bool) ReplicationValue {
	return ReplicationValue{Present: present, Flags: uint64(v.Flags), SchemaVer: v.SchemaVer, ModCount: v.ModCount, Data: v.Data}
}
func rawReplicationValue(raw []byte) ReplicationValue {
	return ReplicationValue{Present: raw != nil, Data: raw}
}

func (tx *Tx) captureOptions(obj ReplicationObject) *ReplicationCaptureOptions {
	r := tx.db.replication
	if r == nil || r.opt.Capture == nil || tx.rawWrites || tx.replication == nil {
		return nil
	}
	opt := r.opt.Capture
	if opt.BestEffort {
		tx.syncCapturePoison()
		included := true
		if err := captureCall(func() {
			if opt.Include != nil {
				included = opt.Include(obj)
			}
		}); err != nil {
			tx.captureFailure(err)
			return nil
		}
		if !included {
			return nil
		}
	} else if opt.Include != nil && !opt.Include(obj) {
		return nil
	}
	return opt
}

func (tx *Tx) stageReplicationChange(opt *ReplicationCaptureOptions, obj ReplicationObject, op ReplicationOp, key []byte, old, next ReplicationValue) {
	if opt == nil {
		return
	}
	if !opt.BestEffort || tx.replication.err != nil {
		tx.stageReplicationStrict(opt, obj, op, key, old, next)
		return
	}
	if err := captureCall(func() {
		tx.stageReplicationStrict(opt, obj, op, key, old, next)
	}); err != nil {
		tx.replication.err = nil
		tx.captureFailure(err)
	}
	tx.syncCapturePoison()
}

func (tx *Tx) stageReplicationStrict(opt *ReplicationCaptureOptions, obj ReplicationObject, op ReplicationOp, key []byte, old, next ReplicationValue) {
	if opt == nil {
		return
	}
	a := tx.replication
	if a.err != nil {
		panic(a.err)
	}
	if a.reservation == nil {
		reservation, err := opt.Sink.Reserve()
		if err != nil {
			panic(tx.poisonReplication(err))
		}
		if reservation == nil {
			panic(tx.poisonReplication(errors.New("edb: nil replication reservation")))
		}
		a.reservation = reservation
		r := tx.db.replication
		tx.btx.OnCommit(func() {
			r.mu.Lock()
			a.committed = true
			a.buckets = nil
			delete(r.attempts, a.btx)
			r.mu.Unlock()
			if opt.BestEffort {
				if err := captureCall(func() { reservation.Publish(a.transactionID, opt.SchemaRev) }); err != nil {
					r.poisonCapture(err)
					captureCall(reservation.Cancel)
				}
			} else {
				reservation.Publish(a.transactionID, opt.SchemaRev)
			}
		})
	}
	if a.transactionID == 0 {
		b := tx.btx.Bucket([]byte(replicationBucket))
		checkpoint := row2.Open(b.Get(replicationCommittedKey))
		head := checkpoint.Get(replicationCommittedProp, row2.Word).AsUint()
		if head == ^uint64(0) {
			panic(tx.poisonReplication(errors.New("edb: replication sequence exhausted")))
		}
		var (
			id  = head + 1
			err error
		)
		if opt.NextTransactionID != nil {
			id, err = opt.NextTransactionID(head)
			if err != nil {
				panic(tx.poisonReplication(err))
			}
		}
		if id <= head {
			panic(tx.poisonReplication(ErrReplicationTransactionID))
		}
		a.transactionID = id
		updated := row2.Edit(checkpoint)
		updated.Set([]row2.Step{{Key: replicationCommittedProp}}, values.Uint(a.transactionID), row2.Word)
		ensure(tx.poisonReplication(b.Put(replicationCommittedKey, updated.Snapshot().Raw())))

	}
	resolve := func(v ReplicationValue) []byte {
		shard, err := opt.Shard(obj, key, v)
		if err != nil {
			panic(tx.poisonReplication(err))
		}
		return shard
	}
	change := ReplicationChange{Object: obj, Op: op, Ordinal: a.ordinal, Key: key, Value: next}
	if op == ReplicationDelete {
		change.Shard = resolve(old)
	} else {
		change.Shard = resolve(next)
	}
	staged, err := a.reservation.Stage(change)
	if err != nil {
		panic(tx.poisonReplication(err))
	}
	if op == ReplicationPut && old.Present && !bytes.Equal(resolve(old), staged.Shard) {
		panic(tx.poisonReplication(ErrReplicationShardChange))
	}
	a.ordinal++
}

// ReplicationStatus returns the database's last committed replication transaction
// ID from a snapshot. Capture allocates the next ID; apply preserves the incoming
// ID. Both update this checkpoint atomically with their data. Uncommitted/aborted
// attempts are invisible. Missing status is zero, even with replication disabled.
// This is neither a Bolt ID nor an external replication log cursor. The consumer validates
// history identity, detects gaps and chooses recovery policy.
func (db *DB) ReplicationStatus() (committedID uint64, err error) {
	if db.bdb == nil {
		return 0, errors.New("edb: replication status requires Bolt storage")
	}
	err = db.ReadErr(TxMetadata{Kind: "system:replication_status", Tenant: "none"}, func(tx *Tx) error {
		b := tx.btx.Bucket([]byte(replicationBucket))
		if b == nil {
			return nil
		}
		checkpoint, err := row2.TryOpen(b.Get(replicationCommittedKey))
		if err != nil {
			return err
		}
		committedID = checkpoint.Get(replicationCommittedProp, row2.Word).AsUint()
		return nil
	})
	return
}

// The guard poisons storage failures even if application code catches a panic or
// requests CommitDespiteError after a partially persisted primary/index mutation.
type replicationStorageTx struct {
	storageTx
	tx *Tx
}

func (s *replicationStorageTx) Bucket(name, sub string) storageBucket {
	a := s.tx.replication
	key := replicationBucketKey{name, sub}
	if cached := a.buckets[key]; cached != nil {
		return cached
	}
	b := s.storageTx.Bucket(name, sub)
	if b == nil {
		return nil
	}
	return s.cacheBucket(key, b)
}

func (s *replicationStorageTx) cacheBucket(key replicationBucketKey, b storageBucket) storageBucket {
	a := s.tx.replication
	if cached := a.buckets[key]; cached != nil {
		return cached
	}
	if a.buckets == nil {
		a.buckets = make(map[replicationBucketKey]*replicationStorageBucket)
	}
	wrapped := &replicationStorageBucket{storageBucket: b, attempt: a}
	a.buckets[key] = wrapped
	return wrapped
}

func (s *replicationStorageTx) CreateBucket(name, sub string) (storageBucket, error) {
	b, err := s.storageTx.CreateBucket(name, sub)
	if err != nil {
		return nil, s.tx.poisonReplication(err)
	}
	return s.cacheBucket(replicationBucketKey{name, sub}, b), nil
}
func (s *replicationStorageTx) DeleteBucket(name, sub string) error {
	err := s.storageTx.DeleteBucket(name, sub)
	if err == nil || errors.Is(err, ErrBucketNotFound) {
		a := s.tx.replication
		if sub == "" {
			for key := range a.buckets {
				if key.name == name {
					delete(a.buckets, key)
				}
			}
		} else {
			delete(a.buckets, replicationBucketKey{name, sub})
		}
	} else {
		s.tx.poisonReplication(err)
	}
	return err
}

type replicationStorageBucket struct {
	storageBucket
	attempt *replicationAttempt
}

func (b *replicationStorageBucket) Put(k, v []byte) error {
	return b.attempt.poison(b.storageBucket.Put(k, v))
}
func (b *replicationStorageBucket) Delete(k []byte) error {
	return b.attempt.poison(b.storageBucket.Delete(k))
}
func (b *replicationStorageBucket) Cursor() storageCursor {
	return &replicationStorageCursor{b.storageBucket.Cursor(), b.attempt}
}

type replicationStorageCursor struct {
	storageCursor
	attempt *replicationAttempt
}

func (c *replicationStorageCursor) Delete() error {
	return c.attempt.poison(c.storageCursor.Delete())
}

func (tx *Tx) guardReplicationMutation(complete *bool) {
	if p := recover(); p != nil {
		if !*complete {
			tx.mutationFailed = true
			tx.poisonReplication(fmt.Errorf("edb: incomplete storage mutation: %v", p))
		}
		panic(p)
	}
}

func (db *DB) admitReplicationWrite() func() {
	r := db.replication
	if r == nil {
		return func() {}
	}
	r.mu.Lock()
	if db.IsClosed() {
		r.mu.Unlock()
		panic("database closed")
	}
	r.writers.Add(1)
	r.mu.Unlock()
	return r.writers.Done
}
func (db *DB) drainReplication() {
	r := db.replication
	if r == nil {
		return
	}
	// Synchronize admission's Add with Wait, after closed was set.
	r.mu.Lock()
	r.mu.Unlock()
	r.writers.Wait()
	if r.opt.Capture != nil {
		if r.opt.Capture.BestEffort {
			var err error
			panicErr := captureCall(func() { err = r.opt.Capture.Sink.Drain() })
			r.poisonCapture(errors.Join(err, panicErr))
		} else {
			ensure(r.opt.Capture.Sink.Drain())
		}
	}
}

type ReplicationObjectKind uint8

const (
	ReplicationInvalidObject ReplicationObjectKind = iota
	ReplicationRowTable
	ReplicationKVTable
	ReplicationSingletonMap
)

func (o ReplicationObject) Kind() ReplicationObjectKind {
	switch {
	case o.Table != nil && o.KVTable == nil && o.Map == nil:
		return ReplicationRowTable
	case o.KVTable != nil && o.Table == nil && o.Map == nil:
		return ReplicationKVTable
	case o.Map != nil && o.Table == nil && o.KVTable == nil:
		return ReplicationSingletonMap
	default:
		return ReplicationInvalidObject
	}
}

// NameHash returns the registered object's cached schema hash. Invalid or
// ambiguous descriptors panic, rather than silently selecting an object kind.
func (o ReplicationObject) NameHash() uint64 {
	switch o.Kind() {
	case ReplicationRowTable:
		return o.Table.NameHash()
	case ReplicationKVTable:
		return o.KVTable.NameHash()
	case ReplicationSingletonMap:
		return o.Map.NameHash()
	default:
		panic("invalid replication object")
	}
}
