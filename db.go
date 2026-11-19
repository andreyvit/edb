package edb

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

const debugTrackTxns = true

type DB struct {
	storage                 storage
	bdb                     *bbolt.DB
	schema                  *Schema
	logf                    func(format string, args ...any)
	verbose                 bool
	strict                  bool
	captureTxStack          bool
	reuseTxBuffers          bool
	observeWriteTransaction func(TxObservation)
	observeReadTransaction  func(TxObservation)
	txObserveBatch          func(BatchObservation)
	batchAttempt            batchAttempt
	observerPanicLogged     atomic.Bool

	tableStates []*tableState

	lastSize           atomic.Int64
	ReaderCount        atomic.Int64
	WriterCount        atomic.Int64
	PendingWriterCount atomic.Int64
	ReadCount          atomic.Uint64
	WriteCount         atomic.Uint64

	txns     []*Tx
	txnsLock sync.Mutex

	closed  atomic.Bool
	closeWG sync.WaitGroup

	safeToQuitCallbackLock   sync.Mutex
	safeToQuitCallback       func()
	safeToQuitCallbackCalled bool

	localWrites   LocalWritePolicy
	rowMigrations RowMigrationPolicy
	replication   *replicationState
}

type Options struct {
	// Policies are independent of replication capabilities; zero values preserve existing behavior.
	LocalWrites   LocalWritePolicy
	RowMigrations RowMigrationPolicy
	Replication   *ReplicationOptions
	Logf          func(format string, args ...any)
	Verbose       bool
	IsTesting     bool
	MmapSize      int

	NoPersistentFreeList    bool
	CaptureTxStack          bool
	ReuseTxBuffers          bool
	ObserveWriteTransaction func(TxObservation)
	ObserveReadTransaction  func(TxObservation)
	TxObserveBatch          func(BatchObservation)
}

const InMemory = "memory:"

func Open(path string, schema *Schema, opt Options) (*DB, error) {
	if path == InMemory {
		return openWithStorage(newMemStorage(), schema, opt)
	}
	bopt := &bbolt.Options{
		Timeout: 10 * time.Second,
	}
	*bopt = *bbolt.DefaultOptions
	if opt.IsTesting {
		bopt.NoSync = true
		bopt.NoFreelistSync = true
		bopt.InitialMmapSize = 1024 * 1024 * 5
	} else {
		bopt.InitialMmapSize = 1024 * 1024 * 1024
		bopt.FreelistType = bbolt.FreelistMapType
	}
	if opt.MmapSize != 0 {
		bopt.InitialMmapSize = opt.MmapSize
	}
	if opt.NoPersistentFreeList {
		bopt.NoFreelistSync = true
	}

	start := time.Now()
	bdb, err := bbolt.Open(path, 0666, bopt)
	if err != nil {
		return nil, fmt.Errorf("kvdb: %w", err)
	}
	if elapsed := time.Since(start); elapsed >= 5*time.Millisecond {
		if opt.Logf != nil {
			opt.Logf("db: bbolt open took %d ms", elapsed.Milliseconds())
		}
	}

	db, err := openWithStorage(newBoltStorage(bdb), schema, opt)
	if err != nil {
		_ = bdb.Close()
		return nil, err
	}
	db.bdb = bdb

	return db, nil
}

func openWithStorage(storage storage, schema *Schema, opt Options) (*DB, error) {
	db := &DB{
		storage:                 storage,
		localWrites:             opt.LocalWrites,
		rowMigrations:           opt.RowMigrations,
		schema:                  schema,
		logf:                    opt.Logf,
		verbose:                 opt.Verbose,
		captureTxStack:          opt.CaptureTxStack,
		reuseTxBuffers:          opt.ReuseTxBuffers,
		observeWriteTransaction: opt.ObserveWriteTransaction,
		observeReadTransaction:  opt.ObserveReadTransaction,
		txObserveBatch:          opt.TxObserveBatch,
		tableStates:             make([]*tableState, len(schema.tables)),
		strict:                  opt.IsTesting,
	}
	if err := validateWritePolicies(opt); err != nil {
		return nil, err
	}
	if err := db.initReplication(opt.Replication); err != nil {
		return nil, err
	}
	db.closeWG.Add(1)

	tx := db.BeginUpdate(TxMetadata{Kind: "startup:edb_schema", Tenant: "none"})
	defer tx.Close()
	tx.internalWrite = true
	if err := db.prepareReplication(tx); err != nil {
		return nil, err
	}
	prepareSchema := func(tx *Tx) error {
		now := time.Now()
		for i, tbl := range schema.tables {
			db.tableStates[i] = prepareTable(tx, tbl, now)
		}
		for _, tbl := range schema.kvtables {
			prepareKVTable(tx, tbl)
		}
		for _, mp := range schema.maps {
			prepareMap(tx, mp)
		}
		for _, ts := range db.tableStates {
			ts.migrate(tx)
		}
		for _, ts := range db.tableStates {
			ts.save(tx)
		}
		return nil
	}
	if db.replication == nil {
		ensure(prepareSchema(tx))
	} else if err := safelyCall(prepareSchema, tx); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		if db.replication == nil {
			panic(fmt.Errorf("commit: %w", err))
		}
		return nil, err
	}

	return db, nil
}

func (db *DB) Bolt() *bbolt.DB {
	return db.bdb
}

func (db *DB) Size() int64 {
	return db.lastSize.Load()
}

// Close is safe to call multiple times, but not concurrently.
func (db *DB) Close() {
	db.CloseWithSafeToQuitCallback(nil)
}

// Close is safe to call multiple times, but not concurrently.
func (db *DB) CloseWithSafeToQuitCallback(safeToQuit func()) {
	if safeToQuit != nil {
		if !db.addSafeToQuitCallback(safeToQuit) {
			safeToQuit()
		}
	}

	if db.closed.CompareAndSwap(false, true) {
		db.doClose()
	}
	db.closeWG.Wait()
}

func (db *DB) IsClosed() bool {
	return db.closed.Load()
}

func (db *DB) addSafeToQuitCallback(safeToQuit func()) bool {
	db.safeToQuitCallbackLock.Lock()
	defer db.safeToQuitCallbackLock.Unlock()

	if db.safeToQuitCallbackCalled {
		return false
	}

	if prev := db.safeToQuitCallback; prev == nil {
		db.safeToQuitCallback = safeToQuit
	} else {
		db.safeToQuitCallback = func() {
			prev()
			safeToQuit()
		}
	}
	return true
}

func (db *DB) retrieveSafeToQuitCallback() func() {
	db.safeToQuitCallbackLock.Lock()
	defer db.safeToQuitCallbackLock.Unlock()

	f := db.safeToQuitCallback
	db.safeToQuitCallback = nil
	db.safeToQuitCallbackCalled = true
	return f
}

func (db *DB) doClose() {
	defer db.closeWG.Done()
	db.drainReplication()

	start := time.Now()
	if db.logf != nil {
		db.logf("db: close started")
	}

	if db.bdb != nil && db.bdb.NoFreelistSync && db.WriteCount.Load() > 0 {
		// Write freelist to make startup fast.
		start := time.Now()
		err := db.writeFreelist()
		if db.logf != nil {
			elapsed := time.Since(start)
			if err != nil {
				db.logf("db: writing bbolt freelist failed: %v", err)
			} else {
				db.logf("db: bbolt freelist written in %d ms", elapsed.Milliseconds())
			}
		}
	}

	f := db.retrieveSafeToQuitCallback()
	if f != nil {
		f()
	}

	if db.storage != nil {
		start := time.Now()
		if db.logf != nil {
			db.logf("db: storage close started")
		}
		err := db.storage.Close()
		if err != nil {
			panic(fmt.Errorf("kvdb: closing: %w", err))
		}
		if db.logf != nil {
			db.logf("db: storage close completed in %d ms", time.Since(start).Milliseconds())
		}
	}
	if db.logf != nil {
		db.logf("db: close completed in %d ms", time.Since(start).Milliseconds())
	}
}

func (db *DB) writeFreelist() error {
	observation := db.startObservation(TxMetadata{Kind: "system:edb_shutdown_freelist", Tenant: "none"}, true)
	defer observation.finish()
	db.bdb.NoFreelistSync = false
	start := time.Now()
	if db.logf != nil {
		db.logf("db: bbolt freelist Update requested")
	}
	return db.bdb.Update(func(*bbolt.Tx) error {
		if db.logf != nil {
			db.logf("db: bbolt freelist Update callback reached in %d ms (writer transaction acquired)", time.Since(start).Milliseconds())
		}
		return nil
	})
}

func (db *DB) addTx(tx *Tx) {
	db.txnsLock.Lock()
	defer db.txnsLock.Unlock()
	db.txns = append(db.txns, tx)
}

func (db *DB) removeTx(tx *Tx) {
	db.txnsLock.Lock()
	defer db.txnsLock.Unlock()

	found := -1
	for i, t := range db.txns {
		if t == tx {
			found = i
			break
		}
	}
	if found < 0 {
		panic("tx not found in list")
	}

	n := len(db.txns)
	db.txns[found] = db.txns[n-1]
	db.txns[n-1] = nil // ensure it gets collected
	db.txns = db.txns[:n-1]
}

func (db *DB) DescribeOpenTxns() string {
	if !debugTrackTxns {
		return "OPEN TX TRACKING DISABLED"
	}

	db.txnsLock.Lock()
	txns := slices.Clone(db.txns)
	db.txnsLock.Unlock()

	if len(txns) == 0 {
		return "NO OPEN TRANSACTIONS"
	}

	slices.SortFunc(txns, func(a, b *Tx) int {
		return a.startTime.Compare(b.startTime)
	})

	now := time.Now()

	var buf strings.Builder
	fmt.Fprintf(&buf, "%d OPEN TRANSACTIONS:\n", len(txns))
	for _, tx := range txns {
		ms := now.Sub(tx.startTime).Milliseconds()
		mode := "read"
		if tx.IsWritable() {
			mode = "write"
		}
		kind := cmp.Or(tx.metadata.Kind, "system:unknown")
		tenant := cmp.Or(tx.metadata.Tenant, "unknown")
		fmt.Fprintf(&buf, "\n---\nopen for %d ms (%s) Kind=%s Tenant=%s", ms, mode, kind, tenant)
		if ms < 100 || len(tx.stack) == 0 {
			buf.WriteByte('\n')
		} else {
			fmt.Fprintf(&buf, ":\n%s", tx.stack)
		}
	}

	return buf.String()
}
