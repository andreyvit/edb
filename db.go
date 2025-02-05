package edb

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

const debugTrackTxns = false

type DB struct {
	bdb     *bbolt.DB
	schema  *Schema
	logf    func(format string, args ...any)
	verbose bool
	strict  bool

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
}

type Options struct {
	Logf      func(format string, args ...any)
	Verbose   bool
	IsTesting bool
	MmapSize  int

	NoPersistentFreeList bool
}

func Open(path string, schema *Schema, opt Options) (*DB, error) {
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

	db := &DB{
		bdb:         bdb,
		schema:      schema,
		logf:        opt.Logf,
		verbose:     opt.Verbose,
		tableStates: make([]*tableState, len(schema.tables)),
		strict:      opt.IsTesting,
	}
	db.closeWG.Add(1)

	db.Write(func(tx *Tx) {
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
	})

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
	if db.closed.CompareAndSwap(false, true) {
		db.doClose()
	}
	db.closeWG.Wait()
}

func (db *DB) IsClosed() bool {
	return db.closed.Load()
}

func (db *DB) doClose() {
	defer db.closeWG.Done()

	if db.bdb.NoFreelistSync && db.WriteCount.Load() > 0 {
		// Write freelist to make startup fast.
		db.bdb.NoFreelistSync = false
		start := time.Now()
		db.bdb.Update(func(*bbolt.Tx) error {
			return nil
		})
		if db.logf != nil {
			elapsed := time.Since(start)
			db.logf("db: bbolt freelist written in %d ms", elapsed.Milliseconds())
		}
	}

	err := db.bdb.Close()
	if err != nil {
		panic(fmt.Errorf("kvdb: closing: %w", err))
	}
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
		if ms < 100 {
			fmt.Fprintf(&buf, "\n---\nopen for %d ms\n", ms)
		} else {
			fmt.Fprintf(&buf, "\n---\nopen for %d ms:\n%s", ms, tx.stack)
		}
	}

	return buf.String()
}

func Foo() int64 {
	return 42
}
