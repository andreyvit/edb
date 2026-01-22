package edb

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

var (
	ReaderCount atomic.Int64
	WriterCount atomic.Int64
)

type Txish interface {
	DBTx() *Tx
}

type Tx struct {
	db        *DB
	stx       storageTx
	btx       *bbolt.Tx
	managed   bool
	closed    bool
	startTime time.Time
	stack     []byte
	verbosity int
	logger    *slog.Logger

	written          bool
	commitDespiteErr bool
	reindexing       bool

	memo map[string]any

	indexKeyBufs   [][]byte
	valueBufs      [][]byte
	indexValueBufs [][]byte

	changeHandler func(tx *Tx, chg *Change)
	changeOptions map[*Table]ChangeFlags
}

func (db *DB) newTx(stx storageTx, managed bool, memo map[string]any, stack []byte) *Tx {
	if db.IsClosed() {
		panic("database closed")
	}
	db.lastSize.Store(stx.Size())
	if stx.Writable() {
		WriterCount.Add(1)
	} else {
		ReaderCount.Add(1)
	}
	if debugTrackTxns && db.captureTxStack && stack == nil {
		stack = debug.Stack()
	}
	var btx *bbolt.Tx
	if p, ok := stx.(interface{ BoltTx() *bbolt.Tx }); ok {
		btx = p.BoltTx()
	}
	tx := &Tx{
		db:        db,
		stx:       stx,
		btx:       btx,
		managed:   managed,
		memo:      memo,
		startTime: time.Now(),
		stack:     stack,
		logger:    slog.Default(),
	}
	if db.verbose {
		tx.verbosity = 1
	}
	if debugTrackTxns {
		db.addTx(tx)
	}
	return tx
}

// DBTx implements Txish
func (tx *Tx) DBTx() *Tx {
	return tx
}

func (tx *Tx) DB() *DB {
	if tx.closed {
		panic("tx is closed")
	}
	return tx.db
}

func (tx *Tx) StartTime() time.Time {
	return tx.startTime
}

func (tx *Tx) SetLogger(logger *slog.Logger) {
	tx.logger = logger
}

func (tx *Tx) isVerboseLoggingEnabled() bool {
	return tx.verbosity > 0
}

func (tx *Tx) BeginVerbose() {
	tx.verbosity++
}
func (tx *Tx) EndVerbose() {
	tx.verbosity--
}

func (tx *Tx) Schema() *Schema {
	if tx == nil {
		panic("tx is nil")
	}
	if tx.closed {
		panic("tx is closed")
	}
	if tx.db == nil {
		panic("db is nil??")
	}
	return tx.db.schema
}

func (tx *Tx) OnChange(opts map[*Table]ChangeFlags, f func(tx *Tx, chg *Change)) {
	tx.changeOptions = opts
	tx.changeHandler = f
}

// Tx currently implements Check-Mutate phases for writable transactions:
//
// Phase 1, Check: before any modifications are made. Runs inside bdb.Batch.
// Returning an error won't cause the entire batch to fail.
//
// Phase 2, Mutate: from the first mutation. Runs inside bdb.Batch.
// The entire transaction will be retried in case of error.
//
// TODO: split Mutate phase into Mutate and Post-Mutate phases:
//
// Phase 1, Check (initial phase): inside bdb.Batch, error does not fail batch.
//
// Phase 2, Mutate (initiated by any mutation call): inside bdb.Batch,
// error fails batch.
//
// Phase 3, Read (initiated by explicit call like Commit): mutations committed,
// outside bdb.Batch, a new read-only tx is opened on demand.
//
// Check-Mutate-Read would allow to avoid holding the batch during rendering.
//
// A read-only transaction would be a natural extension of Check-Mutate-Read
// with Check and Mutate phases skipped.
func (db *DB) Tx(writable bool, f func(tx *Tx) error) error {
	if db.IsClosed() {
		panic("database closed")
	}
	if writable {
		if db.bdb == nil {
			stx, err := db.storage.BeginTx(true)
			if err != nil {
				return err
			}
			tx := db.newTx(stx, false, nil, nil)
			defer tx.Close()

			funcErr := safelyCall(f, tx)
			if funcErr != nil && tx.written && !tx.commitDespiteErr {
				return funcErr
			}
			commitErr := tx.Commit()
			if commitErr != nil {
				return commitErr
			}
			return funcErr
		}

		var funcErr error
		var tx *Tx
		// var calls int
		var memo map[string]any
		// debug.PrintStack()
		// log.Printf("Tx.BATCH.BEGIN")
		pending := true
		db.PendingWriterCount.Add(1)
		var stack []byte
		if debugTrackTxns && db.captureTxStack {
			stack = debug.Stack()
		}
		err := db.bdb.Batch(func(btx *bbolt.Tx) error {
			if pending {
				pending = false
				db.PendingWriterCount.Add(-1)
			}

			if funcErr != nil {
				// don't retry failed transactions
				return funcErr
			}

			// calls++
			// if calls > 1 {
			// 	log.Printf("Tx.REPEAT: calls = %d, memo = %v, prev err = %v", calls, memo, funcErr)
			// } else {
			// 	log.Printf("Tx.START")
			// }
			tx = db.newTx(&boltStorageTx{btx: btx}, true, memo, stack)
			defer tx.Close()
			funcErr = safelyCall(f, tx)
			memo = tx.memo
			// log.Printf("Tx.END: calls = %d, memo = %v, w = %v, cde = %v, err = %v", calls, memo, tx.written, tx.commitDespiteErr, funcErr)
			if funcErr != nil && (!tx.written || tx.commitDespiteErr) {
				return nil
			} else {
				return funcErr
			}
		})
		// log.Printf("Tx.BATCH.END")
		tx.Close()
		if err == nil && funcErr != nil {
			err = funcErr
		}
		return err
	} else {
		if db.bdb == nil {
			stx, err := db.storage.BeginTx(false)
			if err != nil {
				return err
			}
			tx := db.newTx(stx, false, nil, nil)
			defer tx.Close()
			return f(tx)
		}

		return db.bdb.View(func(btx *bbolt.Tx) error {
			tx := db.newTx(&boltStorageTx{btx: btx}, true, nil, nil)
			defer tx.Close()
			return f(tx)
		})
	}
}

type panicked struct {
	reason interface{}
	stack  string
}

func (p panicked) Error() string {
	return fmt.Sprintf("panic: %v\n\n%s", p.reason, p.stack)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p, string(debug.Stack())}
		}
	}()
	return fn(tx)
}

func (db *DB) BeginRead() *Tx {
	if db.IsClosed() {
		panic("database closed")
	}
	stx, err := db.storage.BeginTx(false)
	if err != nil {
		panic(fmt.Errorf("failed to start reading: %w", err))
	}
	return db.newTx(stx, false, nil, nil)
}

func (db *DB) Read(f func(tx *Tx)) {
	tx := db.BeginRead()
	defer tx.Close()
	f(tx)
}
func (db *DB) ReadErr(f func(tx *Tx) error) error {
	tx := db.BeginRead()
	defer tx.Close()
	return f(tx)
}

func (db *DB) Write(f func(tx *Tx)) {
	tx := db.BeginUpdate()
	defer tx.Close()
	f(tx)
	err := tx.Commit()
	if err != nil {
		panic(fmt.Errorf("commit: %w", err))
	}
}

func (db *DB) BeginUpdate() *Tx {
	if db.IsClosed() {
		panic("database closed")
	}
	stx, err := db.storage.BeginTx(true)
	if err != nil {
		panic(fmt.Errorf("db.BeginTx(true) failed: %w", err))
	}
	return db.newTx(stx, false, nil, nil)
}

func (tx *Tx) IsWritable() bool {
	return tx.stx.Writable()
}

func (tx *Tx) CommitDespiteError() {
	tx.commitDespiteErr = true
}

func (tx *Tx) markWritten() {
	tx.written = true
}

func (tx *Tx) prepareToRead() {
	if tx.stx == nil {
		return
	}
}

func (tx *Tx) addValueBuf(buf []byte) {
	if tx.valueBufs == nil {
		tx.valueBufs = arrayOfBytesPool.Get().([][]byte)
	}
	tx.valueBufs = append(tx.valueBufs, buf)
}
func (tx *Tx) addIndexValueBuf(buf []byte) {
	if tx.indexValueBufs == nil {
		tx.indexValueBufs = arrayOfBytesPool.Get().([][]byte)
	}
	tx.indexValueBufs = append(tx.indexValueBufs, buf)
}
func (tx *Tx) addIndexKeyBuf(buf []byte) {
	if tx.indexKeyBufs == nil {
		tx.indexKeyBufs = arrayOfBytesPool.Get().([][]byte)
	}
	tx.indexKeyBufs = append(tx.indexKeyBufs, buf)
}

func (tx *Tx) Close() {
	if tx.closed {
		return
	}
	tx.closed = true
	if debugTrackTxns {
		tx.db.removeTx(tx)
	}
	if tx.stx.Writable() {
		WriterCount.Add(-1)
		tx.db.WriteCount.Add(1)
	} else {
		ReaderCount.Add(-1)
		tx.db.ReadCount.Add(1)
	}
	if !tx.managed {
		err := tx.stx.Rollback()
		ensure(err)
	}
	tx.release()
}

func (tx *Tx) release() {
	if !tx.db.reuseTxBuffers {
		// Disabled by default because we suspected database corruption caused by
		// buffer reuse.
		return
	}
	if tx.valueBufs != nil {
		for i, buf := range tx.valueBufs {
			valueBytesPool.Put(buf[:0])
			tx.valueBufs[i] = nil
		}
		arrayOfBytesPool.Put(tx.valueBufs[:0])
		tx.valueBufs = nil
	}
	if tx.indexValueBufs != nil {
		for i, buf := range tx.indexValueBufs {
			indexValueBytesPool.Put(buf[:0])
			tx.indexValueBufs[i] = nil
		}
		arrayOfBytesPool.Put(tx.indexValueBufs[:0])
		tx.indexValueBufs = nil
	}
	if tx.indexKeyBufs != nil {
		for i, buf := range tx.indexKeyBufs {
			keyBytesPool.Put(buf[:0])
			tx.indexKeyBufs[i] = nil
		}
		arrayOfBytesPool.Put(tx.indexKeyBufs[:0])
		tx.indexKeyBufs = nil
	}
}

func (tx *Tx) Commit() error {
	return tx.stx.Commit()
}

// BoltTx returns the underlying Bolt transaction if this DB uses Bolt, otherwise nil.
func (tx *Tx) BoltTx() *bbolt.Tx {
	return tx.btx
}

func (tx *Tx) GetMemo(key string) (any, bool) {
	v, found := tx.memo[key]
	return v, found
}

func (tx *Tx) Memo(key string, f func() (any, error)) (any, error) {
	v, found := tx.memo[key]
	if found {
		if e, ok := v.(error); ok {
			return nil, e
		}
		return v, nil
	}

	if tx.memo == nil {
		tx.memo = make(map[string]any)
	}

	v, err := f()
	if err != nil {
		tx.memo[key] = err
	} else {
		tx.memo[key] = v
	}
	return v, err
}

func Memo[T any](txish Txish, key string, f func() (T, error)) (T, error) {
	tx := txish.DBTx()
	v, err := tx.Memo(key, func() (any, error) {
		return f()
	})
	return v.(T), err
}
