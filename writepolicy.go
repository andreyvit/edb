package edb

import (
	"errors"
	"fmt"
)

var ErrLocalWriteRejected = errors.New("edb: local application writes are fenced")

// LocalWritePolicy controls application mutations, not DDL, apply or index maintenance.
type LocalWritePolicy struct {
	Mode    LocalWriteMode
	Include func(ReplicationObject) bool
}

type LocalWriteMode uint8

const (
	AllowLocalWrites LocalWriteMode = iota
	RejectLocalWrites
)

func (p LocalWriteMode) String() string {
	switch p {
	case AllowLocalWrites:
		return "allow"
	case RejectLocalWrites:
		return "reject"
	default:
		return fmt.Sprintf("LocalWritePolicy(%d)", p)
	}
}

// RowMigrationPolicy controls row transforms during startup, decode and reindex.
// Skipping row migrations still permits local DDL and index-only backfills.
type RowMigrationPolicy uint8

const (
	AllowRowMigrations RowMigrationPolicy = iota
	SkipRowMigrations
)

func (p RowMigrationPolicy) String() string {
	switch p {
	case AllowRowMigrations:
		return "allow"
	case SkipRowMigrations:
		return "skip"
	default:
		return fmt.Sprintf("RowMigrationPolicy(%d)", p)
	}
}
func validateWritePolicies(opt Options) error {
	switch opt.LocalWrites.Mode {
	case AllowLocalWrites:
		if opt.LocalWrites.Include != nil {
			return errors.New("edb: local-write exceptions require reject mode")
		}
	case RejectLocalWrites:
	default:
		return fmt.Errorf("edb: invalid local-write mode %d", opt.LocalWrites.Mode)
	}
	if opt.RowMigrations != AllowRowMigrations && opt.RowMigrations != SkipRowMigrations {
		return fmt.Errorf("edb: invalid row-migration policy %v", opt.RowMigrations)
	}
	return nil
}
func (db *DB) AllowsLocalWrites() bool { return db.localWrites.Mode == AllowLocalWrites }

// AllowsRowMigrations exposes the policy for application migration schedulers.
// The application remains responsible for gating its jobs and external effects.
func (db *DB) AllowsRowMigrations() bool { return db.rowMigrations == AllowRowMigrations }
func (tx *Tx) allowsRowMigrations() bool {
	return tx.db.AllowsRowMigrations() && !tx.rawWrites
}

func (tx *Tx) checkLocalWrite(obj ReplicationObject) {
	if err := tx.replicationError(); err != nil {
		panic(err)
	}
	if tx.internalWrite || tx.db.AllowsLocalWrites() {
		return
	}
	allowed := false
	defer func() {
		if !allowed {
			tx.localWriteRejected = true
			tx.poisonReplication(ErrLocalWriteRejected)
		}
	}()
	include := tx.db.localWrites.Include
	allowed = include != nil && include(obj)
	if !allowed {
		panic(ErrLocalWriteRejected)
	}
}
