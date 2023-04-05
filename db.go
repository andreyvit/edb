package edb

import (
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type DB struct {
	bdb     *bbolt.DB
	schema  *Schema
	logf    func(format string, args ...any)
	verbose bool
	strict  bool

	tableStates []*tableState
}

type Options struct {
	Logf      func(format string, args ...any)
	Verbose   bool
	IsTesting bool
}

func Open(path string, schema *Schema, opt Options) (*DB, error) {
	bopt := &bbolt.Options{
		Timeout: 10 * time.Second,
	}
	*bopt = *bbolt.DefaultOptions
	if opt.IsTesting {
		bopt.NoSync = true
		bopt.NoFreelistSync = true
		bopt.InitialMmapSize = 1024 * 1024
	} else {
		bopt.InitialMmapSize = 1024 * 1024 * 1024
		bopt.FreelistType = bbolt.FreelistMapType
	}

	bdb, err := bbolt.Open(path, 0666, bopt)
	if err != nil {
		return nil, fmt.Errorf("kvdb: %w", err)
	}

	db := &DB{
		bdb:         bdb,
		schema:      schema,
		logf:        opt.Logf,
		verbose:     opt.Verbose,
		tableStates: make([]*tableState, len(schema.tables)),
		strict:      opt.IsTesting,
	}

	db.Write(func(tx *Tx) {
		now := time.Now()
		for i, tbl := range schema.tables {
			db.tableStates[i] = prepareTable(tx, tbl, now)
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

func (db *DB) Close() {
	err := db.bdb.Close()
	if err != nil {
		panic(fmt.Errorf("kvdb: closing: %w", err))
	}
}
