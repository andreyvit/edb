package edb

import (
	"log"
	"reflect"
	"time"

	"go.etcd.io/bbolt"
)

func (db *DB) tableState(tbl *Table) *tableState {
	return db.tableStates[tbl.pos]
}

type tableState struct {
	MinSchemaVer     uint64                 `msgpack:"s"`
	LastIndexOrdinal uint64                 `msgpack:"li"`
	Indices          map[string]*indexState `msgpack:"i"`
	LastSeen         time.Time              `msgpack:"t"`
	DeletionCounter  int                    `msgpack:"delcnt,omitempty"`

	table            *Table                 `msgpack:"-"`
	indexStates      []*indexState          `msgpack:"-"`
	indexStatesByOrd map[uint64]*indexState `msgpack:"-"`
}

func (ts *tableState) indexOrdinal(idx *Index) uint64 {
	return ts.indexStates[idx.pos].IndexOrdinal
}

func (ts *tableState) indexByOrdinal(ord uint64) *Index {
	is := ts.indexStatesByOrd[ord]
	if is == nil {
		return nil
	}
	return is.index
}

func (ts *tableState) hasPendingIndices() bool {
	for _, is := range ts.Indices {
		if !is.Built {
			return true
		}
	}
	return false
}

type indexState struct {
	index        *Index `msgpack:"-"`
	IndexOrdinal uint64 `msgpack:"o"`
	Built        bool   `msgpack:"f"`
}

var tableStateKey = []byte("_state")

const tableStateEncoding = MsgPack

func prepareTable(tx *Tx, tbl *Table, now time.Time) *tableState {
	tableRootB := must(tx.btx.CreateBucketIfNotExists(tbl.buck.Raw()))
	_ = must(tableRootB.CreateBucketIfNotExists(dataBucket.Raw()))
	for _, idx := range tbl.indices {
		_ = must(tableRootB.CreateBucketIfNotExists(idx.buck.Raw()))
	}

	ts := new(tableState)
	if rawTS := tableRootB.Get(tableStateKey); rawTS != nil {
		err := tableStateEncoding.DecodeValue(rawTS, reflect.ValueOf(ts))
		if err != nil {
			panic(tableErrf(tbl, nil, nil, err, "failed to decode table state"))
		}
	}
	ts.table = tbl
	if ts.Indices == nil {
		ts.Indices = make(map[string]*indexState)
	}

	ts.LastSeen = now
	ts.indexStates = make([]*indexState, len(tbl.indices))
	ts.indexStatesByOrd = make(map[uint64]*indexState)

	for i, idx := range tbl.indices {
		is := ts.Indices[idx.name]
		if is == nil {
			ts.LastIndexOrdinal++
			is = &indexState{
				IndexOrdinal: ts.LastIndexOrdinal,
			}
			ts.Indices[idx.name] = is
		}
		is.index = idx
		ts.indexStates[i] = is
		ts.indexStatesByOrd[is.IndexOrdinal] = is
	}
	for k, is := range ts.Indices {
		if is.index == nil {
			dropDeletedIndex(tbl, tableRootB, k)
			delete(ts.Indices, k)
		}
	}
	return ts
}

func (ts *tableState) migrate(tx *Tx) {
	tbl := ts.table
	if ts.hasPendingIndices() {
		log.Printf("Re-indexing table %s...", tbl.Name())
		start := time.Now()
		var rows int64
		for c := tx.TableScan(tbl, FullScan()); c.Next(); {
			rowVal, _ := c.RowVal()
			tx.PutVal(tbl, rowVal)
			rows++
			if rows%100000 == 0 {
				log.Printf("Still re-indexing %s, so far updated %d rows in %d ms", tbl.Name(), rows, time.Since(start).Milliseconds())
			}
		}
		for _, is := range ts.Indices {
			is.Built = true
		}
		log.Printf("Re-indexing of %d rows in %s took %d ms", rows, tbl.Name(), time.Since(start).Milliseconds())
	}
}

func (ts *tableState) save(tx *Tx) {
	// log.Printf("table state for %s: %s", ts.table.Name(), must(json.Marshal(ts)))
	rawTS := tableStateEncoding.EncodeValue(nil, reflect.ValueOf(ts))
	tableRootB := ts.table.rootBucketIn(tx.btx)
	ensure(tableRootB.Put(tableStateKey, rawTS))
}

func dropDeletedIndex(tbl *Table, tableRootB *bbolt.Bucket, name string) {
	err := tableRootB.DeleteBucket(makeIndexBucketName(name).Raw())
	if err == bbolt.ErrBucketNotFound {
		return
	}
	ensure(err)
	log.Printf("deleted index %s.%s", tbl.Name(), name)
}

func prepareMap(tx *Tx, mp *KVMap) {
	must(tx.btx.CreateBucketIfNotExists(mp.buck.Raw()))
}
