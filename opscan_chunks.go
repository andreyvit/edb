package edb

import (
	"github.com/andreyvit/edb/kvo"
	"iter"
)

// RecordChunk is a single-pass, bounded scan in one transaction. It implements
// Txish: pass the chunk to other table operations to join in the same snapshot.
// Records and their borrowed fields remain valid only within that transaction.
type RecordChunk[Key any] struct{ *scanChunkState }

// RowChunk is the native-row counterpart of RecordChunk. Rows are decoded owned
// values; the chunk itself still belongs to its transaction.
type RowChunk[Row, Key any] struct{ *scanChunkState }

type scanChunkState struct {
	raw       RawCursor
	limit     int
	count     int
	ready     bool
	exhausted bool
	finished  bool
	more      bool
	bookmark  ScanBookmark
}

// Chunk opens a single-pass cursor yielding at most limit entries. Unlike a
// reusable scan, repeated iterations continue the same chunk. limit must be
// positive. Call Bookmark and More before closing the transaction to resume in
// a later transaction. Use ReadChunks to manage that loop automatically.
func (s RecordScan[Key]) Chunk(limit int) *RecordChunk[Key] {
	return &RecordChunk[Key]{s.chunk(limit)}
}
func (s RowScan[Row, Key]) Chunk(limit int) *RowChunk[Row, Key] {
	return &RowChunk[Row, Key]{s.chunk(limit)}
}
func (s typedScan[Key]) chunk(limit int) *scanChunkState {
	if limit <= 0 {
		panic("edb: chunk size must be positive")
	}
	return &scanChunkState{raw: s.Raw(), limit: limit, bookmark: s.opt.resume}
}

// Tx returns the current chunk's transaction. It must not be closed or replaced
// while iterating the chunk. DBTx also makes the chunk usable as a Txish.
func (c *scanChunkState) Tx() *Tx   { return c.raw.Tx() }
func (c *scanChunkState) DBTx() *Tx { return c.raw.Tx() }

// Count returns the number of entries consumed from this chunk, including
// entries rejected by application filters.
func (c *scanChunkState) Count() int { return c.count }

// Bookmark owns the last consumed position. An unconsumed chunk returns its
// starting bookmark. Capture it before closing the transaction.
func (c *scanChunkState) Bookmark() ScanBookmark {
	if c.finished || c.count == 0 {
		return c.bookmark
	}
	return c.raw.Bookmark()
}

// More finishes the chunk and checks for another matching entry without loading
// its row. After More, iteration is no longer allowed; Bookmark remains usable.
// This is a statement about this transaction's snapshot, not future writes.
func (c *scanChunkState) More() bool {
	if !c.finished {
		c.bookmark = c.Bookmark()
		c.more = !c.exhausted && (c.ready || c.raw.Next())
		c.finished = true
	}
	return c.more
}

func (c *scanChunkState) next() bool {
	if c.finished {
		panic("edb: scan chunk already finished")
	}
	if c.count == c.limit || c.exhausted {
		return false
	}
	if c.ready {
		c.ready = false
	} else if !c.raw.Next() {
		c.exhausted = true
		return false
	}
	c.count++
	return true
}

func (c *RecordChunk[Key]) Keys() iter.Seq[Key]   { return chunkKeys[Key](c.scanChunkState) }
func (c *RowChunk[Row, Key]) Keys() iter.Seq[Key] { return chunkKeys[Key](c.scanChunkState) }
func chunkKeys[Key any](c *scanChunkState) iter.Seq[Key] {
	return func(yield func(Key) bool) {
		for c.next() {
			if !yield(decodeTypedKey[Key](c.raw.Table(), c.raw.RawKey())) {
				return
			}
		}
	}
}
func (c *RecordChunk[Key]) Records() iter.Seq[kvo.Record] {
	return func(yield func(kvo.Record) bool) {
		for c.next() {
			if !yield(c.record()) {
				return
			}
		}
	}
}
func (c *RecordChunk[Key]) Entries() iter.Seq2[Key, kvo.Record] {
	return func(yield func(Key, kvo.Record) bool) {
		for c.next() {
			if !yield(decodeTypedKey[Key](c.raw.Table(), c.raw.RawKey()), c.record()) {
				return
			}
		}
	}
}
func (c *scanChunkState) record() kvo.Record {
	r, _, err := c.raw.Tx().record(c.raw.Table(), c.raw.RawKey(), c.raw.RawRow())
	if err != nil {
		panic(err)
	}
	return r
}
func (c *RowChunk[Row, Key]) Rows() iter.Seq[*Row] {
	return func(yield func(*Row) bool) {
		for c.next() {
			if !yield((Cursor[Row]{c.raw}).Row()) {
				return
			}
		}
	}
}
func (c *RowChunk[Row, Key]) Entries() iter.Seq2[Key, *Row] {
	return func(yield func(Key, *Row) bool) {
		for c.next() {
			if !yield(decodeTypedKey[Key](c.raw.Table(), c.raw.RawKey()), (Cursor[Row]{c.raw}).Row()) {
				return
			}
		}
	}
}

type chunkScan[C any] interface{ resumedChunk(int, ScanBookmark) C }
type scanChunk interface{ state() *scanChunkState }

func (c *scanChunkState) state() *scanChunkState { return c }
func (s RecordScan[Key]) resumedChunk(limit int, after ScanBookmark) *RecordChunk[Key] {
	if !after.IsZero() {
		s = s.Resume(after)
	}
	return s.Chunk(limit)
}
func (s RowScan[Row, Key]) resumedChunk(limit int, after ScanBookmark) *RowChunk[Row, Key] {
	if !after.IsZero() {
		s = s.Resume(after)
	}
	return s.Chunk(limit)
}

// ReadChunks yields nonempty chunks, opening and closing a read transaction for
// each one. query must return the same query using the supplied transaction on
// each call; an initial Resume bookmark is honored. Both RecordScan and RowScan
// are supported, with their corresponding chunk type inferred automatically.
//
// Use the yielded chunk as Txish for joins. Breaking the outer loop, returning,
// or panicking closes its transaction. Breaking an inner iteration ends that
// chunk early and the next starts after the last consumed entry. Leaving a
// chunk entirely unconsumed stops the scan rather than looping forever.
//
// The loop body holds a read transaction; perform writes outside it. Use Chunk
// with explicit transaction boundaries for read-then-write batches.
//
// Each chunk sees a NEW snapshot. This is not a consistent whole-database
// export: writes between chunks can add, remove, or move entries. Retain records
// with Own before leaving a chunk. Never mutate the scanned index while walking
// its cursor; collect keys first, close the reader, then write.
func ReadChunks[S chunkScan[C], C scanChunk](db *DB, limit int, query func(*Tx) S) iter.Seq[C] {
	if limit <= 0 {
		panic("edb: chunk size must be positive")
	}
	return func(yield func(C) bool) {
		var after ScanBookmark
		for {
			more := false
			db.Read(func(tx *Tx) {
				chunk := query(tx).resumedChunk(limit, after)
				state := chunk.state()
				if state.Tx() != tx {
					panic("edb: chunk query must use the supplied transaction")
				}
				if !state.raw.Next() {
					return
				}
				state.ready = true
				if !yield(chunk) || state.count == 0 {
					return
				}
				more = state.More()
				after = state.Bookmark()
			})
			if !more {
				return
			}
		}
	}
}
