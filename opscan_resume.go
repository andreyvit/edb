package edb

import "bytes"

// ScanBookmark is an owned, immutable position in a table or index scan. Its
// zero value starts at the beginning. It can outlive the cursor and transaction.
// Nonunique index positions include both the index key and primary key; a
// primary key alone cannot resume a general index scan.
//
// A bookmark belongs to its table/index and traversal direction. Resume keeps
// the new scan's bounds, so it can also narrow the original query. Bookmarks are
// process-local, not durable pagination tokens or snapshots. Between read
// transactions, inserts behind the position are missed and entries moved ahead
// of it can be visited again. Collect keys first when changing a scanned index.
type ScanBookmark struct {
	table    *TableDef
	index    *IndexDef
	position string
	reverse  bool
}

func (b ScanBookmark) IsZero() bool { return b.table == nil }

func (b ScanBookmark) check(table *TableDef, index *IndexDef, reverse bool) {
	if !b.IsZero() && (b.table != table || b.index != index || b.reverse != reverse) {
		panic("edb: scan bookmark belongs to another table, index, or direction")
	}
}

// Resume continues exclusively after the bookmark in traversal order, retaining
// all query bounds. A zero bookmark leaves the scan unchanged.
func (so ScanOptions) Resume(b ScanBookmark) ScanOptions {
	so.resume = b
	return so
}

// Bookmark copies the last visited position, including after exhaustion. Call
// it before closing the transaction. It does not fetch or decode a row.
func (c *RawTableCursor) Bookmark() ScanBookmark {
	if c.k == nil {
		return ScanBookmark{}
	}
	return ScanBookmark{table: c.table, position: string(c.k), reverse: c.reverse}
}

// Bookmark owns the complete index position. For nonunique indexes the primary
// key is part of the encoded position; unique indexes sort by index key alone.
func (c *RawIndexCursor) Bookmark() ScanBookmark {
	if c.ik == nil {
		return ScanBookmark{}
	}
	return ScanBookmark{table: c.table, index: c.index, position: string(c.ik), reverse: c.reverse}
}

func (s RecordScan[Key]) Resume(b ScanBookmark) RecordScan[Key] {
	s.opt = s.opt.Resume(b)
	return s
}

func (s RowScan[Row, Key]) Resume(b ScanBookmark) RowScan[Row, Key] {
	s.opt = s.opt.Resume(b)
	return s
}

// resumeStorageCursor changes only the initial seek. Existing strategies still
// enforce their own bounds and prefixes, and subsequent steps use the original
// cursor directly, without another interface call or comparison per row.
type resumeStorageCursor struct {
	storageCursor
	after string
}

func (c *resumeStorageCursor) First() ([]byte, []byte) {
	k, v := c.storageCursor.Seek([]byte(c.after))
	if bytes.Equal(k, []byte(c.after)) {
		return c.storageCursor.Next()
	}
	return k, v
}

func (c *resumeStorageCursor) Last() ([]byte, []byte) {
	k, _ := c.storageCursor.Seek([]byte(c.after))
	if k == nil {
		return c.storageCursor.Last()
	}
	return c.storageCursor.Prev()
}

func (c *resumeStorageCursor) Seek(key []byte) ([]byte, []byte) {
	if bytes.Compare(key, []byte(c.after)) <= 0 {
		return c.First()
	}
	return c.storageCursor.Seek(key)
}

func (c *resumeStorageCursor) SeekLast(prefix []byte) ([]byte, []byte) {
	k, v := c.storageCursor.SeekLast(prefix)
	if k != nil && bytes.Compare(k, []byte(c.after)) >= 0 {
		return c.Last()
	}
	return k, v
}
