package edb

import (
	"bytes"
	"context"
	"log/slog"

	"go.etcd.io/bbolt"
)

const (
	debugLogRawScans = false
)

// RawRange defines a range of byte strings. The constructors use mnemonics:
// O means open, I means inclusive, E means exclusive; the first letter is for
// the lower bound, the second for the upper bound.
type RawRange struct {
	Prefix   []byte
	Lower    []byte
	Upper    []byte
	LowerInc bool
	UpperInc bool
	Reverse  bool
}

func RawOO() RawRange            { return RawRange{} }
func RawIO(l []byte) RawRange    { return RawRange{Lower: l, LowerInc: true} }
func RawEO(l []byte) RawRange    { return RawRange{Lower: l, LowerInc: false} }
func RawOI(u []byte) RawRange    { return RawRange{Upper: u, UpperInc: true} }
func RawOE(u []byte) RawRange    { return RawRange{Upper: u, UpperInc: false} }
func RawII(l, u []byte) RawRange { return RawRange{Lower: l, Upper: u, LowerInc: true, UpperInc: true} }
func RawIE(l, u []byte) RawRange {
	return RawRange{Lower: l, Upper: u, LowerInc: true, UpperInc: false}
}
func RawEI(l, u []byte) RawRange {
	return RawRange{Lower: l, Upper: u, LowerInc: false, UpperInc: true}
}
func RawEE(l, u []byte) RawRange {
	return RawRange{Lower: l, Upper: u, LowerInc: false, UpperInc: false}
}
func RawPrefix(p []byte) RawRange                { return RawRange{Prefix: p} }
func (rang RawRange) Prefixed(p []byte) RawRange { rang.Prefix = p; return rang }
func (rang RawRange) Reversed() RawRange         { rang.Reverse = true; return rang }

func (rang *RawRange) items(buck *bbolt.Bucket) func(yield func(k, v []byte) bool) {
	return func(yield func(k, v []byte) bool) {
		c := buck.Cursor()
		for k, v := rang.start(c); k != nil; k, v = rang.next(c) {
			if !yield(k, v) {
				return
			}
		}
	}
}

func (r *RawRange) start(bcur *bbolt.Cursor) ([]byte, []byte) {
	var k, v []byte
	if r.Reverse {
		upper := r.Upper
		if r.Prefix != nil {
			if upper == nil || bytes.Compare(r.Prefix, upper) < 0 {
				upper = r.Prefix
			}
		}
		if upper != nil {
			k, v = boltSeekLast(bcur, upper)
			if debugLogRawScans {
				slog.LogAttrs(context.Background(), slog.LevelDebug, "SEEK to upper", hexAttr("upper", upper), hexAttr("key", k), hexAttr("val", v))
			}
		} else {
			k, v = bcur.Last()
			if debugLogRawScans {
				slog.LogAttrs(context.Background(), slog.LevelDebug, "LAST", hexAttr("key", k), hexAttr("val", v))
			}
		}
	} else {
		lower := r.Lower
		if r.Prefix != nil && (lower == nil || bytes.Compare(r.Prefix, lower) > 0) {
			lower = r.Prefix
		}
		if lower != nil {
			k, v = bcur.Seek(lower)
			if debugLogRawScans {
				slog.LogAttrs(context.Background(), slog.LevelDebug, "SEEK to lower", hexAttr("lower", lower), hexAttr("key", k), hexAttr("val", v))
			}
		} else {
			k, v = bcur.First()
			if debugLogRawScans {
				slog.LogAttrs(context.Background(), slog.LevelDebug, "FIRST", hexAttr("key", k), hexAttr("val", v))
			}
		}
	}
	if k != nil && r.match(k, v) {
		return k, v
	} else {
		return nil, nil
	}
}

func (r *RawRange) next(bcur *bbolt.Cursor) ([]byte, []byte) {
	var k, v []byte
	if r.Reverse {
		k, v := bcur.Prev()
		if debugLogRawScans {
			slog.LogAttrs(context.Background(), slog.LevelDebug, "PREV", hexAttr("key", k), hexAttr("val", v))
		}
	} else {
		k, v = bcur.Next()
		if debugLogRawScans {
			slog.LogAttrs(context.Background(), slog.LevelDebug, "NEXT", hexAttr("key", k), hexAttr("val", v))
		}
	}
	if k != nil && r.match(k, v) {
		return k, v
	} else {
		return nil, nil
	}
}

func (r *RawRange) match(k, v []byte) bool {
	if r.Prefix != nil && !bytes.HasPrefix(k, r.Prefix) {
		if debugLogRawScans {
			slog.LogAttrs(context.Background(), slog.LevelDebug, "BAIL on prefix", hexAttr("prefix", r.Prefix), hexAttr("key", k), hexAttr("val", v))
		}
		return false
	}
	if r.Reverse {
		if lower := r.Lower; lower != nil {
			cmp := bytes.Compare(k, lower)
			if cmp == -1 || (cmp == 0 && !r.LowerInc) {
				if debugLogRawScans {
					slog.LogAttrs(context.Background(), slog.LevelDebug, "BAIL on lower", hexAttr("lower", lower), hexAttr("key", k), hexAttr("val", v))
				}
				return false
			}
		}
	} else {
		if upper := r.Upper; upper != nil {
			cmp := bytes.Compare(k, upper)
			if cmp == 1 || (cmp == 0 && !r.UpperInc) {
				if debugLogRawScans {
					slog.LogAttrs(context.Background(), slog.LevelDebug, "BAIL on upper", hexAttr("upper", upper), hexAttr("key", k), hexAttr("val", v))
				}
				return false
			}
		}
	}
	if debugLogRawScans {
		slog.LogAttrs(context.Background(), slog.LevelDebug, "MATCH", hexAttr("key", k), hexAttr("val", v))
	}
	return true
}

func (rang *RawRange) newCursor(bcur *bbolt.Cursor) *RawRangeCursor {
	return &RawRangeCursor{rang: *rang, bcur: bcur}
}

type RawRangeCursor struct {
	rang RawRange
	bcur *bbolt.Cursor
	k, v []byte
	init bool
}

func (c *RawRangeCursor) Next() bool {
	if c.init {
		c.k, c.v = c.rang.next(c.bcur)
	} else {
		c.init = true
		c.k, c.v = c.rang.start(c.bcur)
	}
	return c.k != nil
}

func (c *RawRangeCursor) Key() []byte   { return c.k }
func (c *RawRangeCursor) Value() []byte { return c.v }
