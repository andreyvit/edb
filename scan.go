package edb

import (
	"bytes"
	"context"
	"log/slog"
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

// func (rang *RawRange) items(buck *bbolt.Bucket) func(yield func(k, v []byte) bool) {
// 	return func(yield func(k, v []byte) bool) {
// 		c := buck.Cursor()
// 		for k, v := rang.start(c); k != nil; k, v = rang.next(c) {
// 			if !yield(k, v) {
// 				return
// 			}
// 		}
// 	}
// }

func (r *RawRange) start(bcur storageCursor, logger *slog.Logger) ([]byte, []byte) {
	var k, v []byte
	var skipInitial bool
	if r.Reverse {
		upper := r.Upper
		if upper != nil {
			skipInitial = !r.UpperInc
			if r.Prefix != nil && !bytes.HasPrefix(upper, r.Prefix) {
				panic("upper bound does not match prefix")
			}
		} else if r.Prefix != nil {
			upper = r.Prefix
		}
		if upper != nil {
			k, v = bcur.SeekLast(upper)
			if debugLogRawScans {
				logger.LogAttrs(context.Background(), slog.LevelDebug, "SEEK to upper", hexAttr("upper", upper), hexAttr("key", k), hexAttr("val", v))
			}
			if skipInitial && !bytes.HasPrefix(k, upper) {
				skipInitial = false
			}
		} else {
			k, v = bcur.Last()
			if debugLogRawScans {
				logger.LogAttrs(context.Background(), slog.LevelDebug, "LAST", hexAttr("key", k), hexAttr("val", v))
			}
		}
	} else {
		lower := r.Lower
		if lower != nil {
			skipInitial = !r.LowerInc
			if r.Prefix != nil && !bytes.HasPrefix(lower, r.Prefix) {
				panic("lower bound does not match prefix")
			}
		} else if r.Prefix != nil {
			lower = r.Prefix
		}
		if lower != nil {
			if debugLogRawScans {
				logger.LogAttrs(context.Background(), slog.LevelDebug, "ALL KEYS:")
				var i int
				for k2, _ := bcur.First(); k2 != nil; k2, _ = bcur.Next() {
					i++
					logger.LogAttrs(context.Background(), slog.LevelDebug, "found", hexAttr("key", k2), slog.Int("i", i))
				}
			}
			k, v = bcur.Seek(lower)
			if debugLogRawScans {
				logger.LogAttrs(context.Background(), slog.LevelDebug, "SEEK to lower", hexAttr("lower", lower), hexAttr("key", k), hexAttr("val", v))
			}
			if skipInitial && !bytes.HasPrefix(k, lower) {
				skipInitial = false
			}
		} else {
			k, v = bcur.First()
			if debugLogRawScans {
				logger.LogAttrs(context.Background(), slog.LevelDebug, "FIRST", hexAttr("key", k), hexAttr("val", v))
			}
		}
	}
	if k != nil && r.match(k, v, logger) {
		if skipInitial {
			if debugLogRawScans {
				logger.LogAttrs(context.Background(), slog.LevelDebug, "SKIP_INITIAL")
			}
			return r.next(bcur, logger)
		} else {
			return k, v
		}
	} else {
		return nil, nil
	}
}

func (r *RawRange) next(bcur storageCursor, logger *slog.Logger) ([]byte, []byte) {
	var k, v []byte
	if r.Reverse {
		k, v = bcur.Prev()
		if debugLogRawScans {
			logger.LogAttrs(context.Background(), slog.LevelDebug, "PREV", hexAttr("key", k), hexAttr("val", v))
		}
	} else {
		k, v = bcur.Next()
		if debugLogRawScans {
			logger.LogAttrs(context.Background(), slog.LevelDebug, "NEXT", hexAttr("key", k), hexAttr("val", v))
		}
	}
	if k != nil && r.match(k, v, logger) {
		return k, v
	} else {
		return nil, nil
	}
}

func (r *RawRange) match(k, v []byte, logger *slog.Logger) bool {
	if r.Prefix != nil && !bytes.HasPrefix(k, r.Prefix) {
		if debugLogRawScans {
			logger.LogAttrs(context.Background(), slog.LevelDebug, "BAIL on prefix", hexAttr("prefix", r.Prefix), hexAttr("key", k), hexAttr("val", v))
		}
		return false
	}
	if r.Reverse {
		if lower := r.Lower; lower != nil {
			cmp := bytes.Compare(k, lower)
			if cmp == -1 || (cmp == 0 && !r.LowerInc) {
				if debugLogRawScans {
					logger.LogAttrs(context.Background(), slog.LevelDebug, "BAIL on lower", hexAttr("lower", lower), hexAttr("key", k), hexAttr("val", v))
				}
				return false
			}
		}
	} else {
		if upper := r.Upper; upper != nil {
			cmp := bytes.Compare(k, upper)
			if cmp == 1 || (cmp == 0 && !r.UpperInc) {
				if debugLogRawScans {
					logger.LogAttrs(context.Background(), slog.LevelDebug, "BAIL on upper", hexAttr("upper", upper), hexAttr("key", k), hexAttr("val", v))
				}
				return false
			}
		}
	}
	if debugLogRawScans {
		logger.LogAttrs(context.Background(), slog.LevelDebug, "MATCH", hexAttr("key", k), hexAttr("val", v))
	}
	return true
}

func (rang *RawRange) newCursor(bcur storageCursor, logger *slog.Logger) *RawRangeCursor {
	return &RawRangeCursor{rang: *rang, bcur: bcur, logger: logger}
}

type RawRangeCursor struct {
	rang   RawRange
	bcur   storageCursor
	logger *slog.Logger
	k, v   []byte
	init   bool
}

func (c *RawRangeCursor) Next() bool {
	if c.init {
		c.k, c.v = c.rang.next(c.bcur, c.logger)
	} else {
		c.init = true
		c.k, c.v = c.rang.start(c.bcur, c.logger)
	}
	return c.k != nil
}

func (c *RawRangeCursor) Key() []byte   { return c.k }
func (c *RawRangeCursor) Value() []byte { return c.v }
