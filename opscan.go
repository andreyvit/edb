package edb

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"go.etcd.io/bbolt"
)

const (
	debugLogIndexScans = false
	debugLogTableScans = false
)

type Cursor[Row any] struct {
	RawCursor
}

func (c Cursor[Row]) Raw() RawCursor {
	return c.RawCursor
}

func (c Cursor[Row]) Next() bool {
	return c.RawCursor.Next()
}

func (c Cursor[Row]) Key() any {
	return c.RawCursor.Key()
}

func (c Cursor[Row]) RowVal() (reflect.Value, ValueMeta) {
	return c.RawCursor.RowVal()
}

func (c Cursor[Row]) Row() *Row {
	rowVal, _ := c.RowVal()
	return valToRow[Row](rowVal)
}

func (c Cursor[Row]) Meta() ValueMeta {
	return c.RawCursor.Meta()
}

func TableScan[Row any](txh Txish, opt ScanOptions) Cursor[Row] {
	tx := txh.DBTx()
	tbl := tableOf[Row](tx)
	return Cursor[Row]{tx.TableScan(tbl, opt)}
}

func (tx *Tx) TableScan(tbl *Table, opt ScanOptions) *RawTableCursor {
	return tx.newTableCursor(tbl, opt)
}

func FullTableScan[Row any](txh Txish) Cursor[Row] {
	return TableScan[Row](txh, FullScan())
}

func FullReverseTableScan[Row any](txh Txish) Cursor[Row] {
	return TableScan[Row](txh, FullScan().Reversed())
}

func RangeTableScan[Row any](txh Txish, lowerValue, upperValue any, lowerInc, upperInc bool) Cursor[Row] {
	return TableScan[Row](txh, RangeScan(lowerValue, upperValue, lowerInc, upperInc))
}
func ReverseRangeTableScan[Row any](txh Txish, lowerValue, upperValue any, lowerInc, upperInc bool) Cursor[Row] {
	return TableScan[Row](txh, RangeScan(lowerValue, upperValue, lowerInc, upperInc).Reversed())
}

func ExactTableScan[Row any](txh Txish, value any) Cursor[Row] {
	return TableScan[Row](txh, RangeScan(value, value, true, true))
}

func IndexScan[Row any](txh Txish, idx *Index, opt ScanOptions) Cursor[Row] {
	tx := txh.DBTx()
	tbl := tableOf[Row](tx)
	if tbl != idx.table {
		if idx.table == nil {
			panic(fmt.Errorf("index %v has not been added to table %v", idx.ShortName(), tbl.Name()))
		}
		panic(fmt.Errorf("row refers to table %v, but index is on table %v", tbl.Name(), idx.table.Name()))
	}
	return Cursor[Row]{tx.IndexScan(idx, opt)}
}

func FullIndexScan[Row any](txh Txish, idx *Index) Cursor[Row] {
	return IndexScan[Row](txh, idx, FullScan())
}

func ExactIndexScan[Row any](txh Txish, idx *Index, indexValue any) Cursor[Row] {
	return IndexScan[Row](txh, idx, ExactScan(indexValue))
}
func ReverseExactIndexScan[Row any](txh Txish, idx *Index, indexValue any) Cursor[Row] {
	return IndexScan[Row](txh, idx, ExactScan(indexValue).Reversed())
}

func RangeIndexScan[Row any](txh Txish, idx *Index, lowerValue, upperValue any, lowerInc, upperInc bool) Cursor[Row] {
	return IndexScan[Row](txh, idx, RangeScan(lowerValue, upperValue, lowerInc, upperInc))
}
func ReverseRangeIndexScan[Row any](txh Txish, idx *Index, lowerValue, upperValue any, lowerInc, upperInc bool) Cursor[Row] {
	return IndexScan[Row](txh, idx, RangeScan(lowerValue, upperValue, lowerInc, upperInc).Reversed())
}

func PrefixIndexScan[Row any](txh Txish, idx *Index, els int, indexValue any) Cursor[Row] {
	return IndexScan[Row](txh, idx, ExactScan(indexValue).Prefix(els))
}
func ReversePrefixIndexScan[Row any](txh Txish, idx *Index, els int, indexValue any) Cursor[Row] {
	return IndexScan[Row](txh, idx, ExactScan(indexValue).Prefix(els).Reversed())
}

func (tx *Tx) IndexScan(idx *Index, opt ScanOptions) *RawIndexCursor {
	return tx.newIndexCursor(idx, opt)
}

func AllTableRows[Row any](txh Txish) []*Row {
	return All(TableScan[Row](txh, FullScan()))
}

func All[Row any](c Cursor[Row]) []*Row {
	var result []*Row
	for c.Next() {
		result = append(result, c.Row())
	}
	return result
}

func AllLimited[Row any](c Cursor[Row], limit int) []*Row {
	var result []*Row
	for c.Next() {
		result = append(result, c.Row())
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result
}

func AllKeys[Key any](c RawCursor) []Key {
	var result []Key
	for c.Next() {
		result = append(result, c.Key().(Key))
	}
	return result
}

func AllUntypedKeys(c RawCursor) []any {
	var result []any
	for c.Next() {
		result = append(result, c.Key())
	}
	return result
}

func AllRawKeys(c RawCursor) [][]byte {
	var result [][]byte
	for c.Next() {
		result = append(result, c.RawKey())
	}
	return result
}

func First[Row any](c Cursor[Row]) *Row {
	if c.Next() {
		return c.Row()
	}
	return nil
}

func Select[Row any](c Cursor[Row], f func(*Row) bool) *Row {
	for c.Next() {
		row := c.Row()
		if f == nil || f(row) {
			return row
		}
	}
	return nil
}

func Filter[Row any](c Cursor[Row], f func(*Row) bool) []*Row {
	var result []*Row
	for c.Next() {
		row := c.Row()
		if f(row) {
			result = append(result)
		}
	}
	return result
}

func Count(c RawCursor) int {
	var count int
	for c.Next() {
		count++
	}
	return count
}

type ScanMethod int

const (
	ScanMethodFull = ScanMethod(iota)
	ScanMethodExact
	ScanMethodRange
)

type ScanOptions struct {
	Reverse  bool
	Method   ScanMethod
	Lower    reflect.Value
	Upper    reflect.Value
	LowerInc bool
	UpperInc bool
	Els      int
}

func (so ScanOptions) Reversed() ScanOptions {
	so.Reverse = true
	return so
}

func (so ScanOptions) Prefix(els int) ScanOptions {
	so.Els = els
	return so
}

func (so ScanOptions) LogString() string {
	var buf strings.Builder
	if so.Reverse {
		buf.WriteString("reverse:")
	}
	switch so.Method {
	case ScanMethodFull:
		buf.WriteString("full")
	case ScanMethodExact:
		buf.WriteString("exact:")
		buf.WriteString(loggableVal(so.Lower))
	case ScanMethodRange:
		buf.WriteString("range")
		if so.LowerInc {
			buf.WriteByte('[')
		} else {
			buf.WriteByte('(')
		}
		buf.WriteString(loggableVal(so.Lower))
		buf.WriteString(":")
		buf.WriteString(loggableVal(so.Upper))
		if so.UpperInc {
			buf.WriteByte(']')
		} else {
			buf.WriteByte(')')
		}
	default:
		buf.WriteString("unknown")
	}
	if so.Els != 0 {
		buf.WriteByte(':')
		buf.WriteString(strconv.Itoa(so.Els))
	}
	return buf.String()
}

func FullScan() ScanOptions {
	return ScanOptions{Method: ScanMethodFull}
}

func ExactScan(v any) ScanOptions {
	return ScanOptions{Method: ScanMethodExact, Lower: reflect.ValueOf(v)}
}
func ExactScanVal(val reflect.Value) ScanOptions {
	return ScanOptions{Method: ScanMethodExact, Lower: val}
}

func LowerBoundScan(lower any, includeEqual bool) ScanOptions {
	return RangeScan(lower, nil, includeEqual, false)
}
func UpperBoundScan(upper any, includeEqual bool) ScanOptions {
	return RangeScan(nil, upper, false, includeEqual)
}
func RangeScan(lower, upper any, lowerInc, upperInc bool) ScanOptions {
	var lowerVal, upperVal reflect.Value
	if lower != nil {
		lowerVal = reflect.ValueOf(lower)
	}
	if upper != nil {
		upperVal = reflect.ValueOf(upper)
	}
	return RangeScanVal(lowerVal, upperVal, lowerInc, upperInc)
}
func RangeScanVal(lower, upper reflect.Value, lowerInc, upperInc bool) ScanOptions {
	return ScanOptions{Method: ScanMethodRange, Lower: lower, Upper: upper, LowerInc: lowerInc, UpperInc: upperInc}
}

type RawCursor interface {
	Table() *Table
	Tx() *Tx
	Next() bool
	Key() any
	RawKey() []byte
	RowVal() (reflect.Value, ValueMeta)
	Meta() ValueMeta
	Row() (any, ValueMeta)
}

type RawTableCursor struct {
	tx       *Tx
	table    *Table
	dcur     *bbolt.Cursor
	prefix   []byte
	lower    []byte
	upper    []byte
	lowerInc bool
	upperInc bool
	init     bool
	reverse  bool
	k, v     []byte
}

func (c *RawTableCursor) Tx() *Tx {
	return c.tx
}

func (c *RawTableCursor) Table() *Table {
	return c.table
}

func (c *RawTableCursor) Next() bool {
	var k, v []byte
	if c.init {
		if c.reverse {
			k, v = c.dcur.Prev()
		} else {
			k, v = c.dcur.Next()
		}
		if debugLogTableScans {
			log.Printf("%s::TableScan: ADVC: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, c.prefix, c.reverse, k, v)
		}
	} else {
		c.init = true
		if c.reverse {
			if c.upper != nil {
				panic("reverse range scan not supported yet")
			} else {
				k, v = c.dcur.Last()
				if debugLogTableScans {
					log.Printf("%s::TableScan: LAST: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, c.prefix, c.reverse, k, v)
				}
			}
		} else {
			lower := c.lower
			if lower == nil && len(c.prefix) > 0 {
				lower = c.prefix
			}
			if lower != nil {
				k, v = c.dcur.Seek(lower)
				if debugLogTableScans {
					log.Printf("%s::TableScan: SEEK to lower = %x: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, lower, c.prefix, c.reverse, k, v)
				}
			} else {
				k, v = c.dcur.First()
				if debugLogTableScans {
					log.Printf("%s::TableScan: FIRST: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, c.prefix, c.reverse, k, v)
				}
			}
		}
	}
	if k == nil {
		if debugLogTableScans {
			log.Printf("%s::TableScan: EOFd: prefix = %x, reverse = %v", c.table.name, c.prefix, c.reverse)
		}
		return false
	}
	if p := c.prefix; p != nil {
		if len(k) < len(p) || !bytes.Equal(p, k[:len(p)]) {
			if debugLogTableScans {
				log.Printf("%s::TableScan: BAIL on prefix: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, c.prefix, c.reverse, k, v)
			}
			return false
		}
	}
	if c.reverse {
		if b := c.lower; b != nil {
			cmp := bytes.Compare(k, b)
			if cmp == -1 || (cmp == 0 && !c.lowerInc) {
				if debugLogTableScans {
					log.Printf("%s::TableScan: BAIL on lower: lower = %x, reverse = %v => k = %x, v = %x", c.table.name, c.lower, c.reverse, k, v)
				}
				return false
			}
		}
	} else {
		if b := c.upper; b != nil {
			cmp := bytes.Compare(k, b)
			if cmp == 1 || (cmp == 0 && !c.upperInc) {
				if debugLogTableScans {
					log.Printf("%s::TableScan: BAIL on upper: upper = %x, reverse = %v => k = %x, v = %x", c.table.name, c.upper, c.reverse, k, v)
				}
				return false
			}
		}
	}
	if debugLogTableScans {
		log.Printf("%s::TableScan: MTCH: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, c.prefix, c.reverse, k, v)
	}
	c.k, c.v = k, v
	return true
}

func (c *RawTableCursor) RawKey() []byte {
	return c.k
}

func (c *RawTableCursor) Key() any {
	return c.table.DecodeKeyVal(c.k).Interface()
}

func (c *RawTableCursor) RowVal() (reflect.Value, ValueMeta) {
	return decodeTableRow(c.table, c.k, c.v, c.tx)
}

func (c *RawTableCursor) Meta() ValueMeta {
	return decodeTableValue(c.table, c.k, c.v).ValueMeta()
}

func (c *RawTableCursor) Row() (any, ValueMeta) {
	rowVal, rowMeta := c.RowVal()
	return valToAny(rowVal), rowMeta
}

func (tx *Tx) newTableCursor(tbl *Table, opt ScanOptions) *RawTableCursor {
	tableBuck := nonNil(tx.btx.Bucket(tbl.buck.Raw()))
	buck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	c := &RawTableCursor{
		tx:      tx,
		table:   tbl,
		dcur:    buck.Cursor(),
		reverse: opt.Reverse,
	}
	switch opt.Method {
	case ScanMethodFull:
		break
	case ScanMethodExact:
		if !opt.Lower.IsValid() {
			panic(fmt.Errorf("Lower must be specified for ScanMethodExact"))
		}
		if at, et := opt.Lower.Type(), tbl.KeyType(); at != et {
			panic(fmt.Errorf("%s: attempted to scan table using lower bound of incorrect type %v, expected %v", tbl.Name(), at, et))
		}

		keyPrefix, _, isFull := encodeTableBoundaryKey(opt.Lower, tbl, opt.Els)
		tx.addIndexKeyBuf(keyPrefix)

		if isFull {
			c.lower = keyPrefix
			c.lowerInc = true
			c.upper = c.lower
			c.upperInc = true
		} else {
			c.prefix = keyPrefix
		}

	case ScanMethodRange:
		if opt.Lower.IsValid() {
			if at, et := opt.Lower.Type(), tbl.KeyType(); at != et {
				panic(fmt.Errorf("%s: attempted to scan table using lower bound of incorrect type %v, expected %v", tbl.Name(), at, et))
			}
			if !opt.LowerInc {
				panic("LowerInc=false not supported")
			}
			c.lower = tbl.EncodeKeyVal(opt.Lower)
			c.lowerInc = opt.LowerInc
		}
		if opt.Upper.IsValid() {
			if at, et := opt.Upper.Type(), tbl.KeyType(); at != et {
				panic(fmt.Errorf("%s: attempted to scan table using upper bound of incorrect type %v, expected %v", tbl.Name(), at, et))
			}
			c.upper = tbl.EncodeKeyVal(opt.Upper)
			c.upperInc = opt.UpperInc
		}
	default:
		panic(fmt.Errorf("unsupported scan method %v", opt.Method))
	}
	return c
}

type RawIndexCursor struct {
	table      *Table
	index      *Index
	strat      indexScanStrategy
	tx         *Tx
	icur       *bbolt.Cursor
	dbuck      *bbolt.Bucket
	prefix     []byte
	resetDone  bool
	reverse    bool
	ik, iv, dk []byte
	itup       tuple
}

func (c *RawIndexCursor) Table() *Table {
	return c.table
}

func (c *RawIndexCursor) Tx() *Tx {
	return c.tx
}

func (c *RawIndexCursor) Next() bool {
	c.ik, c.iv, c.itup, c.dk = c.strat.Next(c.icur, !c.resetDone, c.reverse, c.index)
	c.resetDone = true
	return (c.ik != nil)
}

func (c *RawIndexCursor) RawKey() []byte {
	return c.dk
}

func (c *RawIndexCursor) IndexKey() any {
	return c.index.DecodeIndexKeyVal(c.itup).Interface()
}

func (c *RawIndexCursor) Key() any {
	return c.table.DecodeKeyVal(c.dk).Interface()
}

func (c *RawIndexCursor) RowVal() (reflect.Value, ValueMeta) {
	dv := c.dbuck.Get(c.dk)
	return decodeTableRow(c.table, c.dk, dv, c.tx)
}

func (c *RawIndexCursor) Meta() ValueMeta {
	dv := c.dbuck.Get(c.dk)
	return decodeTableValue(c.table, c.dk, dv).ValueMeta()
}

func (c *RawIndexCursor) Row() (any, ValueMeta) {
	rowVal, rowMeta := c.RowVal()
	return valToAny(rowVal), rowMeta
}

func (tx *Tx) newIndexCursor(idx *Index, opt ScanOptions) *RawIndexCursor {
	idx.requireTable()
	if tx.db.verbose {
		tx.db.logf("db: INDEX_SCAN %s/%v", idx.FullName(), opt.LogString())
	}
	tableBuck := nonNil(tx.btx.Bucket(idx.table.buck.Raw()))
	ibuck := nonNil(tableBuck.Bucket(idx.buck.Raw()))
	dbuck := nonNil(tableBuck.Bucket(dataBucket.Raw()))
	var strat indexScanStrategy
	switch opt.Method {
	case ScanMethodFull:
		strat = fullIndexScanStrategy{}
	case ScanMethodExact:
		if !opt.Lower.IsValid() {
			panic(fmt.Errorf("Lower must be specified for ScanMethodExact"))
		}
		if at, et := opt.Lower.Type(), idx.keyType(); at != et {
			panic(fmt.Errorf("%s: attempted to scan index using lower bound of incorrect type %v, expected %v", idx.FullName(), at, et))
		}

		keyPrefix, keyEls, isFull := encodeIndexBoundaryKey(opt.Lower, idx, opt.Els, false)
		tx.addIndexKeyBuf(keyPrefix)

		if idx.isUnique && isFull {
			strat = &exactIndexScanStrategy{keyPrefix, keyEls}
		} else {
			strat = &prefixIndexScanStrategy{keyPrefix, keyEls}
		}
	case ScanMethodRange:
		if !opt.Lower.IsValid() && !opt.Upper.IsValid() {
			strat = fullIndexScanStrategy{}
		} else {
			var lower, upper []byte
			var els int

			if opt.Lower.IsValid() {
				if at, et := opt.Lower.Type(), idx.keyType(); at != et {
					panic(fmt.Errorf("%s: attempted to scan index using lower bound of incorrect type %v, expected %v", idx.FullName(), at, et))
				}

				lower, els, _ = encodeIndexBoundaryKey(opt.Lower, idx, opt.Els, true)
				tx.addIndexKeyBuf(lower)
			}
			if opt.Upper.IsValid() {
				if at, et := opt.Upper.Type(), idx.keyType(); at != et {
					panic(fmt.Errorf("%s: attempted to scan index using lower bound of incorrect type %v, expected %v", idx.FullName(), at, et))
				}

				var upperEls int
				upper, upperEls, _ = encodeIndexBoundaryKey(opt.Upper, idx, opt.Els, true)
				if !opt.Lower.IsValid() {
					els = upperEls
				} else if els != upperEls {
					panic(fmt.Errorf("%s: attempted to scan index using lower and upper boundaries of different prefix sizes (lower %d, upper %d)", idx.FullName(), els, upperEls))
				}
				tx.addIndexKeyBuf(upper)
			}

			strat = &rangeIndexScanStrategy{els, lower, upper, opt.LowerInc, opt.UpperInc}
		}
	default:
		panic(fmt.Errorf("unsupported scan method %v", opt.Method))
	}
	return &RawIndexCursor{
		table:   idx.table,
		index:   idx,
		tx:      tx,
		icur:    ibuck.Cursor(),
		dbuck:   dbuck,
		reverse: opt.Reverse,
		strat:   strat,
	}
}

func encodeTableBoundaryKey(keyVal reflect.Value, tbl *Table, cutoffEls int) ([]byte, int, bool) {
	keyBuf := keyBytesPool.Get().([]byte)
	fe := flatEncoder{buf: keyBuf}
	tbl.keyEnc.encodeInto(&fe, keyVal)

	keyEls, isFull := fe.count(), true
	if cutoffEls != 0 && cutoffEls < keyEls {
		keyEls, isFull = cutoffEls, false
	}

	if isFull {
		return fe.finalize(), keyEls, true
	} else {
		n := fe.prefixLen(keyEls)
		return fe.buf[:n], keyEls, false
	}
}

func encodeIndexBoundaryKey(keyVal reflect.Value, idx *Index, cutoffEls int, neverFinalize bool) ([]byte, int, bool) {
	indexKeyBuf := keyBytesPool.Get().([]byte)
	fe := flatEncoder{buf: indexKeyBuf}
	idx.keyEnc.encodeInto(&fe, keyVal)

	keyEls, isFull := fe.count(), true
	if cutoffEls != 0 && cutoffEls < keyEls {
		keyEls, isFull = cutoffEls, false
	}

	if idx.isUnique && isFull {
		if neverFinalize {
			return fe.buf, keyEls, false
		} else {
			return fe.finalize(), keyEls, true
		}
	} else if isFull {
		return fe.buf, keyEls, true
	} else {
		n := fe.prefixLen(keyEls)
		return fe.buf[:n], keyEls, false
	}
}

type indexScanStrategy interface {
	Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, tuple, []byte)
}

type fullIndexScanStrategy struct{}

func (_ fullIndexScanStrategy) Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, tuple, []byte) {
	var ik, iv []byte
	if reset {
		ik, iv = boltFirstLast(c, reverse)
	} else {
		ik, iv = boltAdvance(c, reverse)
	}
	if ik == nil {
		return nil, nil, nil, nil
	}
	iktup := decodeIndexKey(ik, idx)
	dk, itup := decodeIndexTableKey(ik, iktup, iv, idx)
	return ik, iv, itup, dk
}

type exactIndexScanStrategy struct {
	prefix []byte
	els    int
}

func (s *exactIndexScanStrategy) Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, tuple, []byte) {
	var ik, iv []byte
	if reset {
		ik, iv = boltSeek(c, s.prefix, reverse)
	} else {
		ik, iv = boltAdvance(c, reverse)
	}
	if ik != nil && bytes.HasPrefix(ik, s.prefix) {
		dk, itup := decodeIndexTableKey(ik, nil, iv, idx)
		return ik, iv, itup, dk
	} else {
		return nil, nil, nil, nil
	}
}

type prefixIndexScanStrategy struct {
	prefix []byte
	els    int
}

func (s *prefixIndexScanStrategy) Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, tuple, []byte) {
	prefix := s.prefix
	var ik, iv []byte
	if reset {
		ik, iv = boltSeek(c, prefix, reverse)
		if debugLogIndexScans {
			log.Printf("prefix index scan step: SEEK: prefix = %x, reverse = %v => ik = %x, iv = %x", prefix, reverse, ik, iv)
		}
	} else {
		if debugLogIndexScans {
			log.Printf("prefix index scan step: ADVC: reverse = %v", reverse)
		}
		ik, iv = boltAdvance(c, reverse)
	}
	for ik != nil {
		if !bytes.HasPrefix(ik, prefix) {
			if debugLogIndexScans {
				log.Printf("prefix index scan step: BAIL: ik = %x, prefix = %x", ik, prefix)
			}
			return nil, nil, nil, nil
		}
		ikTup := decodeIndexKey(ik, idx)
		if len(ikTup) < s.els {
			panic(fmt.Errorf("%s: invalid index key %x: got %d els, wanted at least %d", idx.FullName(), ik, len(ikTup), s.els+1))
		}
		if ikTup.prefixLen(s.els) == len(prefix) {
			if debugLogIndexScans {
				log.Printf("prefix index scan step: MTCH: ik = %x, iv = %q", ik, iv)
			}
			dk, itup := decodeIndexTableKey(ik, ikTup, iv, idx)
			return ik, iv, itup, dk
		}
		if debugLogIndexScans {
			log.Printf("prefix index scan step: SKIP: ik = %x, iv = %q", ik, iv)
		}
		ik, iv = boltAdvance(c, reverse)
	}
	if debugLogIndexScans {
		log.Printf("prefix index scan step: EOFd: prefix = %x", prefix)
	}
	return nil, nil, nil, nil
}

type rangeIndexScanStrategy struct {
	els      int
	lower    []byte
	upper    []byte
	lowerInc bool
	upperInc bool
}

func (s *rangeIndexScanStrategy) Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, tuple, []byte) {
	var ik, iv []byte
	var skippingInitial bool
	if reset {
		if reverse {
			if s.upper == nil {
				if debugLogIndexScans {
					log.Printf("range index scan step: SEEK_LAST")
				}
				ik, iv = c.Last()
			} else {
				if debugLogIndexScans {
					log.Printf("range index scan step: SEEK_REV: upper = %x", s.upper)
				}
				ik, iv = boltSeekLast(c, s.upper)
				if !s.upperInc {
					skippingInitial = true
				}
			}
		} else {
			if s.lower == nil {
				if debugLogIndexScans {
					log.Printf("range index scan step: SEEK_FIRST")
				}
				ik, iv = c.First()
			} else {
				if debugLogIndexScans {
					log.Printf("range index scan step: SEEK_FWD: lower = %x", s.lower)
				}
				ik, iv = c.Seek(s.lower)
				if !s.lowerInc {
					skippingInitial = true
				}
			}
		}
	} else {
		if debugLogIndexScans {
			log.Printf("range index scan step: ADVC: reverse = %v", reverse)
		}
		ik, iv = boltAdvance(c, reverse)
	}

	lower, upper := s.lower, s.upper
	for ik != nil {
		ikTup := decodeIndexKey(ik, idx)
		if len(ikTup) < s.els {
			panic(fmt.Errorf("%s: invalid index key %x: got %d els, wanted at least %d", idx.FullName(), ik, len(ikTup), s.els+1))
		}
		relevantLen := ikTup.prefixLen(s.els)
		relevant := ik[:relevantLen]

		if skippingInitial {
			if reverse {
				if bytes.Equal(relevant, s.upper) {
					if debugLogIndexScans {
						log.Printf("range index scan step: SKIP_INITIAL_EQ_UPPER: ik = %x, relevant = %x", ik, relevant)
					}
					ik, iv = c.Prev()
					continue
				} else {
					skippingInitial = false
				}
			} else {
				if bytes.Equal(relevant, s.lower) {
					if debugLogIndexScans {
						log.Printf("range index scan step: SKIP_INITIAL_EQ_LOWER: ik = %x, relevant = %x", ik, relevant)
					}
					ik, iv = c.Next()
					continue
				} else {
					skippingInitial = false
				}
			}
		}

		if reverse {
			if s.lower != nil {
				// if debugLogScans {
				// 	log.Printf("range index scan step: cmp (reverse+lower): ik = %x, relevant = %x, lower = %x", ik, relevant, lower)
				// }
				cmp := bytes.Compare(relevant, s.lower)
				if cmp < 0 || (cmp == 0 && !s.lowerInc) {
					if debugLogIndexScans {
						log.Printf("range index scan step: BAIL: below lower: ik = %x, lower = %x", ik, lower)
					}
					return nil, nil, nil, nil
				}
			}
		} else {
			if s.upper != nil {
				cmp := bytes.Compare(relevant, s.upper)
				if cmp > 0 || (cmp == 0 && !s.upperInc) {
					if debugLogIndexScans {
						log.Printf("range index scan step: BAIL: above upper: ik = %x, upper = %x", ik, upper)
					}
					return nil, nil, nil, nil
				}
			}
		}

		if debugLogIndexScans {
			log.Printf("range index scan step: MTCH: ik = %x, iv = %q", ik, iv)
		}
		dk, itup := decodeIndexTableKey(ik, ikTup, iv, idx)
		return ik, iv, itup, dk
	}
	if debugLogIndexScans {
		log.Printf("range index scan step: EOFd")
	}
	return nil, nil, nil, nil
}
