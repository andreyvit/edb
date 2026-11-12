package edb

import (
	"bytes"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"sort"
	"strconv"
	"strings"
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

// RowInto replaces *dst with the current row. A scan can reuse the same
// struct on each iteration. Use Row when handing independently owned rows
// to another consumer. Maps and slices from earlier rows are never reused.
func (c Cursor[Row]) RowInto(dst *Row) (ValueMeta, error) {
	t := c.Table()
	row := reflect.ValueOf(dst)
	if dst == nil || row.Type().Elem() != t.rowType {
		return ValueMeta{}, fmt.Errorf("expected *%s", t.rowType)
	}
	var v value
	if err := v.decode(c.RawRow(), false); err != nil {
		return ValueMeta{}, err
	}
	row.Elem().SetZero()
	return decodeRowValueInto(row, &v, t, c.RawKey(), c.Tx())
}

func (c Cursor[Row]) Meta() ValueMeta {
	return c.RawCursor.Meta()
}

func (c Cursor[Row]) Rows() func(yield func(*Row) bool) {
	return func(yield func(*Row) bool) {
		for c.Next() {
			if !yield(c.Row()) {
				break
			}
		}
	}
}

func TableScan[Row any](txh Txish, opt ScanOptions) Cursor[Row] {
	tx := txh.DBTx()
	tbl := tableOf[Row](tx)
	return Cursor[Row]{tx.TableScan(tbl, opt)}
}

func (tx *Tx) TableScan(tblArg AnyTable, opt ScanOptions) *RawTableCursor {
	tbl := tblArg.TableDef()

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

func IndexScan[Row any](txh Txish, idxArg AnyIndex, opt ScanOptions) Cursor[Row] {
	idx := idxArg.IndexDef()

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

func FullIndexScan[Row any](txh Txish, idxArg AnyIndex) Cursor[Row] {
	idx := idxArg.IndexDef()

	return IndexScan[Row](txh, idx, FullScan())
}

func ExactIndexScan[Row any](txh Txish, idxArg AnyIndex, indexValue any) Cursor[Row] {
	idx := idxArg.IndexDef()

	return IndexScan[Row](txh, idx, ExactScan(indexValue))
}
func ReverseExactIndexScan[Row any](txh Txish, idxArg AnyIndex, indexValue any) Cursor[Row] {
	idx := idxArg.IndexDef()

	return IndexScan[Row](txh, idx, ExactScan(indexValue).Reversed())
}

func RangeIndexScan[Row any](txh Txish, idxArg AnyIndex, lowerValue, upperValue any, lowerInc, upperInc bool) Cursor[Row] {
	idx := idxArg.IndexDef()

	return IndexScan[Row](txh, idx, RangeScan(lowerValue, upperValue, lowerInc, upperInc))
}
func ReverseRangeIndexScan[Row any](txh Txish, idxArg AnyIndex, lowerValue, upperValue any, lowerInc, upperInc bool) Cursor[Row] {
	idx := idxArg.IndexDef()

	return IndexScan[Row](txh, idx, RangeScan(lowerValue, upperValue, lowerInc, upperInc).Reversed())
}

func PrefixIndexScan[Row any](txh Txish, idxArg AnyIndex, els int, indexValue any) Cursor[Row] {
	idx := idxArg.IndexDef()

	return IndexScan[Row](txh, idx, ExactScan(indexValue).Prefix(els))
}
func ReversePrefixIndexScan[Row any](txh Txish, idxArg AnyIndex, els int, indexValue any) Cursor[Row] {
	idx := idxArg.IndexDef()

	return IndexScan[Row](txh, idx, ExactScan(indexValue).Prefix(els).Reversed())
}

func (tx *Tx) IndexScan(idxArg AnyIndex, opt ScanOptions) *RawIndexCursor {
	idx := idxArg.IndexDef()

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

func FirstKey[Key, Row any](c Cursor[Row]) Key {
	if c.Next() {
		return c.Key().(Key)
	}
	var zero Key
	return zero
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
			result = append(result, row)
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
	resume          ScanBookmark
	Reverse         bool
	Method          ScanMethod
	PrefixValue     reflect.Value
	PrefixEls       int
	Lower           reflect.Value
	Upper           reflect.Value
	LowerInc        bool
	UpperInc        bool
	LowerPrimaryKey reflect.Value
	UpperPrimaryKey reflect.Value
}

func (so ScanOptions) Reversed() ScanOptions {
	so.Reverse = true
	return so
}

func (so ScanOptions) Prefix(els int) ScanOptions {
	if !so.Lower.IsValid() {
		panic("Prefix requires a lower value")
	}
	so.PrefixValue = so.Lower
	so.PrefixEls = els
	if so.Method == ScanMethodExact {
		so.Method = ScanMethodFull
		so.Lower = reflect.Value{}
		so.Upper = reflect.Value{}
		so.LowerInc = false
		so.UpperInc = false
	}
	return so
}

func (so ScanOptions) PrimaryKeyRange(lower, upper any, lowerInc, upperInc bool) ScanOptions {
	var lowerVal, upperVal reflect.Value
	if lower != nil {
		lowerVal = reflect.ValueOf(lower)
	}
	if upper != nil {
		upperVal = reflect.ValueOf(upper)
	}
	return so.PrimaryKeyRangeVal(lowerVal, upperVal, lowerInc, upperInc)
}

func (so ScanOptions) PrimaryKeyRangeVal(lower, upper reflect.Value, lowerInc, upperInc bool) ScanOptions {
	so.LowerPrimaryKey = lower
	so.UpperPrimaryKey = upper
	so.LowerInc = lowerInc
	so.UpperInc = upperInc
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
	if so.PrefixValue.IsValid() {
		buf.WriteString(":prefix:")
		buf.WriteString(loggableVal(so.PrefixValue))
		if so.PrefixEls != 0 {
			buf.WriteByte(':')
			buf.WriteString(strconv.Itoa(so.PrefixEls))
		}
	}
	if so.LowerPrimaryKey.IsValid() || so.UpperPrimaryKey.IsValid() {
		buf.WriteString(":primary")
		if so.LowerInc {
			buf.WriteByte('[')
		} else {
			buf.WriteByte('(')
		}
		buf.WriteString(loggableVal(so.LowerPrimaryKey))
		buf.WriteByte(':')
		buf.WriteString(loggableVal(so.UpperPrimaryKey))
		if so.UpperInc {
			buf.WriteByte(']')
		} else {
			buf.WriteByte(')')
		}
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

func ExactIDRangeScan(exact, lower, upper any, lowerInc, upperInc bool) ScanOptions {
	var lowerVal, upperVal reflect.Value
	if lower != nil {
		lowerVal = reflect.ValueOf(lower)
	}
	if upper != nil {
		upperVal = reflect.ValueOf(upper)
	}
	return ExactIDRangeScanVal(reflect.ValueOf(exact), lowerVal, upperVal, lowerInc, upperInc)
}
func ExactIDRangeScanVal(exact, lower, upper reflect.Value, lowerInc, upperInc bool) ScanOptions {
	return ExactScanVal(exact).PrimaryKeyRangeVal(lower, upper, lowerInc, upperInc)
}

type RawCursor interface {
	Bookmark() ScanBookmark
	Table() *TableDef
	Tx() *Tx
	Next() bool
	Key() any
	// UintKey returns the primary key as uint64. It accepts unsigned integer
	// types, including named types, and panics for other key types. The built-in
	// unsigned encoding is read without reflection or allocation.
	UintKey() uint64
	RawKey() []byte
	RowVal() (reflect.Value, ValueMeta)
	TryRowVal() (reflect.Value, ValueMeta, error)
	Meta() ValueMeta
	Row() (any, ValueMeta)
	TryRow() (any, ValueMeta, error)
	RawRow() []byte
	ValueMemento() []byte
}

type RawTableCursor struct {
	after    string
	done     bool
	tx       *Tx
	table    *TableDef
	dcur     storageCursor
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

func (c *RawTableCursor) Table() *TableDef {
	return c.table
}

func (c *RawTableCursor) Next() bool {
	if c.done {
		return false
	}
	c.done = true
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
		dcur := c.dcur
		if c.after != "" {
			dcur = &resumeStorageCursor{dcur, c.after}
		}
		if c.reverse {
			upper := c.upper
			if upper == nil && len(c.prefix) > 0 {
				upper = c.prefix
			}
			if upper != nil {
				k, v = dcur.SeekLast(upper)
				if k != nil && !c.upperInc && bytes.Equal(k, upper) {
					k, v = dcur.Prev()
				}
				if debugLogTableScans {
					log.Printf("%s::TableScan: SEEK to upper = %x: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, upper, c.prefix, c.reverse, k, v)
				}
			} else {
				k, v = dcur.Last()
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
				k, v = dcur.Seek(lower)
				if k != nil && !c.lowerInc && bytes.Equal(k, lower) {
					k, v = dcur.Next()
				}
				if debugLogTableScans {
					log.Printf("%s::TableScan: SEEK to lower = %x: prefix = %x, reverse = %v => k = %x, v = %x", c.table.name, lower, c.prefix, c.reverse, k, v)
				}
			} else {
				k, v = dcur.First()
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
	c.done = false
	return true
}

func (c *RawTableCursor) RawKey() []byte {
	return c.k
}

func (c *RawTableCursor) Key() any {
	return c.table.DecodeKeyVal(c.k).Interface()
}

// UintKey returns the current unsigned primary key without boxing it.
func (c *RawTableCursor) UintKey() uint64 {
	return c.table.decodeUintKey(c.k)
}

func (c *RawTableCursor) RowVal() (reflect.Value, ValueMeta) {
	return must2(c.TryRowVal())
}

func (c *RawTableCursor) TryRowVal() (reflect.Value, ValueMeta, error) {
	return decodeTableRow(c.table, c.k, c.v, c.tx, false)
}

func (c *RawTableCursor) RawRow() []byte {
	return c.v
}

func (c *RawTableCursor) ValueMemento() []byte {
	brief, err := briefRawValue(c.RawRow())
	if err != nil {
		err := tableErrf(c.table, nil, c.RawKey(), err, "")
		log.Printf("** ERROR: %v", err)
		panic(err)
	}
	return brief
}

func (c *RawTableCursor) Meta() ValueMeta {
	var vle value
	decodeTableValue(&vle, c.table, c.k, c.v, false)
	return vle.ValueMeta()
}

func (c *RawTableCursor) Row() (any, ValueMeta) {
	rowVal, rowMeta := c.RowVal()
	return valToAny(rowVal), rowMeta
}

func (c *RawTableCursor) TryRow() (any, ValueMeta, error) {
	rowVal, rowMeta, err := c.TryRowVal()
	if err != nil {
		return nil, rowMeta, err
	}
	return valToAny(rowVal), rowMeta, nil
}

func (tx *Tx) newTableCursor(tbl *TableDef, opt ScanOptions) *RawTableCursor {
	opt.resume.check(tbl, nil, opt.Reverse)
	buck := nonNil(tx.stx.Bucket(tbl.name, dataBucketName))
	c := &RawTableCursor{
		tx:      tx,
		table:   tbl,
		dcur:    buck.Cursor(),
		after:   opt.resume.position,
		reverse: opt.Reverse,
	}
	if opt.PrefixValue.IsValid() {
		if at, et := opt.PrefixValue.Type(), tbl.KeyType(); at != et {
			panic(fmt.Errorf("%s: attempted to scan table using prefix of incorrect type %v, expected %v", tbl.Name(), at, et))
		}
		c.prefix, _, _ = tx.encodeTableBoundaryKey(opt.PrefixValue, tbl, opt.PrefixEls)
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

		keyPrefix, _, isFull := tx.encodeTableBoundaryKey(opt.Lower, tbl, 0)

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
	after        string
	done         bool
	table        *TableDef
	index        *IndexDef
	strat        indexScanStrategy
	tx           *Tx
	icur         storageCursor
	dbuck        storageBucket
	dcur         storageCursor
	prefix       []byte
	resetDone    bool
	reverse      bool
	ik, iv, dk   []byte
	itup         tuple
	tupleScratch tuple
}

func (c *RawIndexCursor) Table() *TableDef {
	return c.table
}

func (c *RawIndexCursor) Tx() *Tx {
	return c.tx
}

func (c *RawIndexCursor) Next() bool {
	if c.done {
		return false
	}
	icur := c.icur
	if !c.resetDone && c.after != "" {
		icur = &resumeStorageCursor{icur, c.after}
	}
	ik, iv, itup, dk := c.strat.Next(icur, !c.resetDone, c.reverse, c.index, c.tupleScratch)
	c.resetDone = true
	if ik == nil {
		c.done = true
		return false
	}
	c.ik, c.iv, c.itup, c.dk = ik, iv, itup, dk
	if itup != nil {
		c.tupleScratch = itup[:0]
	}
	return true
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

// UintKey returns the current row's unsigned primary key, not the index key.
func (c *RawIndexCursor) UintKey() uint64 {
	return c.table.decodeUintKey(c.dk)
}

func (c *RawIndexCursor) RowVal() (reflect.Value, ValueMeta) {
	return must2(c.TryRowVal())
}

func (c *RawIndexCursor) TryRowVal() (reflect.Value, ValueMeta, error) {
	dv := c.RawRow()
	return decodeTableRow(c.table, c.dk, dv, c.tx, false)
}

func (c *RawIndexCursor) RawRow() []byte {
	if c.dcur == nil {
		c.dcur = c.dbuck.Cursor()
	}
	k, v := c.dcur.Seek(c.dk)
	if !bytes.Equal(k, c.dk) {
		return nil
	}
	return v
}

func (c *RawIndexCursor) ValueMemento() []byte {
	brief, err := briefRawValue(c.RawRow())
	if err != nil {
		err := tableErrf(c.table, nil, c.RawKey(), err, "")
		log.Printf("** ERROR: %v", err)
		panic(err)
	}
	return brief
}

func (c *RawIndexCursor) Meta() ValueMeta {
	dv := c.RawRow()
	var vle value
	decodeTableValue(&vle, c.table, c.dk, dv, false)
	return vle.ValueMeta()
}

func (c *RawIndexCursor) Row() (any, ValueMeta) {
	rowVal, rowMeta := c.RowVal()
	return valToAny(rowVal), rowMeta
}

func (c *RawIndexCursor) TryRow() (any, ValueMeta, error) {
	rowVal, rowMeta, err := c.TryRowVal()
	if err != nil {
		return nil, rowMeta, err
	}
	return valToAny(rowVal), rowMeta, nil
}

func (tx *Tx) newIndexCursor(idx *IndexDef, opt ScanOptions) *RawIndexCursor {
	idx.requireTable()
	opt.resume.check(idx.table, idx, opt.Reverse)
	if tx.isVerboseLoggingEnabled() {
		tx.db.logf("db: INDEX_SCAN %s/%v", idx.FullName(), opt.LogString())
	}
	ibuck := nonNil(tx.stx.Bucket(idx.table.name, idx.buck))
	dbuck := nonNil(tx.stx.Bucket(idx.table.name, dataBucketName))
	strat := tx.newIndexScanStrategy(idx, opt)
	return &RawIndexCursor{
		table:   idx.table,
		index:   idx,
		tx:      tx,
		icur:    ibuck.Cursor(),
		after:   opt.resume.position,
		dbuck:   dbuck,
		reverse: opt.Reverse,
		strat:   strat,
	}
}

func (tx *Tx) newIndexScanStrategy(idx *IndexDef, opt ScanOptions) indexScanStrategy {
	var strat indexScanStrategy
	if opt.usesRawIndexRange() {
		strat = tx.newRawRangeIndexScanStrategy(idx, opt)
	} else {
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

			keyPrefix, keyEls, isFull := tx.encodeIndexBoundaryKey(opt.Lower, idx, 0, false)

			if idx.isUnique && isFull {
				strat = &exactIndexScanStrategy{keyPrefix, keyEls}
			} else {
				var components tuple
				if keyEls > 1 {
					components = indexBoundaryTuple(idx, opt.Lower, keyEls)
				}
				strat = &prefixIndexScanStrategy{keyPrefix, keyEls, components}
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

					lower, els, _ = tx.encodeIndexBoundaryKey(opt.Lower, idx, 0, true)
				}
				if opt.Upper.IsValid() {
					if at, et := opt.Upper.Type(), idx.keyType(); at != et {
						panic(fmt.Errorf("%s: attempted to scan index using lower bound of incorrect type %v, expected %v", idx.FullName(), at, et))
					}

					var upperEls int
					upper, upperEls, _ = tx.encodeIndexBoundaryKey(opt.Upper, idx, 0, true)
					if !opt.Lower.IsValid() {
						els = upperEls
					} else if els != upperEls {
						panic(fmt.Errorf("%s: attempted to scan index using lower and upper boundaries of different prefix sizes (lower %d, upper %d)", idx.FullName(), els, upperEls))
					}
				}

				if lower != nil && upper != nil && bytes.Compare(lower, upper) > 0 {
					strat = emptyIndexScanStrategy{}
				} else {
					strat = &rangeIndexScanStrategy{
						els: els, lower: lower, upper: upper,
						upperEnvelope: indexUpperEnvelope(idx, els, lower, upper),
						lowerInc:      opt.LowerInc, upperInc: opt.UpperInc, verbose: idx.debugScans,
					}
				}
			}

		default:
			panic(fmt.Errorf("unsupported scan method %v", opt.Method))
		}
	}
	return strat
}

func (so ScanOptions) usesRawIndexRange() bool {
	return so.PrefixValue.IsValid() || so.LowerPrimaryKey.IsValid() || so.UpperPrimaryKey.IsValid()
}

func (tx *Tx) newRawRangeIndexScanStrategy(idx *IndexDef, opt ScanOptions) indexScanStrategy {
	if idx.isUnique && (opt.LowerPrimaryKey.IsValid() || opt.UpperPrimaryKey.IsValid()) {
		panic("primary-key range is not supported for deprecated unique indices")
	}
	rang, empty := tx.indexScanRawRange(idx, opt)
	if empty && opt.Method != ScanMethodRange {
		return emptyIndexScanStrategy{}
	}
	filter := &filteredIndexScanStrategy{base: &rawRangeIndexScanStrategy{rang, tx.logger}, lowerInc: opt.LowerInc, upperInc: opt.UpperInc}
	if opt.PrefixValue.IsValid() {
		filter.prefix = indexBoundaryTuple(idx, opt.PrefixValue, opt.PrefixEls)
	}
	if opt.Method == ScanMethodExact {
		filter.prefix = indexBoundaryTuple(idx, opt.Lower, 0)
	}
	if opt.Method == ScanMethodRange {
		// Raw bounds that append a primary key cannot bound an entire logical
		// index range: a shorter string index key can sort beyond that bound.
		// Use the ordinary range traversal and apply PK limits at its endpoints.
		plain := opt
		plain.PrefixValue, plain.LowerPrimaryKey, plain.UpperPrimaryKey = reflect.Value{}, reflect.Value{}, reflect.Value{}
		plain.PrefixEls = 0
		if opt.LowerPrimaryKey.IsValid() {
			plain.LowerInc = true
		}
		if opt.UpperPrimaryKey.IsValid() {
			plain.UpperInc = true
		}
		filter.base = tx.newIndexScanStrategy(idx, plain)
	}
	if opt.LowerPrimaryKey.IsValid() {
		filter.lower = indexBoundaryTuple(idx, opt.Lower, 0)
		filter.lowerPK = idx.table.EncodeKeyVal(opt.LowerPrimaryKey)
	}
	if opt.UpperPrimaryKey.IsValid() {
		upper := opt.Upper
		if opt.Method == ScanMethodExact {
			upper = opt.Lower
		}
		filter.upper = indexBoundaryTuple(idx, upper, 0)
		filter.upperPK = idx.table.EncodeKeyVal(opt.UpperPrimaryKey)
	}
	return filter
}

func indexBoundaryTuple(idx *IndexDef, key reflect.Value, els int) tuple {
	tup := must(decodeTuple(idx.keyEnc.encode(nil, key)))
	if els != 0 && els < len(tup) {
		tup = tup[:els]
	}
	return tup
}

type filteredIndexScanStrategy struct {
	base                 indexScanStrategy
	prefix, lower, upper tuple
	lowerPK, upperPK     []byte
	lowerInc, upperInc   bool
}

func indexTupleHasPrefix(key, prefix tuple) bool {
	return len(key) >= len(prefix) && key[:len(prefix)].Equal(prefix)
}

func (s *filteredIndexScanStrategy) Next(c storageCursor, reset, reverse bool, idx *IndexDef, scratch tuple) ([]byte, []byte, tuple, []byte) {

	for {
		ik, iv, tup, pk := s.base.Next(c, reset, reverse, idx, scratch)
		if ik == nil {
			return nil, nil, nil, nil
		}
		reset, scratch = false, tup[:0]
		if !indexTupleHasPrefix(tup, s.prefix) {
			continue
		}
		if s.lowerPK != nil && indexTupleHasPrefix(tup, s.lower) {
			cmp := bytes.Compare(pk, s.lowerPK)
			if cmp < 0 || cmp == 0 && !s.lowerInc {
				continue
			}
		}
		if s.upperPK != nil && indexTupleHasPrefix(tup, s.upper) {
			cmp := bytes.Compare(pk, s.upperPK)
			if cmp > 0 || cmp == 0 && !s.upperInc {
				continue
			}
		}
		return ik, iv, tup, pk
	}
}

func (tx *Tx) indexScanRawRange(idx *IndexDef, opt ScanOptions) (RawRange, bool) {
	var rang RawRange
	if opt.PrefixValue.IsValid() {
		if at, et := opt.PrefixValue.Type(), idx.keyType(); at != et {
			panic(fmt.Errorf("%s: attempted to scan index using prefix of incorrect type %v, expected %v", idx.FullName(), at, et))
		}
		rang.Prefix, _, _ = tx.encodeIndexBoundaryKey(opt.PrefixValue, idx, opt.PrefixEls, true)
	}
	if opt.Method == ScanMethodExact {
		if !opt.Lower.IsValid() {
			panic(fmt.Errorf("Lower must be specified for ScanMethodExact"))
		}
		if at, et := opt.Lower.Type(), idx.keyType(); at != et {
			panic(fmt.Errorf("%s: attempted to scan index using lower bound of incorrect type %v, expected %v", idx.FullName(), at, et))
		}
		prefix, _, _ := tx.encodeIndexBoundaryKey(opt.Lower, idx, 0, true)
		rang.Prefix = prefix
	}
	if opt.Lower.IsValid() && (opt.Method == ScanMethodRange || opt.LowerPrimaryKey.IsValid()) {
		lower, inclusive, empty := tx.indexScanRawRangeBound(idx, opt.Lower, opt.LowerPrimaryKey, opt.LowerInc, false)
		if empty {
			return RawRange{}, true
		}
		rang.Lower = lower
		rang.LowerInc = inclusive
	}
	upperIndex := opt.Upper
	if !upperIndex.IsValid() && opt.Method == ScanMethodExact {
		upperIndex = opt.Lower
	}
	if upperIndex.IsValid() && (opt.Method == ScanMethodRange || opt.UpperPrimaryKey.IsValid()) {
		upper, inclusive, empty := tx.indexScanRawRangeBound(idx, upperIndex, opt.UpperPrimaryKey, opt.UpperInc, true)
		if empty {
			return RawRange{}, true
		}
		rang.Upper = upper
		rang.UpperInc = inclusive
	}
	if opt.LowerPrimaryKey.IsValid() && !opt.Lower.IsValid() {
		panic("Lower must be specified when LowerPrimaryKey is specified")
	}
	if opt.UpperPrimaryKey.IsValid() && !upperIndex.IsValid() {
		panic("Upper must be specified when UpperPrimaryKey is specified")
	}
	return rang, false
}

func (tx *Tx) indexScanRawRangeBound(idx *IndexDef, indexKey, primaryKey reflect.Value, inclusive, upper bool) ([]byte, bool, bool) {
	if at, et := indexKey.Type(), idx.keyType(); at != et {
		panic(fmt.Errorf("%s: attempted to scan index using bound of incorrect type %v, expected %v", idx.FullName(), at, et))
	}
	if primaryKey.IsValid() {
		tbl := idx.Table()
		if at, et := primaryKey.Type(), tbl.KeyType(); at != et {
			panic(fmt.Errorf("%s: attempted to scan table using bound of incorrect type %v, expected %v", tbl.Name(), at, et))
		}
		raw := tx.encodeIndexFullKey(indexKey, primaryKey, idx)
		return raw, inclusive, false
	}
	raw, _, _ := tx.encodeIndexBoundaryKey(indexKey, idx, 0, true)
	if upper && inclusive {
		next, ok := rawSuccessor(raw)
		if !ok {
			return nil, false, false
		}
		return next, false, false
	}
	if !upper && !inclusive {
		next, ok := rawSuccessor(raw)
		return next, true, !ok
	}
	return raw, inclusive, false
}

func rawSuccessor(raw []byte) ([]byte, bool) {
	next := append([]byte(nil), raw...)
	return next, inc(next)
}

func (tx *Tx) encodeTableBoundaryKey(keyVal reflect.Value, tbl *TableDef, cutoffEls int) ([]byte, int, bool) {
	keyBuf := acquireKeyBuffer()
	fe := flatEncoder{buf: keyBuf.data}
	tbl.keyEnc.encodeInto(&fe, keyVal)

	keyEls, isFull := fe.count(), true
	if cutoffEls != 0 && cutoffEls < keyEls {
		keyEls, isFull = cutoffEls, false
	}

	if isFull {
		keyBuf.data = fe.finalize()
	} else {
		n := fe.prefixLen(keyEls)
		keyBuf.data = fe.buf[:n]
	}
	tx.addIndexKeyBuf(keyBuf)
	return keyBuf.data, keyEls, isFull
}

func (tx *Tx) encodeIndexFullKey(indexKeyVal, tableKeyVal reflect.Value, idx *IndexDef) []byte {
	tableKeyBuf := acquireKeyBuffer()
	tbl := idx.Table()
	tableKeyRaw := tbl.encodeKeyVal(tableKeyBuf.data, tableKeyVal, false)
	tableKeyBuf.data = tableKeyRaw[:0]
	defer releaseKeyBuffer(tableKeyBuf)

	indexKeyBuf := acquireKeyBuffer()
	keyEnc := flatEncoder{buf: indexKeyBuf.data}
	idx.keyEnc.encodeInto(&keyEnc, indexKeyVal)
	keyEnc.begin()
	keyEnc.buf = appendRaw(keyEnc.buf, tableKeyRaw)

	indexKeyBuf.data = keyEnc.finalize()
	tx.addIndexKeyBuf(indexKeyBuf)
	return indexKeyBuf.data
}

func (tx *Tx) encodeIndexBoundaryKey(keyVal reflect.Value, idx *IndexDef, cutoffEls int, neverFinalize bool) ([]byte, int, bool) {
	indexKeyBuf := acquireKeyBuffer()
	fe := flatEncoder{buf: indexKeyBuf.data}
	idx.keyEnc.encodeInto(&fe, keyVal)

	keyEls, isFull := fe.count(), true
	if cutoffEls != 0 && cutoffEls < keyEls {
		keyEls, isFull = cutoffEls, false
	}

	indexKeyBuf.data = fe.buf
	if idx.isUnique && isFull {
		if neverFinalize {
			isFull = false
		} else {
			indexKeyBuf.data = fe.finalize()
		}
	} else if !isFull {
		n := fe.prefixLen(keyEls)
		indexKeyBuf.data = fe.buf[:n]
	}
	tx.addIndexKeyBuf(indexKeyBuf)
	return indexKeyBuf.data, keyEls, isFull
}

type indexScanStrategy interface {
	Next(c storageCursor, reset, reverse bool, idx *IndexDef, scratch tuple) ([]byte, []byte, tuple, []byte)
}

type emptyIndexScanStrategy struct{}

func (_ emptyIndexScanStrategy) Next(storageCursor, bool, bool, *IndexDef, tuple) ([]byte, []byte, tuple, []byte) {
	return nil, nil, nil, nil
}

type fullIndexScanStrategy struct{}

func (_ fullIndexScanStrategy) Next(c storageCursor, reset, reverse bool, idx *IndexDef, scratch tuple) ([]byte, []byte, tuple, []byte) {

	var ik, iv []byte
	if reset {
		if reverse {
			ik, iv = c.Last()
		} else {
			ik, iv = c.First()
		}
	} else {
		if reverse {
			ik, iv = c.Prev()
		} else {
			ik, iv = c.Next()
		}
	}
	if ik == nil {
		return nil, nil, nil, nil
	}
	iktup := decodeIndexKeyInto(scratch, ik, idx)
	dk, itup := decodeIndexTableKey(ik, iktup, iv, idx)
	return ik, iv, itup, dk
}

type exactIndexScanStrategy struct {
	prefix []byte
	els    int
}

func (s *exactIndexScanStrategy) Next(c storageCursor, reset, reverse bool, idx *IndexDef, scratch tuple) ([]byte, []byte, tuple, []byte) {

	var ik, iv []byte
	if reset {
		if reverse {
			ik, iv = c.SeekLast(s.prefix)
		} else {
			ik, iv = c.Seek(s.prefix)
		}
	} else {
		if reverse {
			ik, iv = c.Prev()
		} else {
			ik, iv = c.Next()
		}
	}
	for reverse && ik != nil && bytes.Compare(ik, s.prefix) > 0 {
		ik, iv = c.Prev()
	}
	if ik != nil && bytes.Equal(ik, s.prefix) {
		dk, itup := decodeIndexTableKey(ik, decodeIndexKeyInto(scratch, ik, idx), iv, idx)
		return ik, iv, itup, dk
	} else {
		return nil, nil, nil, nil
	}
}

type prefixIndexScanStrategy struct {
	prefix     []byte
	els        int
	components tuple
}

func (s *prefixIndexScanStrategy) Next(c storageCursor, reset, reverse bool, idx *IndexDef, scratch tuple) ([]byte, []byte, tuple, []byte) {

	prefix := s.prefix
	var ik, iv []byte
	if reset {
		if reverse {
			ik, iv = c.SeekLast(prefix)
		} else {
			ik, iv = c.Seek(prefix)
		}
		if debugLogIndexScans {
			log.Printf("prefix index scan step: SEEK: prefix = %x, reverse = %v => ik = %x, iv = %x", prefix, reverse, ik, iv)
		}
	} else {
		if debugLogIndexScans {
			log.Printf("prefix index scan step: ADVC: reverse = %v", reverse)
		}
		if reverse {
			ik, iv = c.Prev()
		} else {
			ik, iv = c.Next()
		}
	}
	for ik != nil {
		if !bytes.HasPrefix(ik, prefix) {
			if debugLogIndexScans {
				log.Printf("prefix index scan step: BAIL: ik = %x, prefix = %x", ik, prefix)
			}
			return nil, nil, nil, nil
		}
		ikTup := decodeIndexKeyInto(scratch, ik, idx)
		scratch = ikTup[:0]
		if len(ikTup) < s.els {
			panic(fmt.Errorf("%s: invalid index key %x: got %d els, wanted at least %d", idx.FullName(), ik, len(ikTup), s.els+1))
		}
		matches := ikTup.prefixLen(s.els) == len(prefix)
		if matches && s.components != nil {
			matches = indexTupleHasPrefix(ikTup, s.components)
		}
		if matches {
			if debugLogIndexScans {
				log.Printf("prefix index scan step: MTCH: ik = %x, iv = %q", ik, iv)
			}
			dk, itup := decodeIndexTableKey(ik, ikTup, iv, idx)
			return ik, iv, itup, dk
		}
		if debugLogIndexScans {
			log.Printf("prefix index scan step: SKIP: ik = %x, iv = %q", ik, iv)
		}
		if reverse {
			ik, iv = c.Prev()
		} else {
			ik, iv = c.Next()
		}
	}
	if debugLogIndexScans {
		log.Printf("prefix index scan step: EOFd: prefix = %x", prefix)
	}
	return nil, nil, nil, nil
}

type rangeIndexScanStrategy struct {
	els           int
	lower         []byte
	upper         []byte
	upperEnvelope []byte
	lowerInc      bool
	upperInc      bool
	verbose       bool
}

// Tuple payloads have no separators. A matching index value that is a proper
// prefix of upper can have its primary-key suffix sort beyond upper ("a"+"z"
// sorts after "aa"). Use the shortest possible such prefix as the raw ceiling,
// then check logical bounds on every entry. Fixed-width keys keep their exact
// ceiling. Variable keys may require a wider scan, but no wire change/reindex.
func indexUpperEnvelope(idx *IndexDef, els int, lower, upper []byte) []byte {
	if upper == nil {
		return nil
	}
	minimum := 0
	for _, c := range idx.keyEnc.components[:els] {
		minimum += c.minEncodedLen
	}
	n := minimum + sort.Search(len(upper)-minimum+1, func(i int) bool {
		return bytes.Compare(upper[:minimum+i], lower) >= 0
	})
	return upper[:n]
}

func (s *rangeIndexScanStrategy) Next(c storageCursor, reset, reverse bool, idx *IndexDef, scratch tuple) ([]byte, []byte, tuple, []byte) {

	var ik, iv []byte
	if reset {
		if reverse {
			if len(s.upperEnvelope) == 0 {
				ik, iv = c.Last()
			} else {
				ik, iv = c.SeekLast(s.upperEnvelope)
			}
		} else {
			if s.lower == nil {
				ik, iv = c.First()
			} else {
				ik, iv = c.Seek(s.lower)
			}
		}
	} else if reverse {
		ik, iv = c.Prev()
	} else {
		ik, iv = c.Next()
	}
	for ik != nil {
		// Tuple components have lengths in a footer, not separators. String index
		// keys that are prefixes of one another can interleave in storage order.
		// Stop only outside the raw envelope; check both logical bounds on every
		// candidate, including when resuming in the middle of that envelope.
		if reverse && s.lower != nil && bytes.Compare(ik, s.lower) < 0 {
			break
		}
		if !reverse && len(s.upperEnvelope) != 0 && bytes.Compare(ik, s.upperEnvelope) > 0 && !bytes.HasPrefix(ik, s.upperEnvelope) {
			break
		}
		ikTup := decodeIndexKeyInto(scratch, ik, idx)
		scratch = ikTup[:0]
		if len(ikTup) < s.els {
			panic(fmt.Errorf("%s: invalid index key %x: got %d els, wanted at least %d", idx.FullName(), ik, len(ikTup), s.els+1))
		}
		relevant := ik[:ikTup.prefixLen(s.els)]
		matches := true
		if s.lower != nil {
			cmp := bytes.Compare(relevant, s.lower)
			matches = cmp > 0 || (cmp == 0 && s.lowerInc)
		}
		if matches && s.upper != nil {
			cmp := bytes.Compare(relevant, s.upper)
			matches = cmp < 0 || (cmp == 0 && s.upperInc)
		}
		if matches {
			dk, itup := decodeIndexTableKey(ik, ikTup, iv, idx)
			return ik, iv, itup, dk
		}
		if reverse {
			ik, iv = c.Prev()
		} else {
			ik, iv = c.Next()
		}
	}
	return nil, nil, nil, nil
}

type rawRangeIndexScanStrategy struct {
	rang   RawRange
	logger *slog.Logger
}

func (s *rawRangeIndexScanStrategy) Next(c storageCursor, reset, reverse bool, idx *IndexDef, scratch tuple) ([]byte, []byte, tuple, []byte) {

	var ik, iv []byte
	if reset {
		s.rang.Reverse = reverse
		ik, iv = s.rang.start(c, s.logger)
	} else {
		ik, iv = s.rang.next(c, s.logger)
	}
	if ik == nil {
		return nil, nil, nil, nil
	}
	ikTup := decodeIndexKeyInto(scratch, ik, idx)
	dk, itup := decodeIndexTableKey(ik, ikTup, iv, idx)
	return ik, iv, itup, dk
}
