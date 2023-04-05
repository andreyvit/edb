package edb

import (
	"bytes"
	"fmt"
	"log"
	"reflect"

	"go.etcd.io/bbolt"
)

const (
	debugLogScans = false
)

type Cursor[Row any] struct {
	RawCursor
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

func IndexScan[Row any](txh Txish, idx *Index, opt ScanOptions) Cursor[Row] {
	tx := txh.DBTx()
	tbl := tableOf[Row](tx)
	if tbl != idx.table {
		panic(fmt.Errorf("row referes to table %v, but index is on table %v", tbl.Name(), idx.table.Name()))
	}
	return Cursor[Row]{tx.IndexScan(idx, opt)}
}

func FullIndexScan[Row any](txh Txish, idx *Index) Cursor[Row] {
	return IndexScan[Row](txh, idx, FullScan())
}

func ExactIndexScan[Row any](txh Txish, idx *Index, indexValue any) Cursor[Row] {
	return IndexScan[Row](txh, idx, ExactScan(indexValue))
}

func PrefixIndexScan[Row any](txh Txish, idx *Index, els int, indexValue any) Cursor[Row] {
	return IndexScan[Row](txh, idx, ExactScan(indexValue).Prefix(els))
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

type ScanMethod int

const (
	ScanMethodFull = ScanMethod(iota)
	ScanMethodExact
)

type ScanOptions struct {
	Reverse bool
	Method  ScanMethod
	Lower   reflect.Value
	Els     int
}

func (so ScanOptions) Reversed() ScanOptions {
	so.Reverse = true
	return so
}

func (so ScanOptions) Prefix(els int) ScanOptions {
	so.Els = els
	return so
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
	tx      *Tx
	table   *Table
	dcur    *bbolt.Cursor
	prefix  []byte
	init    bool
	reverse bool
	k, v    []byte
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
	} else {
		c.init = true
		if c.reverse {
			k, v = c.dcur.Last()
		} else {
			k, v = c.dcur.First()
		}
	}
	if k == nil {
		return false
	}
	if p := c.prefix; p != nil {
		if len(k) < len(p) || !bytes.Equal(p, k[:len(p)]) {
			return false
		}
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
	return &RawTableCursor{
		tx:      tx,
		table:   tbl,
		dcur:    buck.Cursor(),
		reverse: opt.Reverse,
	}
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
}

func (c *RawIndexCursor) Table() *Table {
	return c.table
}

func (c *RawIndexCursor) Tx() *Tx {
	return c.tx
}

func (c *RawIndexCursor) Next() bool {
	c.ik, c.iv, c.dk = c.strat.Next(c.icur, !c.resetDone, c.reverse, c.index)
	c.resetDone = true
	return (c.ik != nil)
}

func (c *RawIndexCursor) RawKey() []byte {
	return c.dk
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

		keyPrefix, keyEls, isFull := encodeIndexBoundaryKey(opt.Lower, idx, opt.Els)
		tx.addIndexKeyBuf(keyPrefix)

		if idx.isUnique && isFull {
			strat = &exactIndexScanStrategy{keyPrefix, keyEls}
		} else {
			strat = &prefixIndexScanStrategy{keyPrefix, keyEls}
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

func encodeIndexBoundaryKey(keyVal reflect.Value, idx *Index, cutoffEls int) ([]byte, int, bool) {
	indexKeyBuf := keyBytesPool.Get().([]byte)
	fe := flatEncoder{buf: indexKeyBuf}
	idx.keyEnc.encodeInto(&fe, keyVal)

	keyEls, isFull := fe.count(), true
	if cutoffEls != 0 && cutoffEls < keyEls {
		keyEls, isFull = cutoffEls, false
	}

	if idx.isUnique && isFull {
		return fe.finalize(), keyEls, true
	} else if isFull {
		return fe.buf, keyEls, true
	} else {
		n := fe.prefixLen(keyEls)
		return fe.buf[:n], keyEls, false
	}
}

type indexScanStrategy interface {
	Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, []byte)
}

type fullIndexScanStrategy struct{}

func (_ fullIndexScanStrategy) Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, []byte) {
	var ik, iv []byte
	if reset {
		ik, iv = boltFirstLast(c, reverse)
	} else {
		ik, iv = boltAdvance(c, reverse)
	}
	if ik == nil {
		return nil, nil, nil
	}
	dk := decodeIndexTableKey(ik, nil, iv, idx)
	return ik, iv, dk
}

type exactIndexScanStrategy struct {
	prefix []byte
	els    int
}

func (s *exactIndexScanStrategy) Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, []byte) {
	var ik, iv []byte
	if reset {
		ik, iv = boltSeek(c, s.prefix, reverse)
	} else {
		ik, iv = boltAdvance(c, reverse)
	}
	if ik != nil && bytes.HasPrefix(ik, s.prefix) {
		dk := decodeIndexTableKey(ik, nil, iv, idx)
		return ik, iv, dk
	} else {
		return nil, nil, nil
	}
}

type prefixIndexScanStrategy struct {
	prefix []byte
	els    int
}

func (s *prefixIndexScanStrategy) Next(c *bbolt.Cursor, reset, reverse bool, idx *Index) ([]byte, []byte, []byte) {
	prefix := s.prefix
	var ik, iv []byte
	if reset {
		if debugLogScans {
			log.Printf("prefix index scan step: SEEK: prefix = %x, reverse = %v", prefix, reverse)
		}
		ik, iv = boltSeek(c, prefix, reverse)
	} else {
		if debugLogScans {
			log.Printf("prefix index scan step: ADNC: reverse = %v", reverse)
		}
		ik, iv = boltAdvance(c, reverse)
	}
	for ik != nil {
		if !bytes.HasPrefix(ik, prefix) {
			if debugLogScans {
				log.Printf("prefix index scan step: BAIL: ik = %x, prefix = %x", ik, prefix)
			}
			break
		}
		ikTup := decodeIndexKey(ik, idx)
		if len(ikTup) < s.els {
			panic(fmt.Errorf("%s: invalid index key %x: got %d els, wanted at least %d", idx.FullName(), ik, len(ikTup), s.els+1))
		}
		if ikTup.prefixLen(s.els) == len(prefix) {
			if debugLogScans {
				log.Printf("prefix index scan step: MTCH: ik = %x, iv = %q", ik, iv)
			}
			dk := decodeIndexTableKey(ik, ikTup, iv, idx)
			return ik, iv, dk
		}
		if debugLogScans {
			log.Printf("prefix index scan step: SKIP: ik = %x, iv = %q", ik, iv)
		}
		ik, iv = boltAdvance(c, reverse)
	}
	return nil, nil, nil
}
