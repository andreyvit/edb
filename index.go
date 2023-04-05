package edb

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
)

type IndexRow struct {
	IndexOrd uint64
	Index    *Index
	KeyRaw   []byte
	KeyBuf   []byte
	ValueRaw []byte
	ValueBuf []byte
}

type IndexBuilder struct {
	ts      *tableState
	rows    indexRows
	rowsBuf indexRows
	key     []byte
}

func makeIndexBuilder(ts *tableState, keyRaw []byte) IndexBuilder {
	indexRowsBuf := indexRowsPool.Get().(indexRows)
	return IndexBuilder{
		ts:      ts,
		rows:    indexRowsBuf,
		rowsBuf: indexRowsBuf,
		key:     keyRaw,
	}
}

func (b *IndexBuilder) Add(idx *Index, value any) *IndexRow {
	valueVal := reflect.ValueOf(value)
	if at, et := valueVal.Type(), idx.keyType(); at != et {
		panic(fmt.Errorf("%s: attempted to add index entry with incorrect type %v, expected %v", idx.FullName(), at, et))
	}

	keyBuf := keyBytesPool.Get().([]byte)
	keyEnc := flatEncoder{buf: keyBuf}
	idx.keyEnc.encodeInto(&keyEnc, valueVal)

	var valueRaw []byte
	var valueBuf []byte
	if idx.isUnique {
		valueBuf = indexValueBytesPool.Get().([]byte)
		valueEnc := flatEncoder{buf: valueBuf}
		valueEnc.begin()
		valueEnc.append(b.key)
		valueRaw = valueEnc.finalize()
	} else {
		valueRaw = emptyIndexValue

		keyEnc.begin()
		keyEnc.buf = appendRaw(keyEnc.buf, b.key)
	}

	indexOrd := b.ts.indexOrdinal(idx)

	b.rows = append(b.rows, IndexRow{indexOrd, idx, keyEnc.finalize(), keyBuf, valueRaw, valueBuf})
	return &b.rows[len(b.rows)-1]
}

func (b *IndexBuilder) release(tx *Tx) {
	for _, row := range b.rows {
		keyBytesPool.Put(row.KeyBuf[:0])
		if row.ValueBuf != nil {
			tx.addIndexValueBuf(row.ValueBuf)
		}
	}
	indexRowsPool.Put(b.rowsBuf[:0])
}

func (b *IndexBuilder) finalize() {
	sort.Sort(b.rows)
}

type indexRows []IndexRow

func (a indexRows) Len() int      { return len(a) }
func (a indexRows) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a indexRows) Less(i, j int) bool {
	lp, rp := a[i].Index.pos, a[j].Index.pos
	if lp != rp {
		return lp < rp
	}
	return bytes.Compare(a[i].KeyRaw, a[j].KeyRaw) < 0
}
