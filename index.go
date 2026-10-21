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
	rowsBuf *indexRowsBuffer
	key     []byte
}

func makeIndexBuilder(ts *tableState, keyRaw []byte) IndexBuilder {
	indexRowsBuf := acquireIndexRowsBuffer()
	return IndexBuilder{
		ts:      ts,
		rows:    indexRowsBuf.data,
		rowsBuf: indexRowsBuf,
		key:     keyRaw,
	}
}

func (b *IndexBuilder) Add(idx *Index, value any) *IndexRow {
	valueVal := reflect.ValueOf(value)
	if idx.table != b.ts.table {
		panic(fmt.Errorf("%s: attempted to add entry to another table's index %s", b.ts.table.Name(), idx.FullName()))
	}
	if at, et := valueVal.Type(), idx.keyType(); at != et {
		panic(fmt.Errorf("%s: attempted to add index entry with incorrect type %v, expected %v", idx.FullName(), at, et))
	}

	keyBuf := acquireKeyBuffer()
	b.rowsBuf.buffers = append(b.rowsBuf.buffers, indexRowBuffers{key: keyBuf})
	keyEnc := flatEncoder{buf: keyBuf.data}
	idx.keyEnc.encodeInto(&keyEnc, valueVal)

	var valueRaw []byte
	var valueBuf []byte
	if idx.isUnique {
		buf := acquireIndexValueBuffer()
		b.rowsBuf.buffers[len(b.rowsBuf.buffers)-1].value = buf
		valueBuf = buf.data
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

	b.rows = append(b.rows, IndexRow{indexOrd, idx, keyEnc.finalize(), keyBuf.data, valueRaw, valueBuf})
	return &b.rows[len(b.rows)-1]
}

func (b *IndexBuilder) release(tx *Tx) {
	for _, buffers := range b.rowsBuf.buffers {
		releaseKeyBuffer(buffers.key)
		if buffers.value != nil {
			tx.addIndexValueBuf(buffers.value)
		}
	}
	releaseIndexRowsBuffer(b.rowsBuf)
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
