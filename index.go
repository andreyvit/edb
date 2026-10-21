package edb

import (
	"bytes"
	"cmp"
	"fmt"
	"reflect"
	"slices"
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
		buf.data = valueRaw[:0]
		valueBuf = buf.data
	} else {
		valueRaw = emptyIndexValue

		keyEnc.begin()
		keyEnc.buf = appendRaw(keyEnc.buf, b.key)
	}

	indexOrd := b.ts.indexOrdinal(idx)
	keyRaw := keyEnc.finalize()
	keyBuf.data = keyRaw[:0]

	b.rows = append(b.rows, IndexRow{indexOrd, idx, keyRaw, keyBuf.data, valueRaw, valueBuf})
	return &b.rows[len(b.rows)-1]
}

func (b *IndexBuilder) transferValuesTo(tx *Tx) {
	for i := range b.rowsBuf.buffers {
		buffers := &b.rowsBuf.buffers[i]
		if buffers.value != nil {
			tx.addIndexValueBuf(buffers.value)
			buffers.value = nil
		}
	}
}

func (b *IndexBuilder) release() {
	for _, buffers := range b.rowsBuf.buffers {
		releaseKeyBuffer(buffers.key)
		if buffers.value != nil {
			releaseIndexValueBuffer(buffers.value)
		}
	}
	releaseIndexRowsBuffer(b.rowsBuf)
}

func (b *IndexBuilder) finalize() {
	slices.SortFunc(b.rows, compareIndexRows)
}

type indexRows []IndexRow

func (a indexRows) Len() int      { return len(a) }
func (a indexRows) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a indexRows) Less(i, j int) bool {
	return compareIndexRows(a[i], a[j]) < 0
}

func compareIndexRows(a, b IndexRow) int {
	if order := cmp.Compare(a.Index.pos, b.Index.pos); order != 0 {
		return order
	}
	return bytes.Compare(a.KeyRaw, b.KeyRaw)
}
