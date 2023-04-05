package edb

import (
	"bytes"
	"encoding/binary"

	"go.etcd.io/bbolt"
)

func appendIndexKeys(buf []byte, rows []IndexRow) []byte {
	var total = binary.MaxVarintLen32 + len(rows)*(binary.MaxVarintLen32+binary.MaxVarintLen32)
	for _, row := range rows {
		total += len(row.KeyRaw)
	}

	w := prealloc(buf, total)
	w.AppendUvarinti(len(rows))
	for _, row := range rows {
		w.AppendUvarint(row.IndexOrd)
		w.AppendVarBytes(row.KeyRaw)
	}
	return w.Trimmed()
}

func decodeIndexKeys(data []byte, f func(ord uint64, key []byte)) {
	d := makeByteDecoder(data)
	n := must(d.Uvarinti())
	for i := 0; i < n; i++ {
		ord := must(d.Uvarint())
		key := must(d.VarBytes())
		f(ord, key)
	}
}

type indexDiffer struct {
	newRows indexRows
}

func (d *indexDiffer) checkOldKey(oldOrd uint64, oldKey []byte) bool {
	// Look for a new row that's >= old row.
	for len(d.newRows) > 0 {
		newOrd := d.newRows[0].IndexOrd
		if oldOrd < newOrd {
			return false
		} else if oldOrd == newOrd {
			c := bytes.Compare(oldKey, d.newRows[0].KeyRaw)
			if c < 0 {
				return false
			} else if c == 0 {
				return true // found exact match
			}
		}
		d.newRows = d.newRows[1:] // shift to next new row and compare again
	}
	return false // no more new rows, so remaining old rows have been deleted
}

func findRemovedIndexKeys(oldData []byte, newRows indexRows, removed func(ord uint64, key []byte)) {
	d := indexDiffer{newRows}
	decodeIndexKeys(oldData, func(ord uint64, key []byte) {
		if !d.checkOldKey(ord, key) {
			removed(ord, key)
		}
	})
}

func prepareToDeleteIndexEntries(tableBuck *bbolt.Bucket, ts *tableState) func(ord uint64, key []byte) {
	var idxOrd uint64
	var idxBuck *bbolt.Bucket

	return func(ord uint64, key []byte) {
		if idxOrd != ord {
			idxOrd = ord
			if idx := ts.indexByOrdinal(ord); idx != nil {
				idxBuck = nonNil(tableBuck.Bucket(idx.buck.Raw()))
			} else {
				idxBuck = nil
			}
		}
		if idxBuck != nil { // If the index no longer exists, we don't spend time deleting values from it.
			ensure(idxBuck.Delete(key))
		}
	}
}
