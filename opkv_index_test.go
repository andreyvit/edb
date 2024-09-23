package edb

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/andreyvit/edb/kvo"
)

var (
	wumpetsByB *KVIndex
	wumpets    = DefineKVTable(basicSchema, "wumpets", nil, nil, func(b *KVTableBuilder) {
		wumpetsByB = b.DefineIndex("b", nil, func(ik []byte) []byte {
			return ik[2:4]
		}, func(b *KVIndexContentBuilder, pk []byte, v kvo.ImmutableRecord) {
			iv := v.Root().Get(0x42)
			ik := make([]byte, 4)
			binary.BigEndian.PutUint16(ik[0:2], uint16(iv))
			copy(ik[2:4], pk)
			b.Add(ik)
		})
	})
)

func TestKVIndexScan(t *testing.T) {
	var (
		k1  = x("10 12")
		k2  = x("10 14")
		k3  = x("10 16")
		k4  = x("10 18")
		v1  = buildKV(0x42, 0x0055)
		v2  = buildKV(0x42, 0x8877)
		v3  = buildKV(0x42, 0x8899)
		ik1 = x("00 55 10 16")
		ik2 = x("88 77 10 12")
		ik3 = x("88 77 10 14")
		ik4 = x("88 99 10 18")
	)
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		tx.KVPutRaw(wumpets, k1, v2.Bytes())
		tx.KVPutRaw(wumpets, k2, v2.Bytes())
		tx.KVPutRaw(wumpets, k3, v1.Bytes())
		tx.KVPutRaw(wumpets, k4, v3.Bytes())

		indexScanIKs(t, tx, wumpetsByB, RawRange{}, ik1, ik2, ik3, ik4)
		indexScanIKs(t, tx, wumpetsByB, RawRange{Prefix: x("88 77")}, ik2, ik3)
		indexScan(t, tx, wumpetsByB, RawRange{}, k3, k1, k2, k4)
		indexScan(t, tx, wumpetsByB, RawRange{Prefix: x("88 77")}, k1, k2)
		indexScan(t, tx, wumpetsByB, RawRange{Prefix: x("00 55")}, k3)
		indexScan(t, tx, wumpetsByB, RawRange{Prefix: x("88 99")}, k4)
	})
}

func indexScanIKs(t testing.TB, tx *Tx, idx *KVIndex, rang RawRange, exp ...[]byte) {
	t.Helper()
	var out []string
	for k := range tx.KVIndexScan(idx, rang).IndexKeys() {
		out = append(out, hex.EncodeToString(k))
	}
	var expstr []string
	for _, k := range exp {
		expstr = append(expstr, hex.EncodeToString(k))
	}
	deepEqual(t, out, expstr)
}

func indexScan(t testing.TB, tx *Tx, idx *KVIndex, rang RawRange, exp ...[]byte) {
	t.Helper()
	var out []string
	for k := range tx.KVIndexScan(idx, rang).Keys() {
		out = append(out, hex.EncodeToString(k))
	}
	var expstr []string
	for _, k := range exp {
		expstr = append(expstr, hex.EncodeToString(k))
	}
	deepEqual(t, out, expstr)
}

func buildKV(kvs ...uint64) kvo.ImmutableRecordData {
	if len(kvs)%2 != 0 {
		panic("buildKV: odd number of arguments")
	}
	r := kvo.NewRecord(nil)
	for i := 0; i < len(kvs); i += 2 {
		r.Set(kvs[i], kvs[i+1])
	}
	return r.Record().Pack()
}
