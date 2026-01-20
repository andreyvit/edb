package kvo

import (
	"reflect"
	"testing"
)

func TestImmutableRecord_LoadRecordDataAndAccessors(t *testing.T) {
	root := NewRecord(nil)
	root.Set(1, 42)
	root.Set(2, 0) // should be omitted by packing

	data := root.Record().Pack()
	if data == nil || len(data) == 0 {
		t.Fatalf("Pack returned empty data")
	}

	rec := LoadRecord(data.Bytes(), nil)
	m := rec.Root()
	eq(t, m.Get(1), uint64(42))
	eq(t, m.KeyCount(), 1)

	var keys []uint64
	for k := range m.KeySeq() {
		keys = append(keys, k)
	}
	eq(t, len(keys), 1)
	eq(t, keys[0], uint64(1))

	var items [][2]uint64
	for k, v := range m.Items() {
		items = append(items, [2]uint64{k, v})
	}
	eq(t, len(items), 1)
	eq(t, items[0][0], uint64(1))
	eq(t, items[0][1], uint64(42))

	if !reflect.DeepEqual(rec.Pack(), rec.Data()) {
		t.Fatalf("Pack() and Data() returned different slices")
	}
	if !reflect.DeepEqual(data.Pack(), data) {
		t.Fatalf("ImmutableRecordData.Pack() returned different slice")
	}
	_ = rec.AnyRoot()
	_ = m.RecordWithThisRoot()
	_ = m.RecordData()
	_ = m.Packable()
	_ = EmptyImmutableRecordData()
	_ = data.Uints()
	_ = data.HexString()

	if b := data.bytes(0, 8); len(b) != 8 {
		t.Fatalf("bytes(0,8) returned %d bytes, wanted 8", len(b))
	}
}

func TestLoadRecordData_PanicsOnMisalignedLength(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	_ = LoadRecordData([]byte{1})
}
