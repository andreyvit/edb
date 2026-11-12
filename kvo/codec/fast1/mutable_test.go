package fast1

import (
	"slices"
	"testing"
)

func TestMutableRecord_zero_fields_in_new_record(t *testing.T) {
	l := NewRecord(nil)
	l.Set(1, 0)
	l.Set(42, 0)
	eq(t, l.rec.Pack().HexString(), "01 02:00")
}

func TestMutableRecord_zero_fields_updating_existing_record(t *testing.T) {
	l := NewRecord(nil)
	l.Set(0x10, 1)
	l.Set(0x42, 2)
	p := l.rec.PackedRecord()
	eq(t, p.Data().HexString(), "01 02:20 10 42 01 02")

	l = UpdateRecord(p)
	l.Set(0x01, 0)
	l.Set(0x42, 0)
	eq(t, l.rec.Pack().HexString(), "01 02:10 10 01")
}

func TestPackMergesSortedKeysAndEdits(t *testing.T) {
	root := NewRecord(nil)
	for key := uint64(1); key < 64; key += 2 {
		root.Set(key, key*10)
	}
	before := root.Record().PackedRecord()
	edit := UpdateRecord(before)
	for key := uint64(54); key > 0; key-- {
		if key%3 == 0 {
			edit.Set(key, 0)
		} else {
			edit.Set(key, 1000+key)
		}
	}
	order := edit.Keys()
	after := edit.Record().PackedRoot()
	var wantKeys []uint64
	for key := uint64(1); key < 64; key++ {
		want := before.Root().Get(key)
		if key <= 54 {
			if key%3 == 0 {
				want = 0
			} else {
				want = 1000 + key
			}
		}
		if got := after.Get(key); got != want {
			t.Fatalf("key %d: got %d want %d", key, got, want)
		}
		if want != 0 {
			wantKeys = append(wantKeys, key)
		}
	}
	if !slices.Equal(after.Keys(), wantKeys) || !slices.Equal(order, edit.Keys()) {
		t.Fatalf("packed keys %v, edit order %v", after.Keys(), edit.Keys())
	}
}
