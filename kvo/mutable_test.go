package kvo

import "testing"

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
