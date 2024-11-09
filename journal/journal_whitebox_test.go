package journal

import (
	"testing"
)

func TestParseName(t *testing.T) {
	seq, ts, id, err := parseSegmentName("123-20230101T000000-11223344aabbccdd")
	if err != nil {
		t.Fatal(err)
	}
	if e := uint32(123); seq != e {
		t.Errorf("seq = %v, expected %v", seq, e)
	}
	if e := uint32(1672531200); ts != e {
		t.Errorf("ts = %v, expected %v", ts, e)
	}
	if e := uint64(0x11223344_aabbccdd); id != e {
		t.Errorf("id = %x, expected %x", id, e)
	}
}

func TestFormatName(t *testing.T) {
	name := formatSegmentName("x", "y", 123, 1672531200, 0x11223344_aabbccdd)
	exp := "x000000000123-20230101T000000-11223344aabbccddy"
	if name != exp {
		t.Errorf("name = %q, expected %q", name, exp)
	}
}
