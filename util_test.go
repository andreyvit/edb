package edb

import (
	"log/slog"
	"testing"
)

func TestSplitByte(t *testing.T) {
	a, b, ok := splitByte("a:b", ':')
	if !ok || a != "a" || b != "b" {
		t.Fatalf("splitByte = (%q, %q, %v), wanted (\"a\", \"b\", true)", a, b, ok)
	}

	a, b, ok = splitByte("ab", ':')
	if ok || a != "ab" || b != "" {
		t.Fatalf("splitByte(no sep) = (%q, %q, %v), wanted (\"ab\", \"\", false)", a, b, ok)
	}
}

func TestRpad(t *testing.T) {
	if got := rpad("abc", 5, '.'); got != "abc.." {
		t.Fatalf("rpad = %q, wanted %q", got, "abc..")
	}
	if got := rpad("abc", 1, '.'); got != "abc" {
		t.Fatalf("rpad = %q, wanted %q", got, "abc")
	}
}

func TestIncDec(t *testing.T) {
	b := []byte{0x00, 0x00}
	if !inc(b) || b[0] != 0x00 || b[1] != 0x01 {
		t.Fatalf("inc = %x, wanted 0001", b)
	}
	if !dec(b) || b[0] != 0x00 || b[1] != 0x00 {
		t.Fatalf("dec = %x, wanted 0000", b)
	}
	if dec([]byte{0x00}) {
		t.Fatalf("dec(00) = true, wanted false")
	}
	if inc([]byte{0xFF}) {
		t.Fatalf("inc(FF) = true, wanted false")
	}
}

func TestHexHelpers(t *testing.T) {
	if got := hexstr(nil); got != "<nil>" {
		t.Fatalf("hexstr(nil) = %q, wanted <nil>", got)
	}
	if got := hexstr([]byte{}); got != "<empty>" {
		t.Fatalf("hexstr(empty) = %q, wanted <empty>", got)
	}
	if got := hexstr([]byte{0xAA, 0xBB}); got != "aabb" {
		t.Fatalf("hexstr = %q, wanted aabb", got)
	}
	a := hexAttr("k", []byte{0xAA})
	if a.Key != "k" || a.Value.Kind() != slog.KindString {
		t.Fatalf("hexAttr returned unexpected attr: %+v", a)
	}
}

func TestContainsBytes(t *testing.T) {
	list := [][]byte{[]byte{1, 2}, []byte{3}}
	if !containsBytes(list, []byte{1, 2}) {
		t.Fatalf("containsBytes should find existing item")
	}
	if containsBytes(list, []byte{2, 1}) {
		t.Fatalf("containsBytes should not find non-existing item")
	}
}

