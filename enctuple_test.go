package edb

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

func TestTuple(t *testing.T) {
	l1024 := longhex(1024)
	tests := []struct {
		input    string
		expected string
	}{
		{"", "01"},
		{"4241", "424101"},
		{l1024, l1024 + "01"},
		{"4241|393837", "42413938370202"},
		{"1122|334455|66778899", "112233445566778899020303"},
		{l1024 + "|" + l1024, l1024 + l1024 + "088002"},
		{"|", "0002"},
		{"||", "000003"},
		{"|||", "00000004"},
		{"||||", "0000000005"},
		{"01000000|7b224944223a312c22456d61696c223a22666f6f406578616d706c652e636f6d222c224e616d65223a22666f6f227d|", "010000007b224944223a312c22456d61696c223a22666f6f406578616d706c652e636f6d222c224e616d65223a22666f6f227d042f03"},
	}
	for _, tt := range tests {
		src := parseTupleString(tt.input)
		if src.String() != tt.input {
			t.Errorf("** parseTupleString(%q).String() does not round-trip", tt.input)
			continue
		}

		encoded := src.encode(nil)
		encodedStr := hex.EncodeToString(encoded)
		if encodedStr != tt.expected {
			t.Errorf("** tuple(%q).encode() = %q, wanted %q", tt.input, encodedStr, tt.expected)
		} else {
			decoded := must(decodeTuple(encoded))
			if !reflect.DeepEqual(src, decoded) {
				t.Errorf("** decodeTuple(%q) = %s, wanted %s", encodedStr, decoded.String(), tt.input)

			}
		}
	}
}

func parseTupleString(s string) tuple {
	els := strings.Split(s, "|")
	tup := make(tuple, len(els))
	for i, el := range els {
		tup[i] = must(hex.DecodeString(el))
	}
	return tup
}

func longhex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return hex.EncodeToString(b)
}
