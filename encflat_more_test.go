package edb

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMarshalFlat_SmallerIntegers(t *testing.T) {
	tests := []struct {
		input      any
		decodeBase any
	}{
		{uint8(0x42), uint8(0)},
		{uint16(0x4243), uint16(0)},
		{uint32(0x42434445), uint32(0)},
	}

	for _, tt := range tests {
		inputVal := reflect.ValueOf(tt.input)
		enc := flatEncodingOf(inputVal.Type())
		raw := enc.encode(nil, inputVal)
		t.Logf("flat %T => %s", tt.input, hex.EncodeToString(raw))

		out := reflect.New(reflect.TypeOf(tt.decodeBase))
		if err := enc.decodePtr(raw, out); err != nil {
			t.Fatalf("decode %T failed: %v", tt.input, err)
		}
		if got := out.Elem().Interface(); got != tt.input {
			t.Fatalf("roundtrip %T: got %v, wanted %v", tt.input, got, tt.input)
		}
	}
}

func TestPathPrefix(t *testing.T) {
	if got := pathPrefix(""); got != "" {
		t.Fatalf("pathPrefix(\"\") = %q, wanted empty", got)
	}
	if got := pathPrefix(".Foo"); got == "" {
		t.Fatalf("pathPrefix should return non-empty for non-empty paths")
	}
}

func TestFlatEncoding_BoundaryValues(t *testing.T) {
	tests := []any{
		int8(-128),
		int8(127),
		int64(-1 << 63),
		int64(1<<63 - 1),
		uint64(0),
		uint64(1<<64 - 1),
		time.Unix(-1, 0),
		time.Unix(1<<31, 0),
		[4]byte{0, 1, 0xfe, 0xff},
	}

	for _, in := range tests {
		enc := flatEncodingOf(reflect.TypeOf(in))
		raw := enc.encode(nil, reflect.ValueOf(in))
		out := reflect.New(reflect.TypeOf(in))
		if err := enc.decodePtr(raw, out); err != nil {
			t.Fatalf("decode %T failed: %v", in, err)
		}
		if got := out.Elem().Interface(); got != in {
			t.Fatalf("roundtrip %T = %v, wanted %v", in, got, in)
		}
	}
}

func TestFlatEncoding_InvalidInputs(t *testing.T) {
	assertFlatDecodeError(t, uint64(0), tuple{[]byte{1}}.encode(nil), "invalid int length")
	assertFlatDecodeError(t, time.Time{}, tuple{[]byte{1}}.encode(nil), "invalid time.Time data length")
	assertFlatDecodeError(t, [4]byte{}, tuple{[]byte{1, 2, 3}}.encode(nil), "invalid data length")

	if _, err := flatEncodingOf(reflect.TypeOf("")).tryTupleToStrings(tuple{[]byte{0xff}}); err == nil {
		t.Fatalf("tryTupleToStrings accepted invalid UTF-8")
	}
	if _, err := flatEncodingOf(reflect.TypeOf(uint64(0))).stringsToRawKey(nil, []string{"nope"}); err == nil {
		t.Fatalf("stringsToRawKey accepted invalid uint")
	}
	if _, err := flatEncodingOf(reflect.TypeOf([]byte{})).stringsToRawKey(nil, []string{"xx"}); err == nil {
		t.Fatalf("stringsToRawKey accepted invalid hex")
	}
	if _, err := flatEncodingOf(reflect.TypeOf(uint64(0))).stringsToRawKey(nil, nil); err == nil {
		t.Fatalf("stringsToRawKey accepted wrong component count")
	}
}

func assertFlatDecodeError(t testing.TB, sample any, raw []byte, contains string) {
	t.Helper()
	err := flatEncodingOf(reflect.TypeOf(sample)).decodePtr(raw, reflect.New(reflect.TypeOf(sample)))
	if err == nil {
		t.Fatalf("decode %T returned nil error", sample)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Fatalf("decode %T error = %q, wanted substring %q", sample, err.Error(), contains)
	}
}
