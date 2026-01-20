package edb

import (
	"encoding/hex"
	"reflect"
	"testing"
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

