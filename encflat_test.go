package edb

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

func TestMarshalFlat(t *testing.T) {
	type Foo struct {
		A int64
		B string
	}
	tests := []struct {
		input      any
		expected   string
		decodeBase any
	}{
		{"test", "74657374 01", ""},
		{0x42, "0000000000000042 01", 0},
		{Foo{0x42, "test"}, "0000000000000042 74657374 08 02", Foo{}},
		{&Foo{0x42, "test"}, "0000000000000042 74657374 08 02", &Foo{}},
		{[]byte("test"), "74657374 01", []byte(nil)},
		// {[4]byte{'t', 'e', 's', 't'}, "74657374 01", [4]byte{}},
	}
	for _, test := range tests {
		test.expected = strings.Map(removeSpaces, test.expected)
		inputVal := reflect.ValueOf(test.input)
		enc := flatEncodingOf(inputVal.Type())
		a := enc.encode(nil, inputVal)
		aStr := hex.EncodeToString(a)
		if aStr != test.expected {
			t.Errorf("** MarshalFlat(%v) = %v, wanted %q", test.input, aStr, test.expected)
		} else {
			// t.Logf("✓ MarshalFlat(%v) = %q", test.input, a)

			decodedVal := reflect.New(reflect.TypeOf(test.decodeBase))
			err := enc.decode(a, decodedVal)
			if err != nil {
				t.Errorf("** UnmarshalFlat(%s) failed: %v", aStr, err)
			} else {
				decoded := decodedVal.Elem().Interface()
				if !reflect.DeepEqual(decoded, test.input) {
					t.Errorf("** UnmarshalFlat(%s) = %v, wanted %v", aStr, decoded, test.input)
				} else {
					// t.Logf("✓ UnmarshalFlat(%s) = %v", aStr, decoded)
				}
			}
		}
	}
}

func removeSpaces(r rune) rune {
	if r == ' ' {
		return -1
	} else {
		return r
	}
}
