package msgpack

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	upstream "github.com/vmihailenco/msgpack/v5"
)

func TestViewPreservesUnknownFields(t *testing.T) {
	// Unknown ext payload, nested array and non-minimal integer widths.
	raw := []byte{0x83, 0xa1, 'x', 0xd1, 0, 7, 0xa1, 'u', 0xd6, 42, 1, 2, 3, 4, 0xa1, 'a', 0x92, 0xc0, 0x81, 0xa1, 'b', 0xc3}
	v, err := TryOpen(raw)
	if err != nil {
		t.Fatal(err)
	}
	if v.Lookup("x").Int() != 7 {
		t.Fatal("integer")
	}
	original := bytes.Clone(raw)
	next := v.Set("x", []byte{0xff})
	if next.Lookup("x").Int() != -1 || !bytes.Equal(next.Lookup("u").Raw(), v.Lookup("u").Raw()) || !bytes.Equal(next.Lookup("a").Raw(), v.Lookup("a").Raw()) || !bytes.Equal(raw, original) {
		t.Fatal("rewrite corrupted untouched data")
	}
	added := next.Set("new", []byte{0xc3})
	if !added.Lookup("new").Bool() {
		t.Fatal("insert")
	}
	if !bytes.Equal(next.Raw(), v.Set("x", []byte{0xff}).Raw()) {
		t.Fatal("subsequent write changed snapshot")
	}
	duplicate := Trusted([]byte{0x82, 0xa1, 'x', 1, 0xa1, 'x', 2})
	if duplicate.Lookup("x").Int() != 2 || duplicate.Set("x", []byte{3}).Lookup("x").Int() != 3 {
		t.Fatal("duplicate key behavior")
	}
}
func TestViewAllTags(t *testing.T) {
	for code := 0; code < 256; code++ {
		raw := append([]byte{byte(code)}, make([]byte, 256)...)
		// Determine one value's size independently with the released decoder.
		// Reserved 0xc1 is invalid; ext types have no registered decoder, but Open
		// must still accept their structurally valid byte spans.
		if code == 0xc1 {
			if _, err := TryOpen(raw[:1]); err == nil {
				t.Fatal("reserved tag")
			}
			continue
		}
		reader := bytes.NewReader(raw)
		if err := upstream.NewDecoder(reader).Skip(); err != nil {
			t.Fatalf("tag %x: %v", code, err)
		}
		n := len(raw) - reader.Len()
		if _, err := TryOpen(raw[:n]); err != nil {
			t.Fatalf("tag %x: %v", code, err)
		}
		for i := 0; i < n; i++ {
			if _, err := TryOpen(raw[:i]); err == nil {
				t.Fatalf("tag %x truncation %d accepted", code, i)
			}
		}
	}
	for _, x := range []any{int64(-129), uint64(1 << 63), float64(1.25), "hello", []byte{1, 2}, map[string]any{"x": []any{true, nil, 4}}} {
		raw, err := TryEncode(nil, reflect.ValueOf(x))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = TryOpen(raw); err != nil {
			t.Fatal(err)
		}
	}
	huge := binary.BigEndian.AppendUint32([]byte{0xdf}, 0xffffffff)
	if _, err := TryOpen(huge); err == nil {
		t.Fatal("huge container")
	}
	deep := append(bytes.Repeat([]byte{0x91}, 258), 0xc0)
	if _, err := TryOpen(deep); err == nil {
		t.Fatal("deep container")
	}
}
func FuzzView(f *testing.F) {
	for _, b := range [][]byte{{0x80}, {0x81, 0xa1, 'x', 1}, {0x91, 0xc0}, {0xc9, 0xff, 0xff, 0xff, 0xff, 1}} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := TryOpen(b)
		if err != nil {
			return
		}
		reader := bytes.NewReader(b)
		if err := upstream.NewDecoder(reader).Skip(); err != nil || reader.Len() != 0 {
			t.Fatalf("accepted invalid value: %v", err)
		}
		if len(v.Raw()) != len(b) {
			t.Fatal("length")
		}
	})
}
