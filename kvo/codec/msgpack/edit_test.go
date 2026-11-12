package msgpack

import (
	"bytes"
	"testing"
)

func TestApplyMapEdits(t *testing.T) {
	raw := []byte{0x84, 0xa1, 'a', 1, 0xa1, 'b', 2, 0xa1, 'c', 0xcc, 3, 0xa1, 'a', 4}
	frozen := bytes.Clone(raw)
	v := Open(raw)
	edits := []FieldEdit{{"a", []byte{5}}, {"b", nil}, {"n", []byte{0xc0}}, {"zero", []byte{0}}, {"a", []byte{7}}}
	got := v.Apply(edits)
	if got.Lookup("a").Int() != 7 || !got.Lookup("b").Missing() || got.Lookup("n").Missing() || !got.Lookup("n").Nil() || got.Lookup("zero").Missing() || got.Lookup("zero").Int() != 0 {
		t.Fatal(got.Raw())
	}
	if !bytes.Equal(raw, frozen) || !bytes.Equal(got.Lookup("c").Raw(), v.Lookup("c").Raw()) {
		t.Fatal("input or untouched wire value changed")
	}
	if n := testing.AllocsPerRun(100, func() { _ = v.Apply(edits) }); n != 1 {
		t.Fatalf("batch: %g allocations", n)
	}
	next := got.Apply([]FieldEdit{{"a", nil}, {"n", nil}})
	if it := next.Iter(); it.Len() != 2 || got.Lookup("a").Int() != 7 {
		t.Fatal(next.Raw(), got.Raw())
	}
	if empty := (View{}).Apply([]FieldEdit{{"missing", nil}}); !bytes.Equal(empty.Raw(), []byte{0x80}) {
		t.Fatal(empty.Raw())
	}
}
