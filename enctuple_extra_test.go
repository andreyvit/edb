package edb

import "testing"

func TestTuple_EqualAndLen(t *testing.T) {
	a := tuple{[]byte{1}, []byte{2, 3}}
	b := tuple{[]byte{1}, []byte{2, 3}}
	c := tuple{[]byte{1}, []byte{9}}

	if !a.Equal(b) || a.Equal(c) {
		t.Fatalf("tuple.Equal returned unexpected results")
	}
	if a.len() < len(a.encode(nil)) {
		t.Fatalf("tuple.len() = %d, encode len = %d; wanted len() >= encoded length", a.len(), len(a.encode(nil)))
	}
}
