package edb

import "testing"

func TestIndexRows_Swap(t *testing.T) {
	a := indexRows{
		{Index: &Index{pos: 1}, KeyRaw: []byte{1}},
		{Index: &Index{pos: 2}, KeyRaw: []byte{2}},
	}
	a.Swap(0, 1)
	if a[0].Index.pos != 2 || a[1].Index.pos != 1 {
		t.Fatalf("Swap did not swap items as expected: %#v", a)
	}
}

