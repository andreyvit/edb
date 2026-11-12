//go:build !race

package kvolab

import "testing"

// The caller may keep small edit scratch on its stack. Capacity growth is the
// only intended reason for the overlay itself to allocate.
func TestLengthEditStackScratch(t *testing.T) {
	var w lengthWriter
	raw := lengthEncode(&w, nil, numeric(128, 0), nil)
	view, _ := lengthOpen(raw)
	dst := make([]byte, 0, 2048)
	var warm [8]classMeta
	e := newLengthEditor(view, warm[:0], nil)
	e.Set(3, lengthWord(7))
	dst = e.Append(&w, dst, false)
	n := testing.AllocsPerRun(100, func() {
		var scratch [8]classMeta
		e := newLengthEditor(view, scratch[:0], nil)
		e.Set(3, lengthWord(7))
		dst = e.Append(&w, dst[:0], false)
	})
	if n != 0 {
		t.Fatalf("stack numeric overlay allocates %v", n)
	}
}

func TestLengthDictionaryStackScratch(t *testing.T) {
	var w lengthWriter
	raw := lengthEncode(&w, nil, dictionary(8, false), nil)
	view, _ := lengthOpen(raw)
	dst := make([]byte, 0, 2048)
	var warm [8]classMeta
	var wa [128]byte
	e := newLengthEditor(view, warm[:0], wa[:0])
	e.SetString("new-key", lengthBytes([]byte("replacement")))
	dst = e.Append(&w, dst, false)
	n := testing.AllocsPerRun(100, func() {
		var scratch [8]classMeta
		var arena [128]byte
		e := newLengthEditor(view, scratch[:0], arena[:0])
		e.SetString("new-key", lengthBytes([]byte("replacement")))
		dst = e.Append(&w, dst[:0], false)
	})
	if n != 0 {
		t.Fatalf("stack dictionary overlay allocates %v", n)
	}
}
