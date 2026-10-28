package kvodraft

import (
	"bytes"
	"encoding/hex"
	"math"
	"testing"
)

func TestChanges(t *testing.T) {
	c := Changes{Base: 41, Next: 42, Items: []Change{
		{Path: []Step{{Kind: FieldStep, Number: 1}}, Value: Uint(7)},
		{Path: []Step{{Kind: FieldStep, Number: 2}, {Kind: StringStep, Text: "unknown"}}, Value: ByteString([]byte{0xff, 0x80})},
		{Path: []Step{{Kind: FieldStep, Number: 2}, {Kind: StringStep, Text: "other"}}, Delete: true},
		{Path: []Step{{Kind: NumberStep, Number: math.MaxUint64}}, Value: ZeroValue()},
		{Path: []Step{{Kind: FieldStep, Number: 4}, {Kind: IndexStep, Number: 3}}, Value: Int(-2)},
	}}
	raw := AppendChanges(nil, c)
	var scratch [8]PathPart
	r, e := OpenChanges(raw, scratch[:])
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range c.Items {
		got, ok, e := r.NextChange()
		if e != nil || !ok {
			t.Fatal("read", e)
		}
		if got.Delete != want.Delete || len(got.Path) != len(want.Path) {
			t.Fatal("change")
		}
		for j, w := range want.Path {
			p := got.Path[j]
			if p.Kind != w.Kind {
				t.Fatal("step kind")
			}
			if p.Kind == StringStep {
				if string(r.Key(p)) != w.Text {
					t.Fatal("key")
				}
			} else if p.Number != w.Number {
				t.Fatal("number")
			}
		}
		if !want.Delete {
			checkValue(t, want.Value, got.Value)
		}
	}
	if _, ok, e := r.NextChange(); e != nil || ok {
		t.Fatal("end")
	}
	for i := 0; i < len(raw); i++ {
		r, e := OpenChanges(raw[:i], scratch[:])
		if e == nil {
			var ok bool
			for {
				_, ok, e = r.NextChange()
				if e != nil || !ok {
					break
				}
			}
		}
		if e == nil {
			t.Fatalf("truncation %d", i)
		}
	}
	if n := testing.AllocsPerRun(100, func() {
		var scratch [8]PathPart
		r, e := OpenChanges(raw, scratch[:])
		if e != nil {
			panic(e)
		}
		for {
			_, ok, e := r.NextChange()
			if e != nil {
				panic(e)
			}
			if !ok {
				break
			}
		}
	}); n != 0 {
		t.Fatalf("allocations %g", n)
	}
}
func TestChangeGolden(t *testing.T) {
	raw := AppendChanges(nil, Changes{Base: 4, Next: 5, Items: []Change{{Path: []Step{{Kind: FieldStep, Number: 1}}, Value: Uint(7)}}})
	if hex.EncodeToString(raw) != "040501000204020207" {
		t.Fatalf("got %x", raw)
	}
}
func FuzzChanges(f *testing.F) {
	raw := AppendChanges(nil, Changes{Base: 1, Next: 2, Items: []Change{{Path: []Step{{Kind: StringStep, Text: "key"}}, Value: Uint(9)}}})
	f.Add(raw)
	f.Add([]byte{1, 2, 1})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		var scratch [16]PathPart
		r, e := OpenChanges(b, scratch[:])
		if e != nil {
			return
		}
		for i := 0; i <= len(b); i++ {
			_, ok, e := r.NextChange()
			if e != nil || !ok {
				return
			}
		}
		t.Fatal("unbounded")
	})
}
func TestChangePayloadOpaque(t *testing.T) {
	raw := AppendChanges(nil, Changes{Base: 1, Next: 2, Items: []Change{{Path: []Step{{Kind: FieldStep, Number: 100}}, Value: ByteString([]byte{0xff})}, {Path: []Step{{Kind: FieldStep, Number: 101}}, Value: Uint(3)}}})
	var scratch [1]PathPart
	r, e := OpenChanges(raw, scratch[:])
	if e != nil {
		t.Fatal(e)
	}
	a, _, e := r.NextChange()
	if e != nil {
		t.Fatal(e)
	}
	payload := a.Value.Bytes()
	if !bytes.Equal(payload, []byte{0xff}) {
		t.Fatal("opaque")
	}
	b, ok, e := r.NextChange()
	n := b.Value.Uint()
	if e != nil || !ok || n != 3 {
		t.Fatal("following change")
	}
}

func TestZeroChangesAndGrowingPath(t *testing.T) {
	path := []Step{{Kind: FieldStep, Number: 1}, {Kind: StringStep, Text: "nested"}, {Kind: IndexStep, Number: 2}}
	encoded := AppendChanges(nil, Changes{Base: 1, Next: 2, Items: []Change{
		{Path: path, Value: Uint(0)},
		{Path: path, Value: Uint(9)},
		{Path: path, Delete: true},
	}})
	// Nil scratch grows automatically. Zero Set remains an operation, distinct
	// from no operation and from Delete (which matters for list positions).
	reader, err := OpenChanges(encoded, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		change, ok, err := reader.NextChange()
		if err != nil || !ok || len(change.Path) != len(path) {
			t.Fatal("change", err)
		}
		if string(reader.Key(change.Path[1])) != "nested" {
			t.Fatal("prefix after growth")
		}
		if i == 0 && (change.Delete || !change.Value.IsZero()) {
			t.Fatal("lost zero assignment")
		}
		if i == 1 && (change.Delete || change.Value.Uint() != 9) {
			t.Fatal("set")
		}
		if i == 2 && !change.Delete {
			t.Fatal("delete")
		}
	}
	if _, ok, err := reader.NextChange(); ok || err != nil {
		t.Fatal("end", err)
	}
	expectPanic(t, ErrValue, func() { AppendChanges(nil, Changes{Base: 2, Next: 2}) })
	expectPanic(t, ErrValue, func() { reader.Key(PathPart{Kind: IndexStep}) })
}
