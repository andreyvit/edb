package kvolab

import (
	"bytes"
	"math"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func TestCurrentSignedChanges(t *testing.T) {
	for _, n := range currentSignedBoundaries() {
		path := []draft.Step{{Kind: draft.NumberStep, Number: math.MaxUint64}, {Kind: draft.StringStep, Text: "arbitrary"}, {Kind: draft.IndexStep, Number: 3}}
		batch := currentChanges{Base: 7, Next: 8, Items: []currentChange{
			{Path: path, Value: currentInt(n)},
			{Path: path, Value: currentInt(0)},
			{Path: path, Delete: true},
			{Path: nil, Value: currentBytes(bytes.Repeat([]byte{0xab}, 127))},
		}}
		var w currentChangeWriter
		raw := w.Append(nil, batch)
		var scratch [8]lengthPathPart
		r, err := openCurrentChanges(raw, scratch[:0])
		if err != nil {
			t.Fatal(err)
		}
		for pass := 0; pass < 2; pass++ {
			for i, want := range batch.Items {
				got, ok := r.NextChange()
				if !ok || got.Delete != want.Delete || len(got.Path) != len(want.Path) {
					t.Fatal("operation")
				}
				if i < 3 && (got.Path[0].Number != math.MaxUint64 || string(r.Key(got.Path[1])) != "arbitrary" || got.Path[2].Number != 3) {
					t.Fatal("path")
				}
				if i == 0 && got.Value.Int() != n {
					t.Fatalf("change %d -> %d", n, got.Value.Int())
				}
				if i == 1 && (!got.Value.IsZero() || got.Delete) {
					t.Fatal("Set-zero became Delete")
				}
				if i == 3 && !bytes.Equal(got.Value.Bytes(), want.Value.Bytes()) {
					t.Fatal("extended length")
				}
			}
			if _, ok := r.NextChange(); ok {
				t.Fatal("extra operation")
			}
			r.Reset()
		}
	}
}

func TestCurrentChangesDirectAndUnknown(t *testing.T) {
	var w currentChangeWriter
	w.Reset(make([]byte, 0, 2048), 1, 2)
	path := []draft.Step{{Kind: draft.FieldStep, Number: 1}}
	w.Put(path, currentInt(-129))
	path[0].Number = 2 // The writer retains the prior path, not caller scratch.
	start := len(w.out)
	var row currentWriter
	row.Reset(w.out)
	row.Begin(0, "", 0, false)
	row.Put(1, "", currentInt(-1))
	row.End()
	w.out = row.BytesWritten()
	w.RecordSpan(path, start)
	path[0].Number = 3
	w.Put(path, currentBytes([]byte{0xff, 0xff, 0xff}))
	raw := w.Finish()
	r, err := openCurrentChanges(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := r.NextChange()
	if first.Path[0].Number != 1 || first.Value.Int() != -129 {
		t.Fatal("borrowed path changed")
	}
	second, _ := r.NextChange()
	if second.Path[0].Number != 2 || currentTrusted(second.Value.Bytes()).Get(1).Int() != -1 {
		t.Fatal("streamed child")
	}
	third, _ := r.NextChange()
	if third.Path[0].Number != 3 || third.Value.Int() != -1 || len(third.Value.Bytes()) != 3 {
		t.Fatal("unknown signed width")
	}
	var copyWriter currentChangeWriter
	copyWriter.Reset(nil, 1, 2)
	r.Reset()
	for {
		item, ok := r.NextChange()
		if !ok {
			break
		}
		copyWriter.Put([]draft.Step{{Kind: draft.FieldStep, Number: item.Path[0].Number}}, item.Value)
	}
	if !bytes.Equal(raw, copyWriter.Finish()) {
		t.Fatal("unknown change rewrite")
	}
}

func TestCurrentChangeAllocations(t *testing.T) {
	var w currentChangeWriter
	batch := currentChanges{Base: 1, Next: 2, Items: []currentChange{{Path: []draft.Step{{Number: 1}}, Value: currentInt(-129)}}}
	dst := w.Append(nil, batch)
	if n := testing.AllocsPerRun(100, func() { dst = w.Append(dst[:0], batch) }); n != 0 {
		t.Fatalf("write allocations=%g", n)
	}
	// The returning reader retains caller path scratch. Reuse it across opens;
	// the compiler currently lets per-call local scratch escape in this API.
	var scratch [8]lengthPathPart
	if n := testing.AllocsPerRun(100, func() {
		r, err := openCurrentChanges(dst, scratch[:0])
		if err != nil {
			panic(err)
		}
		item, ok := r.NextChange()
		if !ok || item.Value.Int() != -129 {
			panic("read")
		}
	}); n != 0 {
		t.Fatalf("read allocations=%g", n)
	}
}

func FuzzCurrentChanges(f *testing.F) {
	var w currentChangeWriter
	f.Add(w.Append(nil, currentChanges{Base: 1, Next: 2}))
	f.Add(w.Append(nil, currentChanges{Base: 1, Next: 2, Items: []currentChange{{Value: currentInt(-129)}, {Delete: true}}}))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			t.Skip()
		}
		var scratch [8]lengthPathPart
		r, err := openCurrentChanges(raw, scratch[:0])
		if err != nil {
			if err != draft.ErrFormat {
				t.Fatal(err)
			}
			return
		}
		count := 0
		for {
			if _, ok := r.NextChange(); !ok {
				break
			}
			count++
		}
		if count != r.count {
			t.Fatal("count")
		}
	})
}
