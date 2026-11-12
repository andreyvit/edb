package kvo_test

import (
	"bytes"
	"runtime"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

var retainedRecord kvo.Record
var retainedValue values.Value

func TestCompactRecordReads(t *testing.T) {
	if n := unsafe.Sizeof(kvo.Record{}); n != 16 {
		t.Fatalf("Record is %d bytes", n)
	}
	_, _, fixtures := recordHandleFixtures(t)
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			root := f.open(13)
			child := root.Child(row2.Step{Key: 9})
			want := child.GetValue(5)
			if got := kvo.FromValue(root.Value()); got.GetValue(1).AsUint() != 14 {
				t.Fatal("cell transport")
			}
			if n := testing.AllocsPerRun(100, func() {
				retainedRecord = f.open(13)
				retainedValue = retainedRecord.GetValue(1)
				retainedRecord = retainedRecord.Child(row2.Step{Key: 9})
				retainedValue = retainedRecord.GetValue(5)
			}); n != 0 {
				t.Fatalf("read/retain/child allocated %g times", n)
			}
			for i := 0; i < handleRows; i++ {
				retainedRecord = f.open(i)
			}
			runtime.GC()
			if child.GetValue(5) != want {
				t.Fatal("retained child changed")
			}
		})
	}
	if !kvo.Encoded(row2.Trusted([]byte{})).Missing() {
		t.Fatal("empty span is not missing")
	}
}

func TestCompactRecordOwn(t *testing.T) {
	_, model, fixtures := recordHandleFixtures(t)
	for _, f := range fixtures[:2] {
		t.Run(f.name, func(t *testing.T) {
			original := bytes.Clone(f.open(7).Snapshot().Raw())
			r := kvo.Encoded(row2.Trusted(original)).WithModel(model)
			owned := r.Own()
			child := owned.Child(row2.Step{Key: 9})
			want := child.GetValue(5)
			clear(original)
			runtime.GC()
			if !owned.OwnsData() || !child.OwnsData() || owned.GetValue(1).AsUint() != 8 || child.GetValue(5) != want {
				t.Fatal("ownership lost")
			}
			if owned.Own().Value() != owned.Value() {
				t.Fatal("repeated ownership copied")
			}
		})
	}
}
