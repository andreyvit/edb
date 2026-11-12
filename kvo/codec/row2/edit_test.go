package row2_test

import (
	"bytes"
	"testing"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestEditOwnershipAndPresence(t *testing.T) {
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		var w row2.Writer
		w.Begin(0, "", row2.Object, layout)
		w.PutPresent(0, "", values.Uint(0), row2.Word)
		w.Put(1, "", values.Bytes([]byte{0xff, 0xff, 0xff}), row2.Data) // unknown, deliberately non-minimal
		w.Begin(2, "", row2.Object, layout)
		w.Put(5, "", values.Int(128), row2.Signed)
		w.End()
		w.End()
		base := row2.Trusted(w.BytesWritten())
		saved := bytes.Clone(base.Raw())
		edit := row2.Edit(base)
		path := []row2.Step{{Key: 2}, {Key: 5}}
		edit.Set(path, values.Int(-129), row2.Signed)
		first := edit.Snapshot()
		edit.Set(path, values.Int(127), row2.Signed)
		edit.Set([]row2.Step{{Key: 3, Options: row2.Present}, {Key: 7}}, values.Uint(0), row2.Word)
		if edit.Snapshot().Has(3) {
			t.Fatal("deleting absent leaf created child")
		}
		present := []row2.Step{{Key: 3, Options: row2.Present | row2.Compact}, {Key: 7, Options: row2.Present}}
		edit.Set(present, values.Uint(0), row2.Word)
		child := edit.Snapshot().Child(present[0])
		if child.Missing() || !child.Has(7) || child.Layout() == row2.Fast2 {
			t.Fatal("explicit zero or child policy lost")
		}
		present[1].Options &^= row2.Present
		edit.Set(present, values.Uint(0), row2.Word)
		if c := edit.Snapshot().Child(present[0]); c.Missing() || c.Len() != 0 {
			t.Fatal("present empty child lost")
		}
		edit.Set(path, values.Uint(0), row2.Word)
		v := edit.Snapshot()
		if v.Has(2) || !v.Has(0) || !bytes.Equal(v.Get(1, row2.Data).AsBytes(), []byte{0xff, 0xff, 0xff}) {
			t.Fatal("wrong deletion or unknown fields changed")
		}
		if !bytes.Equal(saved, base.Raw()) || first.Child(path[0]).Get(5, row2.Signed).AsInt() != -129 {
			t.Fatal("changed borrowed or frozen bytes")
		}
		if _, err := row2.TryOpen(v.Raw()); err != nil {
			t.Fatal(err)
		}
	}
}

func FuzzOpen(f *testing.F) {
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		f.Add(row2.Empty(layout).Raw())
		var w row2.Writer
		w.Begin(0, "", row2.Object, layout)
		w.Put(1, "", values.Uint(128), row2.Word)
		w.Put(2, "", values.Bytes([]byte("abc")), row2.Data)
		w.End()
		f.Add(w.BytesWritten())
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		v, err := row2.TryOpen(raw)
		if err != nil {
			return
		}
		it := v.Iter()
		for i := 0; i < v.Len(); i++ {
			k, s, field, ok := it.Next()
			if !ok {
				t.Fatal("short iterator")
			}
			// Framing validation does not declare the semantic type of a field.
			// Valid byte spans longer than eight bytes are not Word values.
			// Compare raw lookups with traversal without inventing a schema.
			var lookup row2.Field
			switch v.Kind() {
			case row2.Object:
				lookup = v.RawField(k)
			case row2.Dictionary:
				lookup = v.RawString(string(s))
			case row2.List:
				lookup = v.RawAt(i)
			}
			if lookup != field {
				t.Fatal("lookup disagrees with iteration")
			}
		}
		if _, _, _, ok := it.Next(); ok {
			t.Fatal("long iterator")
		}
	})
}
