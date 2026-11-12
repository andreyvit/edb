package kvodraft

import (
	"fmt"
	"testing"
)

var benchBytes []byte
var benchNumber uint64
var benchView View

func BenchmarkDraft(b *testing.B) {
	fields := make([]Field, 128)
	for i := range fields {
		fields[i] = Field{uint64(i + 1), Word(uint64(i*37 + 1))}
	}
	for _, layout := range []Layout{Compact, Indexed} {
		raw := AppendNumeric(nil, fields, layout)
		v := Trusted(raw)
		b.Run(fmt.Sprintf("%d/open", layout), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				benchView, err = Open(raw)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("%d/scan", layout), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				it := v.Iter()
				for it.Next() {
					n := it.Entry().Value.Uint()
					sum += n
				}
			}
			benchNumber = sum
		})
		b.Run(fmt.Sprintf("%d/encode-reuse", layout), func(b *testing.B) {
			dst := make([]byte, 0, len(raw))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = AppendNumeric(dst[:0], fields, layout)
			}
			benchBytes = dst
		})
		b.Run(fmt.Sprintf("%d/edit-four-reuse", layout), func(b *testing.B) {
			dst := make([]byte, 0, len(raw)+128)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var scratch [8]Edit
				e := NewEditor(v, scratch[:], nil)
				for j := uint64(1); j <= 4; j++ {
					e.Set(j*17, Word(uint64(i)))
				}
				dst = e.Append(dst[:0], layout)
			}
			benchBytes = dst
		})
	}
	raw := AppendNumeric(nil, fields, Indexed)
	b.Run("patch-owned", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !TryPatchOwned(raw, uint64(i%128+1), Word(uint64(i+1))) {
				b.Fatal("patch")
			}
		}
		benchBytes = raw
	})
	changes := Changes{Base: 41, Next: 42, Items: []Change{
		{Path: []Step{{Kind: FieldStep, Number: 1}}, Value: Uint(9)},
		{Path: []Step{{Kind: FieldStep, Number: 2}, {Kind: StringStep, Text: "region"}}, Value: ByteString([]byte("west"))},
	}}
	encoded := AppendChanges(nil, changes)
	b.Run("changes-write-reuse", func(b *testing.B) {
		dst := make([]byte, 0, len(encoded))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			dst = AppendChanges(dst[:0], changes)
		}
		benchBytes = dst
	})
	b.Run("changes-read", func(b *testing.B) {
		b.ReportAllocs()
		var n uint64
		for i := 0; i < b.N; i++ {
			var scratch [8]PathPart
			r, err := OpenChanges(encoded, scratch[:])
			if err != nil {
				b.Fatal(err)
			}
			for {
				_, ok, err := r.NextChange()
				if err != nil {
					b.Fatal(err)
				}
				if !ok {
					break
				}
				n++
			}
		}
		benchNumber = n
	})
}
