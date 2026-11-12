package kvo_test

import (
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func BenchmarkTypedProperty(b *testing.B) {
	schema, _, fixtures := recordHandleFixtures(b)
	earned := kvo.BindProp(schema, 19, values.SignedCodec[int64]())
	spent := kvo.BindProp(schema, 20, values.SignedCodec[int64]())
	earnedCode := kvo.BindSignedProp[int64](schema, 19)
	spentCode := kvo.BindSignedProp[int64](schema, 20)
	earnedRaw, spentRaw := earned.Code(), spent.Code()
	for _, f := range fixtures {
		b.Run(f.name, func(b *testing.B) {
			b.Run("Value", func(b *testing.B) {
				var sum int64
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					r := f.rows[i%handleRows]
					sum += r.GetValue(earnedRaw).AsInt() - r.GetValue(spentRaw).AsInt()
				}
				accessSum = uint64(sum)
			})
			b.Run("Descriptor", func(b *testing.B) {
				var sum int64
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					r := f.rows[i%handleRows]
					sum += r.Get(earned) - r.Get(spent)
				}
				accessSum = uint64(sum)
			})
			b.Run("TypedCode", func(b *testing.B) {
				var sum int64
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					r := f.rows[i%handleRows]
					sum += r.Get(earnedCode) - r.Get(spentCode)
				}
				accessSum = uint64(sum)
			})
			b.Run("DirectCode", func(b *testing.B) {
				var sum int64
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					r := f.rows[i%handleRows]
					sum += earnedCode.Get(r) - spentCode.Get(r)
				}
				accessSum = uint64(sum)
			})
		})
	}
}
