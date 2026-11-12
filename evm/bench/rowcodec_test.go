package bench_test

import (
	"testing"

	"github.com/andreyvit/edb/kvo/codec/fast1"
	"github.com/andreyvit/edb/kvo/codec/row2"
)

// Read the same fields as the guest, outside EVM and the database. Report
// stored value bytes only; keys, pages and transaction overhead are excluded.
func BenchmarkCustomerRowCodec(b *testing.B) {
	want := recordAnswer("ReadCustomers", 0)
	b.Run("WordMap", func(b *testing.B) {
		_, model := customerModel()
		var rows []fast1.ImmutableMap
		var size int
		for id := uint64(1); id <= customerCount; id++ {
			r := fast1.NewRecord(model.Type())
			r.Set(idKey, id)
			if id%5 != 0 {
				r.Set(activeKey, 1)
			}
			p := r.UpdateMap(profileKey)
			p.Set(balanceKey, 100+id%37)
			p.Set(rateKey, 1+id%9)
			row := r.Record().PackedRoot()
			size += len(row.RecordData().Bytes())
			rows = append(rows, row)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			var sum uint64
			for _, r := range rows {
				if r.Get(activeKey) != 0 {
					balance, _ := r.GetChildWord(profileKey, balanceKey)
					rate, _ := r.GetChildWord(profileKey, rateKey)
					sum += balance*rate + r.Get(idKey)
				}
			}
			if sum != want {
				b.Fatal(sum)
			}
		}
		b.ReportMetric(float64(size)/customerCount, "bytes/row")
	})
	for _, layout := range []struct {
		name  string
		value row2.Layout
	}{{"Fast", row2.Fast2}, {"Compact", row2.Compact2}} {
		b.Run(layout.name, func(b *testing.B) {
			var rows []row2.View
			var size int
			for id := uint64(1); id <= customerCount; id++ {
				r := customerRow(id, layout.value)
				size += len(r.Raw())
				rows = append(rows, r)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var sum uint64
				for _, r := range rows {
					if r.Get(activeKey, row2.Word).AsUint() != 0 {
						balance := r.Child(row2.Step{Key: profileKey}).Get(balanceKey, row2.Word).AsUint()
						rate := r.Child(row2.Step{Key: profileKey}).Get(rateKey, row2.Word).AsUint()
						sum += balance*rate + r.Get(idKey, row2.Word).AsUint()
					}
				}
				if sum != want {
					b.Fatal(sum)
				}
			}
			b.ReportMetric(float64(size)/customerCount, "bytes/row")
		})
	}
}
