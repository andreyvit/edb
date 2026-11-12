package row2_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestRuntimeValueNormalization(t *testing.T) {
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		for _, n := range []int64{math.MinInt64, -65537, -129, -128, -1, 0, 127, 128, 255, 256, math.MaxInt64} {
			var w row2.Writer
			w.Begin(0, "", row2.Object, layout)
			w.PutPresent(1, "", values.Int(n), row2.Signed)
			w.End()
			v := row2.Trusted(w.BytesWritten()).Get(1, row2.Signed)
			if v.P != nil || v.AsInt() != n {
				t.Fatalf("layout=%v: %d became %+v", layout, n, v)
			}
		}
		for _, n := range []float32{0, -1.25, math.SmallestNonzeroFloat32, math.MaxFloat32, float32(math.Inf(1)), float32(math.NaN())} {
			var w row2.Writer
			w.Begin(0, "", row2.Object, layout)
			w.PutPresent(1, "", values.Float32(n), row2.Float32)
			w.End()
			v := row2.Trusted(w.BytesWritten()).Get(1, row2.Float32)
			if v.P != nil || (v.AsFloat32() != n && !math.IsNaN(float64(n))) {
				t.Fatalf("layout=%v: %g became %+v", layout, n, v)
			}
			if !math.IsNaN(float64(n)) && v.R != uint64(math.Float32bits(n)) {
				t.Fatal("wire float64 bits escaped into runtime value")
			}
		}
	}
}

func TestEmptyDataKeepsHistoricalZeroEncoding(t *testing.T) {
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		var encoded [2][]byte
		for i, enc := range []row2.Encoding{row2.Word, row2.Data} {
			var w row2.Writer
			w.Begin(0, "", row2.Object, layout)
			w.PutPresent(1, "", values.Value{}, enc)
			w.End()
			encoded[i] = w.BytesWritten()
		}
		if !bytes.Equal(encoded[0], encoded[1]) {
			t.Fatal("empty data changed its encoding")
		}
	}
}

func TestFloat32RejectsUnrepresentableWireValue(t *testing.T) {
	for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
		for _, n := range []float64{1.0000000001, math.MaxFloat64, math.SmallestNonzeroFloat64} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("accepted %g as float32", n)
					}
				}()
				var w row2.Writer
				w.Begin(0, "", row2.Object, layout)
				w.Put(1, "", values.Float(n), row2.Word)
				w.End()
				row2.Trusted(w.BytesWritten()).Get(1, row2.Float32)
			}()
		}
	}
}
