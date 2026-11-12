package row2_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Captured from currentWriter in the independent KVO research implementation.
// The model declares field 1 presence-significant, field 2 signed, and field
// 256 a Compact object with a signed child.
func TestSelectedWireFormats(t *testing.T) {
	for _, tc := range []struct {
		name, wire string
		layout     row2.Layout
	}{
		{"Fast2", "8000010e821800000000000000007fffffffffffffff0000000006000000040100020000010300000051", row2.Fast2},
		{"Compact2", "7fff8000010e82180302000282fc038640", row2.Compact2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := hex.DecodeString(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			row, err := row2.TryOpen(raw)
			if err != nil {
				t.Fatal(err)
			}
			child := row.Child(row2.Step{Key: 256, Options: row2.Compact})
			if row.Layout() != tc.layout || !row.Has(1) || !(row.Get(1, row2.Word).R == 0) || row.Get(2, row2.Signed).AsInt() != -129 || child.Layout() == row2.Fast2 || child.Get(7, row2.Signed).AsInt() != 128 {
				t.Fatal("field values or layout changed")
			}
			var w row2.Writer
			w.Begin(0, "", row2.Object, tc.layout)
			w.PutPresent(1, "", values.Uint(0), row2.Word)
			w.Put(2, "", values.Int(-129), row2.Signed)
			w.Begin(256, "", row2.Object, row2.Compact2)
			w.Put(7, "", values.Int(128), row2.Signed)
			w.End()
			w.End()
			if out := w.BytesWritten(); !bytes.Equal(out, raw) {
				t.Fatalf("wire encoding changed: %x", out)
			}
		})
	}
}

func TestInvalidLayout(t *testing.T) {
	for _, layout := range []row2.Layout{2, 255} {
		for name, build := range map[string]func(){
			"empty": func() { row2.Empty(layout) },
			"root": func() {
				var w row2.Writer
				w.Begin(0, "", row2.Object, layout)
			},
			"compact child": func() {
				var w row2.Writer
				w.Begin(0, "", row2.Object, row2.Compact2)
				w.Begin(1, "", row2.Object, layout)
			},
		} {
			t.Run(layout.String()+"/"+name, func(t *testing.T) {
				defer func() {
					if got := recover(); got != row2.ErrLayout {
						t.Fatalf("got panic %v, want %v", got, row2.ErrLayout)
					}
				}()
				build()
			})
		}
	}
}

func TestByteKeyDirectory(t *testing.T) {
	for n := 1; n <= 20; n++ {
		for _, base := range []int{0, 1, 64, 128, 216} {
			var w row2.Writer
			w.Begin(0, "", row2.Object, row2.Fast2)
			for i := range n {
				w.Put(uint64(base+2*i), "", values.Uint(uint64(1+i)), row2.Word)
			}
			w.End()
			v := row2.Trusted(w.BytesWritten())
			for k := 0; k <= 256; k++ {
				var want uint64
				if k >= base && (k-base)%2 == 0 && (k-base)/2 < n {
					want = uint64(1 + (k-base)/2)
				}
				if got := v.Get(uint64(k), row2.Word).AsUint(); got != want {
					t.Fatalf("length %d, base %d, key %d: got %d want %d", n, base, k, got, want)
				}
			}
		}
	}
}
