package kvolab

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Run the library and the selected research writer independently. Exercise the
// width and layout boundaries that a round trip through one writer can miss.
func TestKVRowMatchesSelectedCodec(t *testing.T) {
	for _, rootFast := range []bool{false, true} {
		for _, childFast := range []bool{false, true} {
			for _, lastKey := range []uint64{255, 256, 65536, 1 << 32} {
				t.Run(fmt.Sprintf("%t/%t/key%d", rootFast, childFast, lastKey), func(t *testing.T) {
					var ref currentWriter
					var w row2.Writer
					rootLayout, childLayout := row2.Compact2, row2.Compact2
					if rootFast {
						rootLayout = row2.Fast2
					}
					if childFast {
						childLayout = row2.Fast2
					}
					ref.Begin(0, "", 0, rootFast)
					w.Begin(0, "", row2.Object, rootLayout)
					ref.PutPresent(1, "", currentWord(0))
					w.PutPresent(1, "", values.Uint(0), row2.Word)
					ref.Begin(2, "", 1, childFast)
					w.Begin(2, "", row2.Dictionary, childLayout)
					ref.Put(0, "a", currentInt(-129))
					w.Put(0, "a", values.Int(-129), row2.Signed)
					ref.Put(0, "b", currentInt(128))
					w.Put(0, "b", values.Int(128), row2.Signed)
					ref.End()
					w.End()
					ref.Begin(3, "", 2, childFast)
					w.Begin(3, "", row2.List, childLayout)
					for _, n := range []uint64{0, 127, 128, 255, math.MaxUint64} {
						ref.Put(0, "", currentWord(n))
						w.Put(0, "", values.Uint(n), row2.Word)
					}
					ref.End()
					w.End()
					opaque := bytes.Repeat([]byte{0, 0x80, 0xff}, 85)
					ref.Put(lastKey, "", currentBytes(opaque))
					w.Put(lastKey, "", values.Bytes(opaque), row2.Data)
					ref.End()
					w.End()
					if !bytes.Equal(ref.BytesWritten(), w.BytesWritten()) {
						t.Fatal("wire mismatch")
					}
					old, err := currentOpen(w.BytesWritten())
					if err != nil {
						t.Fatal(err)
					}
					v, err := row2.TryOpen(ref.BytesWritten())
					if err != nil {
						t.Fatal(err)
					}
					dict := row2.Trusted(v.Get(2, row2.Data).AsBytes())
					if v.Layout() != rootLayout || (dict.Layout() == row2.Fast2) != (rootFast && childFast) || !v.Has(1) || !(v.Get(1, row2.Word).R == 0) || dict.GetString("a", row2.Signed).AsInt() != -129 || dict.GetString("b", row2.Signed).AsInt() != 128 || row2.Trusted(v.Get(3, row2.Data).AsBytes()).At(4, row2.Word).AsUint() != math.MaxUint64 || !bytes.Equal(old.Get(lastKey).Bytes(), opaque) {
						t.Fatal("cross-reader mismatch")
					}
				})
			}
		}
	}
}

func TestKVRowEditsMatchSelectedMerger(t *testing.T) {
	src := Obj(Field{1, I(128)}, Field{2, Obj(Field{4, I(-1)}, Field{5, S("opaque")})})
	for _, layout := range []struct{ root, child bool }{{false, false}, {true, false}, {true, true}} {
		var w currentWriter
		base := currentEncode(&w, nil, src, func(depth int, _ Value) bool {
			if depth == 0 {
				return layout.root
			}
			return layout.child
		})
		lib := row2.Edit(row2.Trusted(base))
		changes := []currentLeaf{
			{[]perfStep{{key: 2}, {key: 4}}, currentInt(-129)},
			{[]perfStep{{key: 1}}, currentWord(0)},
			{[]perfStep{{key: 99}}, currentInt(128)},
		}
		lib.Set([]row2.Step{{Key: 2}, {Key: 4}}, values.Int(-129), row2.Signed)
		lib.Set([]row2.Step{{Key: 1}}, values.Uint(0), row2.Word)
		lib.Set([]row2.Step{{Key: 99}}, values.Int(128), row2.Signed)
		var merger currentLeafMerger
		want := merger.Append(&w, nil, currentTrusted(base), changes, nil, layout.root)
		if got := lib.Snapshot().Raw(); !bytes.Equal(got, want) {
			t.Fatalf("layout %+v: got %x, want %x", layout, got, want)
		}
	}
}
