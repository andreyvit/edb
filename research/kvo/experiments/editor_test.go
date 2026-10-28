package kvolab

import (
	"fmt"
	"testing"
)

func TestEditor(t *testing.T) {
	for _, p := range []Policy{Compact, Indexed, Offsets} {
		base := View(Encode(fixtures()["state"], p))
		var scratch [8]Edit
		e := Editor{Base: base, Edits: scratch[:0]}
		e.Set(5, Cell{Kind: Uint, U: 0})
		e.Set(3, Cell{Kind: Null})
		e.Delete(7)
		e.Set(999, Cell{Kind: True})
		e.Set(999, Cell{Kind: False})
		data := e.AppendCompact(nil)
		for _, k := range []uint64{3, 5, 7, 999} {
			want, exists := e.Get(k)
			got, found := View(data).Get(k)
			if found != exists || got.Kind != want.Kind || got.U != want.U {
				t.Fatalf("key %d", k)
			}
		}
		before, _ := base.Get(57)
		after, _ := View(data).Get(57)
		checkValue(t, fixtures()["profile"], before)
		checkValue(t, fixtures()["profile"], after)
	}
}
func TestEditorNoOp(t *testing.T) {
	base := View(Encode(numeric(8, 0), Compact))
	var edits [4]Edit
	e := Editor{Base: base, Edits: edits[:0]}
	v, _ := base.Get(4)
	e.Set(4, v)
	e.Delete(300)
	if len(e.Edits) != 0 {
		t.Fatal("no-op changes")
	}
}
func BenchmarkEditor(b *testing.B) {
	for _, n := range []int{32, 128, 512} {
		for _, p := range []Policy{Compact, Indexed} {
			base := View(Encode(numeric(n, 0), p))
			for _, pack := range []bool{false, true} {
				b.Run(fmt.Sprintf("n%d/policy%d/pack%t", n, p, pack), func(b *testing.B) {
					dst := make([]byte, 0, len(base)+4096)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						var scratch [8]Edit
						e := Editor{Base: base, Edits: scratch[:0]}
						e.Set(query(i, n), Cell{Kind: Uint, U: uint64(i + 1)})
						if pack {
							dst = e.AppendCompact(dst[:0])
							sinkU += uint64(len(dst))
						} else {
							c, _ := e.Get(query(i, n))
							sinkU += c.U
						}
					}
				})
			}
		}
	}
}
