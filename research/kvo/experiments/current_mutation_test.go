package kvolab

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func TestCurrentSignedEdits(t *testing.T) {
	source := Obj(Field{1, I(-129)}, Field{2, I(128)},
		Field{3, Map(Entry{"a", Obj(Field{1, I(-1)}, Field{2, S("untouched")})}, Entry{"b", Obj(Field{1, I(-128)})})},
		Field{4, Array(I(0), I(-128), I(32767))}, Field{5, S("opaque")})
	edits := []currentLeaf{
		{[]perfStep{{key: 1}}, currentInt(128)},
		{[]perfStep{{key: 3}, {kind: 1, text: "a"}, {key: 1}}, currentInt(-129)},
		{[]perfStep{{key: 3}, {kind: 1, text: "b"}, {key: 1}}, currentInt(255)},
		{[]perfStep{{key: 4}, {kind: 2, key: 1}}, currentInt(math.MinInt64)},
	}
	for layout, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
		var w currentWriter
		raw := bytes.Clone(currentEncode(&w, nil, source, policy))
		original := bytes.Clone(raw)
		for _, mode := range []string{"sorted", "tail", "append", "upsert", "lazy", "hash"} {
			t.Run(fmt.Sprintf("layout%d/%s", layout, mode), func(t *testing.T) {
				var tree currentTreeEditor
				tree.Reset(currentTrusted(raw), mode, layout != 0)
				for _, edit := range edits {
					tree.Set(edit.path, edit.value)
					if got := tree.GetLeaf(edit.path).Int(); got != edit.value.Int() {
						t.Fatalf("overlay: %d != %d", got, edit.value.Int())
					}
				}
				out := tree.Append(&w, nil)
				if _, err := currentOpen(out); err != nil {
					t.Fatal(err)
				}
				for _, edit := range edits {
					if got := currentPath(out, edit.path).Int(); got != edit.value.Int() {
						t.Fatalf("rewrite: %d != %d", got, edit.value.Int())
					}
				}
				view := currentTrusted(out)
				if view.Get(2).Int() != 128 || string(view.Get(5).Bytes()) != "opaque" {
					t.Fatal("untouched root")
				}
				child := currentTrusted(currentTrusted(view.Get(3).Bytes()).GetString("a").Bytes())
				if string(child.Get(2).Bytes()) != "untouched" {
					t.Fatal("untouched child")
				}
				if !bytes.Equal(raw, original) {
					t.Fatal("borrowed row mutated")
				}
			})
		}
		var merger currentLeafMerger
		out := merger.Append(&w, nil, currentTrusted(raw), edits, nil, layout != 0)
		for _, edit := range edits {
			if currentPath(out, edit.path).Int() != edit.value.Int() {
				t.Fatal("merged signed value")
			}
		}
		var scratch [8]patchOffset
		plan, ok := currentPatchPlan(raw, edits, scratch[:0])
		if ok != (layout == 1) {
			t.Fatal("patch eligibility")
		}
		if ok {
			owned := bytes.Clone(raw)
			currentPatchApplyOwned(owned, plan, edits)
			for _, edit := range edits {
				if currentPath(owned, edit.path).Int() != edit.value.Int() {
					t.Fatal("patched signed value")
				}
			}
		}
	}
}

func TestCurrentPresenceAndUnknownWidths(t *testing.T) {
	for _, fast := range []bool{false, true} {
		var w currentWriter
		w.Begin(0, "", 0, fast)
		w.PutPresent(1, "", currentInt(0))
		// Deliberately non-minimal, but valid, unknown signed representation.
		w.Put(2, "", currentBytes([]byte{0xff, 0xff, 0xff}))
		w.Put(3, "", currentInt(128))
		w.End()
		raw := bytes.Clone(w.BytesWritten())
		var tree currentTreeEditor
		tree.Reset(currentTrusted(raw), "append", fast)
		tree.SetPresent([]perfStep{{key: 4}}, currentInt(0))
		tree.Set([]perfStep{{key: 3}}, currentInt(0))
		out := tree.Append(&w, nil)
		v := currentTrusted(out)
		if v.Len() != 3 || v.Get(2).Int() != -1 || !bytes.Equal(v.Get(2).Bytes(), []byte{0xff, 0xff, 0xff}) {
			t.Fatal("unknown width or presence")
		}
		it := v.Iter()
		for _, want := range []uint64{1, 2, 4} {
			key, _, _, ok := it.Next()
			if !ok || key != want {
				t.Fatal("presence keys")
			}
		}
		var merger currentLeafMerger
		edits := []currentLeaf{{[]perfStep{{key: 3}}, currentInt(0)}, {[]perfStep{{key: 4}}, currentInt(0)}}
		merged := merger.Append(&w, nil, currentTrusted(raw), edits, []bool{false, true}, fast)
		if !bytes.Equal(out, merged) {
			t.Fatal("presence merge differs")
		}
		// The simple editor must also retain unknown zeros during a rewrite.
		e := newCurrentLengthEditor(currentTrusted(raw), nil, nil)
		e.Set(3, currentInt(-128))
		simple := currentTrusted(e.Append(&w, nil, fast))
		if simple.Len() != 3 || simple.Get(3).Int() != -128 {
			t.Fatal("simple signed editor")
		}
	}
}

func TestCurrentSignedPatchStack(t *testing.T) {
	var w currentWriter
	raw := currentEncode(&w, nil, Obj(Field{1, I(-128)}, Field{2, I(128)}), func(int, Value) bool { return true })
	dst := make([]byte, len(raw))
	var edits = [2]currentLeaf{{[]perfStep{{key: 1}}, currentInt(-129)}, {[]perfStep{{key: 2}}, currentInt(255)}}
	if n := testing.AllocsPerRun(100, func() {
		var scratch [8]patchOffset
		plan, ok := currentPatchPlan(raw, edits[:], scratch[:0])
		if !ok {
			panic("patch rejected")
		}
		copy(dst, raw)
		currentPatchApplyOwned(dst, plan, edits[:])
	}); n != 0 {
		t.Fatalf("allocations=%g", n)
	}
	if currentTrusted(dst).Get(1).Int() != -129 || currentTrusted(raw).Get(1).Int() != -128 {
		t.Fatal("owned patch")
	}
}

func TestCurrentRewriteReplay(t *testing.T) {
	for name, source := range workloadFixtures(t) {
		for _, fast := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fast%v", name, fast), func(t *testing.T) {
				var w currentWriter
				raw := bytes.Clone(currentEncode(&w, nil, source, func(int, Value) bool { return fast }))
				var tree currentTreeEditor
				tree.Reset(currentTrusted(raw), "append", fast)
				out := tree.Append(&w, nil)
				checkCurrentTree(t, source, out)
				if !bytes.Equal(raw, out) {
					t.Fatal("schema-free rewrite changed bytes")
				}
			})
		}
	}
}

func FuzzCurrentSigned(f *testing.F) {
	for _, n := range currentSignedBoundaries() {
		f.Add(n, byte(0))
	}
	f.Fuzz(func(t *testing.T, n int64, mode byte) {
		kind, fast := int(mode%3), mode&4 != 0
		var w currentWriter
		w.Begin(0, "", kind, fast)
		w.PutPresent(1, "x", currentInt(n))
		w.End()
		raw := bytes.Clone(w.BytesWritten())
		view, err := currentOpen(raw)
		if err != nil {
			t.Fatal(err)
		}
		it := view.Iter()
		_, _, value, ok := it.Next()
		if !ok || value.Int() != n {
			t.Fatal("signed read")
		}
		w.Reset(nil)
		w.Begin(0, "", kind, false)
		w.PutPresent(1, "x", value)
		w.End()
		copyIter := currentTrusted(w.BytesWritten()).Iter()
		_, _, copied, ok := copyIter.Next()
		if !ok || copied.Int() != n {
			t.Fatal("opaque copy")
		}
	})
}

func FuzzCurrentMalformed(f *testing.F) {
	for _, raw := range [][]byte{nil, {0, 8}, {0xff, 1, 2, 0x81, 0x18}, {1, 3, 0x10}, {1, 2, 0xff, 0, 0x20}} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			t.Skip()
		}
		v, err := currentOpen(raw)
		if err != nil {
			if err != draft.ErrFormat {
				t.Fatal(err)
			}
			return
		}
		it := v.Iter()
		count := 0
		for {
			_, _, _, ok := it.Next()
			if !ok {
				break
			}
			count++
		}
		if count != v.Len() {
			t.Fatal("validated count mismatch")
		}
	})
}
