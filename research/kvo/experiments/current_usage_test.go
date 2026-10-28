package kvolab

import (
	"bytes"
	"maps"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

// Portable counterpart of the native key kernel. Keep the architecture branch
// outside timed native lookups so other targets use explicit little-endian reads.
func keysLastSafeFind(h *lengthIndexHeader, key uint64) int {
	lo, hi := 0, h.n
	for lo < hi {
		m := lo + (hi-lo)/2
		k := h.number(m)
		if k == key {
			return m
		}
		if k < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return -1
}

func keysLastPortableGet(v lengthView, key uint64) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if v.Kind() != 0 {
		panic(draft.ErrKind)
	}
	if !v.Indexed() {
		return v.Get(key)
	}
	h := keysLastIndex(v.Raw())
	return h.value(keysLastSafeFind(&h, key))
}

func TestCurrentUsage(t *testing.T) {
	if unsafe.Sizeof(keysLastView{}) != 16 || unsafe.Sizeof(lengthValue{}) != 16 {
		t.Fatal("two-word views and values required on this target")
	}
	var w keysLastWriter
	w.Begin(0, "", 0, true)
	w.ReserveEntries(2)
	w.Put(1, "", lengthWord(7))
	w.Begin(2, "", 1, false)
	w.Put(0, "region", lengthBytes([]byte("west")))
	w.End()
	w.End()
	encoded := w.BytesWritten()
	view, err := keysLastOpen(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if view.Get(1).Uint() != 7 || string(keysLastTrusted(view.Get(2).Bytes()).GetString("region").Bytes()) != "west" {
		t.Fatal("read")
	}
	var scratch [8]classMeta
	var arena [256]byte
	editor := newKeysLastLengthEditor(view, scratch[:0], arena[:0])
	editor.Set(1, lengthWord(editor.Get(1, "").Uint()+1))
	updated := editor.Append(&w, nil, true)
	if keysLastTrusted(updated).Get(1).Uint() != 8 || !bytes.Equal(view.Get(2).Bytes(), keysLastTrusted(updated).Get(2).Bytes()) {
		t.Fatal("edit or opaque child preservation")
	}
	if view.Get(1).Uint() != 7 {
		t.Fatal("borrowed base modified")
	}
}

func TestCurrentFastExample(t *testing.T) {
	var w keysLastWriter
	w.Begin(0, "", 0, true)
	w.Put(1, "", lengthWord(7))
	w.Put(2, "", lengthBytes([]byte("hi")))
	w.End()
	want := []byte{
		0x68, 0x69,
		7, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 2, 0, 0, 0,
		2, 1, 2,
		2, 0, 0, 0, 0x11,
	}
	if !bytes.Equal(w.BytesWritten(), want) {
		t.Fatal("README Fast example")
	}
}

func TestKeysLastPortable(t *testing.T) {
	all := maps.Clone(workloadFixtures(t))
	all["wide-keys"] = mmapFixture("global128")
	all["extreme-keys"] = Obj(Field{0, U(1)}, Field{^uint64(0), U(2)})
	for _, source := range all {
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		var walk func(keysLastView)
		walk = func(v keysLastView) {
			it := v.Iter()
			for {
				key, _, value, ok := it.Next()
				if !ok {
					break
				}
				if v.Kind() == 0 && keysLastPortableGet(v.lengthView, key) != value {
					t.Fatal("portable lookup")
				}
			}
		}
		walk(keysLastTrusted(raw))
		for _, key := range []uint64{0, 1, 127, 255, 256, 1 << 63, ^uint64(0)} {
			if keysLastPortableGet(lengthTrusted(raw), key) != keysLastTrusted(raw).Get(key) {
				t.Fatal("absent/wide target")
			}
		}
	}
}

func TestCurrentLeafBatch(t *testing.T) {
	var w keysLastWriter
	w.Begin(0, "", 0, true)
	w.Put(1, "", lengthWord(7))
	w.Put(2, "", lengthBytes([]byte("x")))
	w.End()
	raw := bytes.Clone(w.BytesWritten())
	view := keysLastTrusted(raw)
	changes := []perfLeaf{{path: []perfStep{{key: 1}}, value: lengthWord(9)}}
	var local [8]patchOffset
	plan, ok := patchPlanOffsets(view.Raw(), changes, local[:0])
	if !ok {
		t.Fatal("patch")
	}
	updated := append([]byte(nil), view.Raw()...)
	patchOffsetsApplyOwned(updated, plan, changes)
	if keysLastTrusted(updated).Get(1).Uint() != 9 || view.Get(1).Uint() != 7 {
		t.Fatal("ownership")
	}
	changes = append(changes, perfLeaf{path: []perfStep{{key: 2}}, value: lengthBytes([]byte("longer"))})
	if _, ok = patchPlanOffsets(view.Raw(), changes, local[:0]); ok {
		t.Fatal("resize accepted")
	}
	var merge leafMerger
	updated = merge.Append(&w, updated[:0], view, changes, nil, true)
	if keysLastTrusted(updated).Get(1).Uint() != 9 || string(keysLastTrusted(updated).Get(2).Bytes()) != "longer" {
		t.Fatal("merge")
	}
	if view.Get(1).Uint() != 7 || string(view.Get(2).Bytes()) != "x" {
		t.Fatal("borrowed base changed")
	}
}

func TestPatchLocalScratch(t *testing.T) {
	r := rowFixtures(t)["row-small"]
	var w keysLastWriter
	raw := keysLastEncode(&w, nil, r.source, func(int, Value) bool { return true })
	edits := rowEdits(r, "few-shallow")
	out := make([]byte, len(raw))
	allocations := testing.AllocsPerRun(100, func() {
		var local [8]patchOffset
		refs, ok := patchPlanOffsets(raw, edits, local[:0])
		if !ok {
			panic("fixture must patch")
		}
		copy(out, raw)
		patchOffsetsApplyOwned(out, refs, edits)
	})
	if allocations != 0 {
		t.Fatalf("local patch scratch allocated %g times", allocations)
	}
}
