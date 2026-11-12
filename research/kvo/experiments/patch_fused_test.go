package kvolab

import (
	"bytes"
	"encoding/binary"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

// Same trusted, resolved-leaf contract as keysLastPatchLocate. Avoid the large
// returned parsed header and share the selected read path's search strategy.
func patchLocateFused(raw []byte, s perfStep) (at, size int, span, found bool) {
	if len(raw) == 0 || raw[len(raw)-1]&1 == 0 {
		return
	}
	if !perfNativeWords {
		return keysLastPatchLocatePrevious(raw, s)
	}
	t := raw[len(raw)-1]
	if int(t>>1&3) != s.kind {
		return
	}
	end := len(raw) - 5
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	if n == 0 {
		return
	}
	width, mode := 1<<uint(t>>6), t>>3&3
	bitmap := 0
	if mode == 2 {
		bitmap = (n + 7) / 8
	}
	i := -1
	if s.kind == 0 {
		keys := end - n*width
		codes := keys - bitmap
		if width == 1 {
			if s.key <= 255 {
				if n <= 4 {
					for j, k := range raw[keys:end] {
						if k == byte(s.key) {
							i = j
							break
						}
					}
				} else {
					i = bytes.IndexByte(raw[keys:end], byte(s.key))
				}
			}
		} else {
			i = perfFind(raw[keys:end], width, s.key, true)
		}
		if i < 0 {
			return
		}
		span = mode == 1 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) != 0
		at, size = codes-n*8+i*8, 8
		if span {
			word := binary.LittleEndian.Uint64(raw[at:])
			at, size = int(uint32(word)), int(word>>32)
		}
		return at, size, span, true
	}
	codes := end - bitmap
	keys := codes - n*width
	valueStart := 0
	if s.kind == 2 {
		if s.key < uint64(n) {
			i = int(s.key)
		}
	} else {
		p := unsafe.Pointer(unsafe.SliceData(raw[keys:codes]))
		switch width {
		case 1:
			i, valueStart = dictLocate(raw, unsafe.Slice((*uint8)(p), n), s.text)
		case 2:
			i, valueStart = dictLocate(raw, unsafe.Slice((*uint16)(p), n), s.text)
		case 4:
			i, valueStart = dictLocate(raw, unsafe.Slice((*uint32)(p), n), s.text)
		case 8:
			i, valueStart = dictLocate(raw, unsafe.Slice((*uint64)(p), n), s.text)
		}
	}
	if i < 0 {
		return
	}
	span = mode == 1 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) != 0
	at = valueStart
	if s.kind == 2 {
		if mode == 0 {
			return i * 8, 8, false, true
		}
		at = int(getWidth(raw[keys+i*width:], width))
	}
	if !span {
		return at, 8, false, true
	}
	finish := keys
	if i+1 < n {
		finish = int(getWidth(raw[keys+(i+1)*width:], width))
	}
	return at, finish - at, true, true
}

func patchPlanFused(base []byte, edits []perfLeaf, dst []patchRef) ([]patchRef, bool) {
	dst = dst[:0]
	for _, edit := range edits {
		if len(edit.path) == 0 {
			panic(draft.ErrKind)
		}
		raw := base
		offset := 0
		for i, s := range edit.path {
			at, size, span, found := patchLocateFused(raw, s)
			if !found {
				return dst[:0], false
			}
			if i+1 < len(edit.path) {
				if !span {
					return dst[:0], false
				}
				raw = raw[at : at+size]
				offset += at
				continue
			}
			v := edit.value
			if s.kind != 2 && v.IsZero() {
				return dst[:0], false
			}
			if span != (v.ptr != nil) || span && int(v.num) != size {
				return dst[:0], false
			}
			dst = append(dst, patchRef{offset + at, size, v})
		}
	}
	return dst, true
}

type patchPrefix struct {
	step   perfStep
	raw    []byte
	offset int
}

// prefix only caches containers within this call. It is not a retained physical
// patch plan and does not cross a row/revision boundary. Eight levels use local
// scratch; deeper paths grow normally. All edits still resolve against base.
func patchPlanCached(base []byte, edits []perfLeaf, dst []patchRef) ([]patchRef, bool) {
	dst = dst[:0]
	var local [8]patchPrefix
	frames := local[:0]
	for _, edit := range edits {
		if len(edit.path) == 0 {
			panic(draft.ErrKind)
		}
		raw, offset, start := base, 0, 0
		for start < len(frames) && start+1 < len(edit.path) && frames[start].step == edit.path[start] {
			raw, offset = frames[start].raw, frames[start].offset
			start++
		}
		frames = frames[:start]
		for i := start; i < len(edit.path); i++ {
			s := edit.path[i]
			at, size, span, found := patchLocateFused(raw, s)
			if !found {
				return dst[:0], false
			}
			if i+1 < len(edit.path) {
				if !span {
					return dst[:0], false
				}
				raw = raw[at : at+size]
				offset += at
				frames = append(frames, patchPrefix{s, raw, offset})
				continue
			}
			v := edit.value
			if s.kind != 2 && v.IsZero() {
				return dst[:0], false
			}
			if span != (v.ptr != nil) || span && int(v.num) != size {
				return dst[:0], false
			}
			dst = append(dst, patchRef{offset + at, size, v})
		}
	}
	return dst, true
}

func TestPatchFused(t *testing.T) {
	for name, row := range rowFixtures(t) {
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, row.source, func(int, Value) bool { return true })
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70"} {
			edits := rowEdits(row, pattern)
			want, accepted := keysLastPatchPlanPrevious(raw, edits, nil)
			for _, plan := range []func([]byte, []perfLeaf, []patchRef) ([]patchRef, bool){patchPlanFused, patchPlanCached, patchPlanReserved, patchPlanFusedBeforeContainers} {
				got, ok := plan(raw, edits, nil)
				if ok != accepted || len(got) != len(want) {
					t.Fatal(name, pattern, "shuffled plan")
				}
				for i := range got {
					if got[i] != want[i] {
						t.Fatal(name, pattern, "shuffled references")
					}
				}
			}
		}
	}
	for name, source := range workloadFixtures(t) {
		var old lengthWriter
		back := lengthEncode(&old, nil, source, func(int, Value) bool { return true })
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		leaves := perfCollect(source, back)
		for _, leaf := range leaves {
			child := raw
			for i, step := range leaf.path {
				a, n, span, ok := keysLastPatchLocatePrevious(child, step)
				x, z, xs, xo := patchLocateFused(child, step)
				if a != x || n != z || span != xs || ok != xo {
					t.Fatal(name, "locator mismatch")
				}
				if i+1 < len(leaf.path) {
					child = child[a : a+n]
				}
			}
		}
		want, ok := keysLastPatchPlanPrevious(raw, leaves, nil)
		for _, plan := range []func([]byte, []perfLeaf, []patchRef) ([]patchRef, bool){patchPlanFused, patchPlanCached, patchPlanReserved, patchPlanFusedBeforeContainers} {
			got, accepted := plan(raw, leaves, nil)
			if accepted != ok || len(got) != len(want) {
				t.Fatal(name, "eligibility")
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatal(name, "patch differs")
				}
			}
		}
	}
	// More than eight parents exercises dynamic scratch growth, then a miss
	// exercises all-or-nothing rejection after an already-resolved valid patch.
	var w keysLastWriter
	for depth := 0; depth < 12; depth++ {
		w.Begin(1, "", 0, true)
	}
	w.Put(1, "", lengthWord(4))
	w.Put(2, "", lengthWord(5))
	for depth := 0; depth < 12; depth++ {
		w.End()
	}
	path := make([]perfStep, 12)
	for i := range path {
		path[i].key = 1
	}
	edits := []perfLeaf{{path: path, value: lengthWord(7)}}
	for _, plan := range []func([]byte, []perfLeaf, []patchRef) ([]patchRef, bool){patchPlanFused, patchPlanCached, patchPlanReserved, patchPlanFusedBeforeContainers} {
		refs, ok := plan(w.BytesWritten(), edits, nil)
		if !ok || len(refs) != 1 {
			t.Fatal("deep scratch")
		}
		bad := append(append([]perfStep(nil), path[:11]...), perfStep{key: 999})
		if refs, ok := plan(w.BytesWritten(), append(edits, perfLeaf{path: bad, value: lengthWord(8)}), refs); ok || len(refs) != 0 {
			t.Fatal("partial plan accepted")
		}
	}
}

func BenchmarkPatchLookup(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		var writer keysLastWriter
		raw := keysLastEncode(&writer, nil, r.source, func(int, Value) bool { return true })
		d := newMmapDataset(b, raw, 1024)
		warmMmapPayloads(d)
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70"} {
			edits := rowEdits(r, pattern)
			for _, mode := range []string{"previous", "fused-before-containers", "fused", "prefix"} {
				b.Run(name+"/"+pattern+"/"+mode, func(b *testing.B) {
					var w keysLastWriter
					var e keysLastTreeEditor
					var refs []patchRef
					var out []byte
					rng := uint64(0x12345678fedabcde)
					run := func() {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						base := d.rows[int(rng>>16)&1023]
						var ok bool
						if mode == "previous" {
							refs, ok = keysLastPatchPlanPrevious(base, edits, refs)
						} else if mode == "fused-before-containers" {
							refs, ok = patchPlanFusedBeforeContainers(base, edits, refs)
						} else if mode == "prefix" {
							refs, ok = patchPlanCached(base, edits, refs)
						} else {
							refs, ok = patchPlanFused(base, edits, refs)
						}
						if ok {
							out = append(out[:0], base...)
							patchApplyOwned(out, refs)
						} else {
							e.Reset(keysLastTrusted(base), "append", true)
							for _, edit := range edits {
								e.Set(edit.path, edit.value)
							}
							out = e.Append(&w, out[:0])
						}
					}
					run()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run()
					}
					b.StopTimer()
					sinkBytes = out
				})
			}
		}
	}
}
