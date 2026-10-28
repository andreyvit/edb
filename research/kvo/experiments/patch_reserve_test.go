package kvolab

// Batch count is known, so reserve references after the first eligible leaf.
// Immediate rejection allocates nothing. A later rejection can retain unused
// capacity; measure that trade-off as well as successful cold and reused plans.
import (
	"bytes"
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"slices"
	"testing"
)

func patchPlanReserved(base []byte, edits []perfLeaf, dst []patchRef) ([]patchRef, bool) {
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
			if len(dst) == 0 {
				dst = slices.Grow(dst, len(edits))
			}
			dst = append(dst, patchRef{offset + at, size, v})
		}
	}
	return dst, true
}

// Eight bytes per resolved leaf, no GC pointers. The corresponding value is
// already in edits at the same ordinal; preserving input order avoids an index.
type patchOffset struct{ at, size uint32 }

func patchPlanOffsets(base []byte, edits []perfLeaf, dst []patchOffset) ([]patchOffset, bool) {
	if len(base) > tagMaxBytes {
		panic(draft.ErrLimit)
	}
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
			if len(dst) == 0 {
				dst = slices.Grow(dst, len(edits))
			}
			dst = append(dst, patchOffset{uint32(offset + at), uint32(size)})
		}
	}
	return dst, true
}

func patchOffsetsApplyOwned(dst []byte, offsets []patchOffset, edits []perfLeaf) {
	if len(offsets) != len(edits) {
		panic(draft.ErrValue)
	}
	for i, ref := range offsets {
		value := edits[i].value
		if value.ptr != nil {
			copy(dst[int(ref.at):int(ref.at)+int(ref.size)], value.Bytes())
		} else {
			binary.LittleEndian.PutUint64(dst[int(ref.at):], value.num)
		}
	}
}

func TestPatchOffsets(t *testing.T) {
	for name, source := range workloadFixtures(t) {
		var old lengthWriter
		back := lengthEncode(&old, nil, source, func(int, Value) bool { return true })
		var w keysLastWriter
		raw := keysLastEncode(&w, nil, source, func(int, Value) bool { return true })
		edits := perfCollect(source, back)
		if len(edits) > 0 {
			edits = rowEdits(perfRow{leaves: edits}, "wide70")
		}
		refs, accepted := patchPlanFused(raw, edits, nil)
		offsets, ok := patchPlanOffsets(raw, edits, nil)
		if ok != accepted || len(refs) != len(offsets) {
			t.Fatal(name, "eligibility")
		}
		for i, ref := range refs {
			if int(offsets[i].at) != ref.at || int(offsets[i].size) != ref.size {
				t.Fatal(name, "offset")
			}
		}
		if ok {
			a, b := bytes.Clone(raw), bytes.Clone(raw)
			patchApplyOwned(a, refs)
			patchOffsetsApplyOwned(b, offsets, edits)
			if !bytes.Equal(a, b) {
				t.Fatal(name, "output")
			}
		}
	}
}

func BenchmarkPatchReserve(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		var writer keysLastWriter
		raw := keysLastEncode(&writer, nil, r.source, func(int, Value) bool { return true })
		d := newMmapDataset(b, raw, 1024)
		warmMmapPayloads(d)
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70", "failed-late"} {
			p := pattern
			if p == "failed-late" {
				p = "wide70"
			}
			edits := rowEdits(r, p)
			if pattern == "failed-late" {
				edits = append(edits, perfLeaf{path: []perfStep{{key: ^uint64(0)}}, value: lengthWord(1)})
			}
			for _, mode := range []string{"append", "reserve", "offsets"} {
				for _, cold := range []bool{false, true} {
					label := "reuse"
					if cold {
						label = "cold"
					}
					b.Run(name+"/"+pattern+"/"+mode+"/"+label, func(b *testing.B) {
						var refs []patchRef
						var offsets []patchOffset
						var out []byte
						var w keysLastWriter
						var e keysLastTreeEditor
						rng := uint64(0x12345678fedabcde)
						run := func() {
							rng ^= rng << 13
							rng ^= rng >> 7
							rng ^= rng << 17
							base := d.rows[int(rng>>16)&1023]
							if cold {
								refs = nil
								offsets = nil
							}
							var ok bool
							if mode == "offsets" {
								offsets, ok = patchPlanOffsets(base, edits, offsets)
							} else if mode == "reserve" {
								refs, ok = patchPlanReserved(base, edits, refs)
							} else {
								refs, ok = patchPlanFused(base, edits, refs)
							}
							if ok {
								out = append(out[:0], base...)
								if mode == "offsets" {
									patchOffsetsApplyOwned(out, offsets, edits)
								} else {
									patchApplyOwned(out, refs)
								}
							} else {
								e.Reset(keysLastTrusted(base), "append", true)
								for _, m := range edits {
									e.Set(m.path, m.value)
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
}
