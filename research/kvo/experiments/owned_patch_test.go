package kvolab

// An owned copy may be patched when membership, scalar/span category and span
// extents stay unchanged. Borrowed mmap bytes are never writable. Every edit is
// planned before any write, so a failed preflight leaves the destination intact.
import (
	"bytes"
	"encoding/binary"
	"testing"
	"unsafe"
)

type patchRef struct {
	at, size int
	value    lengthValue
}

func patchLocate(raw []byte, s perfStep) (at, size int, span, found bool) {
	if len(raw) == 0 || raw[len(raw)-1]&1 == 0 {
		return
	}
	h := lengthIndex(raw)
	if h.kind != s.kind {
		return
	}
	i := -1
	switch s.kind {
	case 0:
		i = perfFind(raw[h.keys:h.codes], h.width, s.key, true)
	case 1:
		i = h.findString(s.text)
	case 2:
		if s.key < uint64(h.n) {
			i = int(s.key)
		}
	}
	if i < 0 {
		return
	}
	span = h.mode == 1 || h.mode == 2 && raw[h.codes+i/8]&(1<<uint(i&7)) != 0
	if s.kind == 0 {
		at = h.cells + i*8
		size = 8
		if span {
			word := binary.LittleEndian.Uint64(raw[at:])
			at = int(uint32(word))
			size = int(word >> 32)
		}
	} else {
		at = int(getWidth(raw[h.keys+i*h.width:], h.width))
		end := h.keys
		if i+1 < h.n {
			end = int(getWidth(raw[h.keys+(i+1)*h.width:], h.width))
		}
		if s.kind == 1 {
			n, rest := lengthU(raw[at:])
			at = len(raw) - len(rest) + int(n)
		}
		size = end - at
	}
	found = true
	return
}
func patchPlan(base []byte, edits []perfLeaf, dst []patchRef) ([]patchRef, bool) {
	dst = dst[:0]
	for _, edit := range edits {
		raw := base
		offset := 0
		for i, s := range edit.path {
			at, size, span, found := patchLocate(raw, s)
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
func patchApplyOwned(dst []byte, refs []patchRef) {
	for _, r := range refs {
		if r.value.ptr == nil {
			binary.LittleEndian.PutUint64(dst[r.at:], r.value.num)
		} else {
			copy(dst[r.at:r.at+r.size], r.value.Bytes())
		}
	}
}
func TestOwnedPatch(t *testing.T) {
	for name, r := range rowFixtures(t) {
		original := bytes.Clone(r.raw)
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70"} {
			edits := rowEdits(r, pattern)
			refs, ok := patchPlan(r.raw, edits, nil)
			if !ok {
				t.Log(name, pattern, "needs rebuild")
				continue
			}
			out := bytes.Clone(r.raw)
			patchApplyOwned(out, refs)
			for _, m := range edits {
				if !equalLengthValue(perfPath(lengthTrusted(out), m.path, "common"), m.value) {
					t.Fatal(name, pattern, "patch")
				}
			}
			if !bytes.Equal(original, r.raw) {
				t.Fatal("borrowed source modified")
			}
		}
	}
	var w lengthWriter
	raw := lengthEncode(&w, nil, Obj(Field{1, U(42)}, Field{2, S("abc")}), func(int, Value) bool { return true })
	for _, edit := range []perfLeaf{{path: []perfStep{{kind: 0, key: 1}}, value: lengthWord(0)}, {path: []perfStep{{kind: 0, key: 3}}, value: lengthWord(1)}, {path: []perfStep{{kind: 0, key: 2}}, value: lengthBytes([]byte("longer"))}, {path: []perfStep{{kind: 0, key: 1}}, value: lengthBytes([]byte("12345678"))}} {
		if _, ok := patchPlan(raw, []perfLeaf{edit}, nil); ok {
			t.Fatal("invalid patch accepted")
		}
	}
}
func BenchmarkOwnedMutation(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, pattern := range []string{"few-shallow", "few-deep", "wide70"} {
			edits := rowEdits(r, pattern)
			for _, mode := range []string{"plan", "copy-patch", "owned-patch", "fallback-tree"} {
				b.Run(name+"/"+pattern+"/"+mode, func(b *testing.B) {
					refs, eligible := patchPlan(r.raw, edits, nil)
					if !eligible && mode != "fallback-tree" {
						b.Skip("size/membership change requires rebuild")
					}
					out := bytes.Clone(r.raw)
					var e treeEditor
					var w lengthWriter
					run := func() {
						switch mode {
						case "owned-patch":
							patchApplyOwned(out, refs)
						case "plan":
							refs, _ = patchPlan(r.raw, edits, refs)
						default:
							var ok bool
							refs, ok = patchPlan(r.raw, edits, refs)
							if ok {
								out = append(out[:0], r.raw...)
								patchApplyOwned(out, refs)
							} else {
								e.Reset(lengthTrusted(r.raw), "upsert", true)
								for _, m := range edits {
									e.Set(m.path, m.value)
								}
								out = e.Append(&w, out[:0])
							}
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
					b.ReportMetric(float64(cap(refs)*int(unsafe.Sizeof(patchRef{}))), "plan-B")
					b.ReportMetric(float64(len(r.raw)), "base-B")
					b.ReportMetric(float64(len(edits)), "edits/op")
				})
			}
		}
	}
}
func BenchmarkDenseWriter(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, layout := range []string{"fast", "compact", "onebit"} {
			b.Run(name+"/"+layout, func(b *testing.B) {
				var w lengthWriter
				var ow oneBitWriter
				ow.omitCount = true
				var out []byte
				run := func() {
					if layout == "onebit" {
						out = oneBitEncode(&ow, out[:0], r.source)
					} else {
						out = lengthEncode(&w, out[:0], r.source, func(int, Value) bool { return layout == "fast" })
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
				b.ReportMetric(float64(len(out)), "encoded-B")
			})
		}
	}
}
