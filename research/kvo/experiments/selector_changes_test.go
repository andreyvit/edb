package kvolab

// Keep the logical path format constant while varying only value selectors.
// Changes have no shared selector: the otherwise mixed code can mean Delete.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math/bits"
	"testing"
)

type selectorChangeWriter struct {
	lengthChangeWriter
	mode selectorMode
}

func (w *selectorChangeWriter) Append(dst []byte, c lengthChanges) []byte {
	w.Reset(dst, c.Base, c.Next)
	shift := w.mode.bits()
	for _, x := range c.Items {
		if len(x.Path) > tagMaxEntries {
			panic(draft.ErrLimit)
		}
		common := 0
		for common < len(w.prev) && common < len(x.Path) && sameLengthStep(w.prev[common], x.Path[common]) {
			common++
		}
		wire := byte((bits.Len64(x.Value.num) + 7) / 8)
		word := x.Value.num
		if x.Value.ptr != nil {
			wire = 9
		} else if w.mode == selectorRounded {
			if wire == 3 {
				wire = 4
			} else if wire > 4 {
				wire = 8
			}
		}
		code := w.mode.code(wire)
		if x.Delete {
			switch w.mode {
			case selectorLiteral:
				code = 10
			case selectorRounded:
				code = 6
			default:
				code = 3
			}
		}
		w.meta = binary.AppendUvarint(w.meta, uint64(common))
		w.meta = binary.AppendUvarint(w.meta, uint64(len(x.Path)-common)<<shift|uint64(code))
		for _, s := range x.Path[common:] {
			w.meta = appendLengthStep(w.meta, s)
		}
		if !x.Delete {
			if wire == 9 {
				if word > tagMaxBytes {
					panic(draft.ErrLimit)
				}
				w.out = append(w.out, x.Value.Bytes()...)
			} else if w.mode < selectorTagged || word > 127 {
				w.out = tagAppendWord(w.out, word, int(wire))
			}
			if w.mode >= selectorTagged && code == 0 {
				if wire == 9 {
					word <<= 32
				}
				w.meta = selectorAppendTag(w.meta, classMeta{wire: draft.Wire(wire), word: word}, w.mode)
			} else if w.mode < selectorTagged && wire == 9 {
				w.meta = binary.AppendUvarint(w.meta, word)
			}
		}
		w.count++
		w.prev = x.Path
		if w.count > tagMaxEntries || len(w.meta) > tagMaxBytes || len(w.out) > tagMaxBytes {
			panic(draft.ErrLimit)
		}
	}
	return w.Finish()
}

type selectorChangeReader struct {
	lengthChangeReader
	mode selectorMode
}

func newSelectorChanges(raw []byte, scratch []lengthPathPart, mode selectorMode) selectorChangeReader {
	if mode > selectorTaggedWide {
		panic(errTag)
	}
	return selectorChangeReader{newLengthCatalogChanges(raw, scratch), mode}
}
func (r *selectorChangeReader) NextChange() (out lengthChangeView, ok bool) {
	if r.left == 0 {
		if r.pos != r.metaEnd || r.valuePos != r.valueEnd {
			panic(errTag)
		}
		return
	}
	b := r.data[r.pos:r.metaEnd]
	common, b := tagReadU(b)
	suffix, b := tagReadU(b)
	code := byte(suffix) & r.mode.mixed()
	suffix >>= r.mode.bits()
	if common > uint64(len(r.path)) || suffix > tagMaxEntries-common {
		panic(errTag)
	}
	r.path = r.path[:int(common)]
	for range int(suffix) {
		tag, rest := tagReadU(b)
		b = rest
		p := lengthPathPart{Kind: draft.StepKind(tag & 3), Number: tag >> 2}
		switch p.Kind {
		case draft.NumberStep:
			if tag != 1 {
				panic(errTag)
			}
			p.Number, b = tagReadU(b)
			p.Kind = draft.FieldStep
		case draft.StringStep:
			if p.Number > uint64(len(b)) {
				panic(errTag)
			}
			p.start = uint32(r.metaEnd - len(b))
			p.end = p.start + uint32(p.Number)
			b = b[int(p.Number):]
			p.Number = 0
		}
		if len(r.path) == cap(r.path) {
			grown := make([]lengthPathPart, len(r.path), max(8, len(r.path)*2))
			copy(grown, r.path)
			r.path = grown
		}
		r.path = r.path[:len(r.path)+1]
		r.path[len(r.path)-1] = p
	}
	n := uint64(0)
	immediate := uint64(0)
	span := false
	switch r.mode {
	case selectorLiteral:
		if code > 10 {
			panic(errTag)
		}
		if code == 10 {
			out.Delete = true
		} else if code == 9 {
			span = true
			n, b = tagReadU(b)
		} else {
			n = uint64(code)
		}
	case selectorRounded:
		if code == 7 {
			panic(errTag)
		}
		if code == 6 {
			out.Delete = true
		} else if code == 5 {
			span = true
			n, b = tagReadU(b)
		} else {
			n = uint64([5]byte{0, 1, 2, 4, 8}[code])
		}
	default:
		if code == 3 {
			out.Delete = true
		} else if code == 1 {
			n = 4
		} else if code == 2 {
			n = 8
		} else {
			if len(b) == 0 {
				panic(errTag)
			}
			tag := b[0]
			b = b[1:]
			sentinel := byte(191)
			base := uint64(63)
			if r.mode == selectorTaggedWide {
				sentinel = 249
				base = 121
			}
			if tag < 128 {
				immediate = uint64(tag)
			} else if tag < sentinel {
				span = true
				n = uint64(tag - 128)
			} else if tag == sentinel {
				span = true
				n, b = tagReadU(b)
				if n > tagMaxBytes-base {
					panic(errTag)
				}
				n += base
			} else if r.mode == selectorTaggedWide {
				n = uint64(tag - 249)
				if n >= 4 {
					n++
				}
			} else {
				if tag > 200 {
					panic(errTag)
				}
				n = uint64(tag - 192)
			}
		}
	}
	body := r.data[r.valuePos:r.valueEnd]
	if n > uint64(len(body)) {
		panic(errTag)
	}
	if span {
		out.Value = lengthBytes(body[:int(n)])
	} else if !out.Delete {
		out.Value = lengthWord(tagWord(body[:int(n)]) | immediate)
	}
	r.valuePos += int(n)
	r.pos = r.metaEnd - len(b)
	r.left--
	out.Path = r.path
	return out, true
}
func openSelectorChanges(raw []byte, scratch []lengthPathPart, mode selectorMode) (r selectorChangeReader, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errTag {
				panic(p)
			}
			r = selectorChangeReader{}
			err = draft.ErrFormat
		}
	}()
	r = newSelectorChanges(raw, scratch, mode)
	for {
		if _, ok := r.NextChange(); !ok {
			break
		}
	}
	r.Reset()
	return
}
func selectorChangeFixtures() map[string]lengthChanges {
	out := make(map[string]lengthChanges)
	for _, n := range []int{1, 4, 16, 64} {
		c, _ := lengthChangeFixture(n)
		out[fmt.Sprintf("n%d", n)] = c
	}
	c, _ := lengthChangeFixture(16)
	for i := range c.Items {
		c.Items[i].Value = lengthWord(1<<56 + uint64(i))
		c.Items[i].Delete = false
	}
	out["word8"] = c
	c, _ = lengthChangeFixture(16)
	for i := range c.Items {
		for j := 0; j < 40; j++ {
			c.Items[i].Path = append(c.Items[i].Path, draft.Step{Kind: draft.FieldStep, Number: uint64(j)})
		}
	}
	out["deep"] = c
	return out
}
func TestSelectorChanges(t *testing.T) {
	all := selectorChangeFixtures()
	edge := lengthChanges{Base: 1, Next: 2}
	for _, v := range []uint64{0, 1, 127, 128, 255, 256, 65535, 65536, 1 << 24, 1 << 32, 1 << 40, 1 << 48, 1 << 56, ^uint64(0)} {
		edge.Items = append(edge.Items, lengthChange{Path: []draft.Step{{Kind: draft.FieldStep, Number: ^uint64(0)}, {Kind: draft.IndexStep, Number: 3}}, Value: lengthWord(v)})
	}
	for _, n := range []int{0, 1, 62, 63, 120, 121, 249, 65536} {
		edge.Items = append(edge.Items, lengthChange{Path: []draft.Step{{Kind: draft.StringStep, Text: ""}}, Value: lengthBytes(bytes.Repeat([]byte{254}, n))})
	}
	edge.Items = append(edge.Items, lengthChange{Delete: true, Path: []draft.Step{{Kind: draft.FieldStep, Number: 1}}})
	all["edge"] = edge
	for name, c := range all {
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorChangeWriter{mode: mode}
			raw := w.Append(nil, c)
			if mode == selectorLiteral {
				var old lengthChangeWriter
				if !bytes.Equal(raw, old.Append(nil, c)) {
					t.Fatal("literal bytes", name)
				}
			}
			var scratch [64]lengthPathPart
			r, err := openSelectorChanges(raw, scratch[:], mode)
			if err != nil {
				t.Fatal(name, mode, err)
			}
			for _, want := range c.Items {
				got, ok := r.NextChange()
				if !ok || got.Delete != want.Delete || len(got.Path) != len(want.Path) {
					t.Fatal("item", name, mode)
				}
				for i, p := range got.Path {
					s := want.Path[i]
					if p.Kind != lengthStepKind(s.Kind) || p.Kind == draft.StringStep && string(r.Key(p)) != s.Text || p.Kind != draft.StringStep && p.Number != s.Number {
						t.Fatal("path", name, mode)
					}
				}
				if !got.Delete {
					if want.Value.ptr == nil {
						if got.Value.Uint() != want.Value.Uint() {
							t.Fatal("word", name, mode)
						}
					} else if !bytes.Equal(got.Value.Bytes(), want.Value.Bytes()) {
						t.Fatal("bytes", name, mode)
					}
				}
			}
			if _, ok := r.NextChange(); ok {
				t.Fatal("extra")
			}
		}
	}
}
func FuzzSelectorChanges(f *testing.F) {
	for _, c := range selectorChangeFixtures() {
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorChangeWriter{mode: mode}
			f.Add(uint8(mode), w.Append(nil, c))
		}
	}
	f.Fuzz(func(t *testing.T, m uint8, raw []byte) {
		if len(raw) > 65536 {
			return
		}
		var scratch [64]lengthPathPart
		r, err := openSelectorChanges(raw, scratch[:], selectorMode(m%4))
		if err != nil {
			return
		}
		count := 0
		for {
			if _, ok := r.NextChange(); !ok {
				break
			}
			count++
		}
		if count != r.count {
			t.Fatal("count")
		}
	})
}
func BenchmarkSelectorChanges(b *testing.B) {
	all := selectorChangeFixtures()
	for _, name := range []string{"n1", "n4", "n16", "n64", "word8", "deep"} {
		c := all[name]
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorChangeWriter{mode: mode}
			raw := w.Append(nil, c)
			for _, op := range []string{"write", "read"} {
				b.Run(fmt.Sprintf("%s/%s/%s", name, mode, op), func(b *testing.B) {
					dst := make([]byte, 0, len(raw))
					var scratch [64]lengthPathPart
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if op == "write" {
							dst = w.Append(dst[:0], c)
						} else {
							r := newSelectorChanges(raw, scratch[:], mode)
							for {
								x, ok := r.NextChange()
								if !ok {
									break
								}
								sum += x.Value.num
							}
						}
					}
					sinkU, sinkBytes = sum, dst
					b.ReportMetric(float64(len(raw)), "encoded-B")
					b.ReportMetric(float64(cap(w.meta)), "scratch-B")
				})
			}
		}
	}
}
